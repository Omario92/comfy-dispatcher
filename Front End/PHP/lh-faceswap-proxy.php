<?php
/**
 * Plugin Name: LH FaceSwap Proxy
 * Description: Async proxy cho Face Check & Face Swap & Phone (v2.1 - Direct Dispatcher Fallback)
 * Version: 2.1
 * Author: LHGenai + Claude
 *
 * KIẾN TRÚC v2.1 (fix n8n callback 404):
 *   /status sẽ tự query Dispatcher API trực tiếp nếu transient vẫn pending.
 *   Không cần n8n callback thành công mới có kết quả.
 *   Flow: Browser poll /status → check transient → nếu pending, query Dispatcher
 *         → nếu done, tự update transient → trả kết quả về frontend
 */

if (!defined('ABSPATH')) exit;

// ================== CONFIG ==================
define('N8N_CHECK_URL', 'https://57234.vpsvinahost.vn/webhook/f874428a-dde1-4e00-8fba-61870384dc75');
define('N8N_SWAP_URL',  'https://57234.vpsvinahost.vn/webhook/faceswap-submit');
define('N8N_PHONE_URL', 'https://57234.vpsvinahost.vn/webhook/94d27113-3fc4-4ddd-830d-8116ecd3e5d9');

// Dispatcher gọi callback về 2 webhook riêng của n8n:
//   image job → n8n xử lý → gọi PHP /result với output_type=image, image_url=result_url
//   video job → n8n xử lý → gọi PHP /result với output_type=video, video_url=result_url
define('N8N_IMG_RESULT_URL', 'https://57234.vpsvinahost.vn/webhook/halida-faceswap-img-result');
define('N8N_VID_RESULT_URL', 'https://57234.vpsvinahost.vn/webhook/halida-faceswap-video-result');

// Dispatcher API — dùng để poll kết quả trực tiếp khi n8n callback fail
// GET {DISPATCHER_URL}/jobs/{job_id} → trả {status, result_url, output_type, ...}
define('DISPATCHER_URL', 'https://comfy-dispatcher-production.up.railway.app');
define('DISPATCHER_API_KEY', '');  // Để trống nếu không cần auth, hoặc set Bearer token

define('N8N_SECRET',       'matkhau_bimat_2026_random_8f3k9x2m7pQvL9wZxYvT5rN2bPqR8sT');
define('NONCE_ACTION',     'lh_faceswap_upload');
define('MAX_FILE_SIZE',    20 * 1024 * 1024); // 20MB
define('JOB_TTL',          1800);             // Lưu kết quả 30 phút
define('POLL_TIMEOUT',     30);               // PHP chờ n8n tối đa 30s/poll cycle

// Map personality number → img-personality URL (dùng khi fallback từ Dispatcher trực tiếp)
// n8n thường tự map và gửi URL, nhưng khi bypass n8n cần map thủ công
define('PERSONALITY_IMG_MAP', [
    0 => 'https://biatuoi-halida.com/wp-content/uploads/sites/2/2026/04/HLD-BTNG.webp',
    1 => 'https://biatuoi-halida.com/wp-content/uploads/sites/2/2026/04/HLD-BTNG.webp',
    2 => 'https://biatuoi-halida.com/wp-content/uploads/sites/2/2026/04/HLD-BTNG.webp',
    3 => 'https://biatuoi-halida.com/wp-content/uploads/sites/2/2026/04/HLD-BTNG.webp',
    4 => 'https://biatuoi-halida.com/wp-content/uploads/sites/2/2026/04/HLD-BTNG.webp',
    5 => 'https://biatuoi-halida.com/wp-content/uploads/sites/2/2026/04/HLD-BTNG.webp',
]);


// ================== ĐĂNG KÝ REST API ==================
add_action('rest_api_init', 'lh_faceswap_register_routes');

function lh_faceswap_register_routes() {
    // Check ảnh (sync OK vì nhanh ~2-5s)
    register_rest_route('faceswap/v1', '/check', [
        'methods'             => 'POST',
        'callback'            => 'lh_handle_face_check',
        'permission_callback' => '__return_true'
    ]);

    // Swap: nhận file → trả job_id ngay, không chờ n8n
    register_rest_route('faceswap/v1', '/swap', [
        'methods'             => 'POST',
        'callback'            => 'lh_handle_face_swap_async',
        'permission_callback' => '__return_true'
    ]);

    // Frontend poll kết quả: GET /wp-json/faceswap/v1/status?job=xxx
    register_rest_route('faceswap/v1', '/status', [
        'methods'             => 'GET',
        'callback'            => 'lh_handle_poll_status',
        'permission_callback' => '__return_true'
    ]);

    // n8n callback khi xong (POST từ n8n về WordPress)
    register_rest_route('faceswap/v1', '/result', [
        'methods'             => 'POST',
        'callback'            => 'lh_handle_n8n_callback',
        'permission_callback' => '__return_true'
    ]);

    // Phone (sync, nhanh)
    register_rest_route('faceswap/v1', '/phone', [
        'methods'             => 'POST',
        'callback'            => 'lh_handle_phone_submit',
        'permission_callback' => '__return_true'
    ]);

    // Token (get fresh token)
    register_rest_route('faceswap/v1', '/token', [
        'methods'             => 'GET',
        'callback'            => 'lh_handle_get_token',
        'permission_callback' => '__return_true'
    ]);
}

function lh_handle_get_token($request) {
    $time_window = floor(time() / 3600);
    $token = hash_hmac('sha256', NONCE_ACTION . ':' . $time_window, N8N_SECRET);
    return new WP_REST_Response(['token' => $token], 200);
}

// ================== VALIDATE FILE ==================
function lh_validate_uploaded_file($file) {
    if (empty($file) || empty($file['tmp_name'])) {
        return new WP_Error('no_file', 'Không tìm thấy file.', ['status' => 400]);
    }
    if ($file['size'] > MAX_FILE_SIZE) {
        return new WP_Error('file_too_large', 'File quá lớn! Giới hạn 20MB.', ['status' => 400]);
    }
    $allowed_ext = ['jpg','jpeg','png','tif','tiff','heic','heif','bmp','raw'];
    $ext = strtolower(pathinfo($file['name'], PATHINFO_EXTENSION));
    if (!in_array($ext, $allowed_ext)) {
        return new WP_Error('invalid_file_type', 'Chỉ chấp nhận file ảnh.', ['status' => 400]);
    }
    $finfo = finfo_open(FILEINFO_MIME_TYPE);
    $mime  = finfo_file($finfo, $file['tmp_name']);
    finfo_close($finfo);
    $allowed_mime = ['image/jpeg','image/png','image/tiff','image/bmp','image/heic','image/heif'];
    if (!in_array($mime, $allowed_mime) && strpos($mime, 'image/') !== 0) {
        return new WP_Error('invalid_mime', 'File không phải ảnh hợp lệ.', ['status' => 400]);
    }
    return true;
}

// ================== HMAC TOKEN (không cần session) ==================
function lh_check_token($request) {
    $token = $request->get_header('X-LH-Faceswap-Nonce');
    if (empty($token)) {
        return new WP_Error('invalid_nonce', 'Token không hợp lệ.', ['status' => 403]);
    }
    $time_window  = floor(time() / 3600);
    $valid_tokens = [
        hash_hmac('sha256', NONCE_ACTION . ':' . $time_window,       N8N_SECRET),
        hash_hmac('sha256', NONCE_ACTION . ':' . ($time_window - 1), N8N_SECRET),
        hash_hmac('sha256', NONCE_ACTION . ':' . ($time_window + 1), N8N_SECRET),
    ];
    if (!in_array($token, $valid_tokens, true)) {
        return new WP_Error('invalid_nonce', 'Token không hợp lệ.', ['status' => 403]);
    }
    return true;
}

// ================== CURL HELPER (sync, dùng cho check & phone) ==================
function lh_curl_forward($n8n_url, $post_fields, $timeout = 60) {
    $ch = curl_init($n8n_url);
    curl_setopt_array($ch, [
        CURLOPT_POST           => true,
        CURLOPT_POSTFIELDS     => $post_fields,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_TIMEOUT        => $timeout,
        CURLOPT_CONNECTTIMEOUT => 30,
        CURLOPT_ENCODING       => '',
        CURLOPT_TCP_NODELAY    => true,
        CURLOPT_HTTP_VERSION   => CURL_HTTP_VERSION_2_0,
        CURLOPT_HTTPHEADER     => [
            'X-Secret-Key: ' . N8N_SECRET,
            'User-Agent: WordPress-LH-Proxy-v1.8'
        ]
    ]);
    $response   = curl_exec($ch);
    $http_code  = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curl_error = curl_error($ch);
    curl_close($ch);
    return [$response, $http_code, $curl_error];
}

// ================== FIRE & FORGET (async, dùng cho swap) ==================
// Gửi request đến n8n KHÔNG chờ response (timeout = 1s, bỏ qua kết quả)
function lh_curl_fire_and_forget($n8n_url, $post_fields) {
    $ch = curl_init($n8n_url);
    curl_setopt_array($ch, [
        CURLOPT_POST           => true,
        CURLOPT_POSTFIELDS     => $post_fields,
        CURLOPT_RETURNTRANSFER => true,    // PHẢI true — false sẽ in response ra output buffer, làm hỏng JSON của WP
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_TIMEOUT        => 10,      // Chỉ chờ đủ để n8n ACK nhận request
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TCP_NODELAY    => true,
        CURLOPT_HTTP_VERSION   => CURL_HTTP_VERSION_2_0,
        CURLOPT_HTTPHEADER     => [
            'X-Secret-Key: ' . N8N_SECRET,
            'User-Agent: WordPress-LH-Proxy-v1.8'
        ]
    ]);
    curl_exec($ch);  // response được capture nội bộ, không leak ra stdout
    curl_close($ch);
}

// ================== HANDLER: CHECK (sync) ==================
function lh_handle_face_check($request) {
    $check = lh_check_token($request);
    if (is_wp_error($check)) return $check;

    $files = $request->get_file_params();
    $validate = lh_validate_uploaded_file($files['file'] ?? null);
    if (is_wp_error($validate)) return $validate;

    $post_fields = [
        'file' => new CURLFile(
            $files['file']['tmp_name'],
            $files['file']['type'] ?? 'application/octet-stream',
            $files['file']['name']
        )
    ];
    foreach ($request->get_body_params() as $k => $v) $post_fields[$k] = $v;

    set_time_limit(60);
    [$response, $http_code, $curl_error] = lh_curl_forward(N8N_CHECK_URL, $post_fields, 55);

    if ($curl_error) {
        return new WP_Error('curl_error', 'Lỗi kết nối: ' . $curl_error, ['status' => 500]);
    }
    
    $data = json_decode($response, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        return new WP_REST_Response(['success' => false, 'reason' => 'Lỗi parse JSON từ n8n'], 500);
    }
    
    // n8n "Respond to Webhook" node thường trả về mảng [{...}]
    // Unwrap mảng để frontend nhận được object gốc
    if (is_array($data) && isset($data[0]) && is_array($data[0])) {
        $data = $data[0];
    }
    
    return new WP_REST_Response($data, $http_code);
}

// ================== HANDLER: SWAP ASYNC (v2.0 — Dual-Job Parallel) ==================
// Tạo 2 job song song: image (priority: high) + video (priority: normal)
// Trả cả image_job_id + video_job_id về frontend ngay lập tức (< 2s)
function lh_handle_face_swap_async($request) {
    $check = lh_check_token($request);
    if (is_wp_error($check)) return $check;

    $files = $request->get_file_params();
    $validate = lh_validate_uploaded_file($files['file'] ?? null);
    if (is_wp_error($validate)) return $validate;

    // Tạo 2 job ID riêng biệt cho image và video
    $image_job_id = 'lhfs_img_' . bin2hex(random_bytes(10));
    $video_job_id = 'lhfs_vid_' . bin2hex(random_bytes(10));

    $now = time();

    // Lưu trạng thái pending cho cả 2 job, kèm sibling_job để link ngược lại
    set_transient('lhfs_job_' . $image_job_id, [
        'status'       => 'pending',
        'output_type'  => 'image',
        'sibling_job'  => $video_job_id,   // dùng khi n8n callback 1 lần cho cả 2
        'created_at'   => $now,
    ], JOB_TTL);

    set_transient('lhfs_job_' . $video_job_id, [
        'status'       => 'pending',
        'output_type'  => 'video',
        'sibling_job'  => $image_job_id,
        'created_at'   => $now,
    ], JOB_TTL);

    // PHP callback URL (WordPress REST): n8n gọi về đây khi xong từng job
    $wp_callback_url = rest_url('faceswap/v1/result');

    // Build post fields — gửi đủ thông tin để n8n submit 2 job riêng biệt lên dispatcher
    $post_fields = [
        'file'              => new CURLFile(
            $files['file']['tmp_name'],
            $files['file']['type'] ?? 'application/octet-stream',
            $files['file']['name']
        ),
        // Backward compat: n8n cũ đọc $json.job_id — map sang image_job_id
        'job_id'            => $image_job_id,
        'image_job_id'      => $image_job_id,
        'video_job_id'      => $video_job_id,

        // n8n dùng các URL này làm callback_url khi submit lên dispatcher
        // Dispatcher sẽ gọi về đúng n8n webhook khi từng job xong
        'image_n8n_callback' => N8N_IMG_RESULT_URL,
        'video_n8n_callback' => N8N_VID_RESULT_URL,

        // WordPress callback: n8n gọi về đây sau khi đã xử lý xong kết quả từ dispatcher
        'callback_url'      => $wp_callback_url,
        'callback_secret'   => N8N_SECRET,
    ];
    foreach ($request->get_body_params() as $k => $v) $post_fields[$k] = $v;

    // Gửi sang n8n — không chờ, PHP trả về ngay
    ignore_user_abort(true);
    lh_curl_fire_and_forget(N8N_SWAP_URL, $post_fields);

    // Trả cả 2 job_id về frontend ngay lập tức
    return new WP_REST_Response([
        'success'       => true,
        'status'        => 'pending',
        'image_job_id'  => $image_job_id,
        'video_job_id'  => $video_job_id,
        'message'       => 'Đang xử lý song song image + video...',
        'poll_interval' => 3000,
    ], 202);
}

// ================== HANDLER: POLL STATUS ==================
// Frontend GET /wp-json/faceswap/v1/status?job=lhfs_xxx
//
// v2.1: Nếu transient vẫn "pending", tự query Dispatcher API để lấy kết quả trực tiếp.
// Điều này bypass hoàn toàn n8n callback — đảm bảo frontend luôn nhận được kết quả
// ngay cả khi n8n webhook bị lỗi 404 hoặc không hoạt động.
function lh_handle_poll_status($request) {
    $job_id = sanitize_text_field($request->get_param('job'));
    $valid_prefix = str_starts_with($job_id, 'lhfs_') || str_starts_with($job_id, 'job_');
    if (empty($job_id) || !$valid_prefix) {
        return new WP_Error('invalid_job', 'Job ID không hợp lệ.', ['status' => 400]);
    }

    $data = get_transient('lhfs_job_' . $job_id);
    if ($data === false) {
        return new WP_REST_Response([
            'success' => false,
            'status'  => 'expired',
            'reason'  => 'Job đã hết hạn hoặc không tồn tại.',
        ], 404);
    }

    // ── v2.1: Fallback — query Dispatcher trực tiếp nếu transient còn pending ──
    // n8n callback có thể fail (404, timeout, v.v.) → transient mãi không đổi
    if (($data['status'] ?? 'pending') === 'pending') {
        $dispatcher_data = lh_query_dispatcher_job($job_id);

        if ($dispatcher_data && in_array($dispatcher_data['status'] ?? '', ['done', 'failed'], true)) {
            $disp_status  = $dispatcher_data['status'];
            $result_url   = $dispatcher_data['result_url'] ?? '';
            $output_type  = $data['output_type'] ?? (str_starts_with($job_id, 'lhfs_img_') ? 'image' : 'video');
            $now          = time();

            if ($disp_status === 'done' && $result_url) {
                // Dispatcher trả về img_personality (dấu _), map sang img-personality (dấu -)
                // Nếu là URL → dùng luôn; nếu là số → map sang URL qua PERSONALITY_IMG_MAP
                $raw_personality = $dispatcher_data['img_personality'] ?? $dispatcher_data['personality'] ?? '';
                if (filter_var($raw_personality, FILTER_VALIDATE_URL)) {
                    $img_personality_direct = $raw_personality;
                } elseif (is_numeric($raw_personality)) {
                    $map = defined('PERSONALITY_IMG_MAP') ? PERSONALITY_IMG_MAP : [];
                    $img_personality_direct = $map[(int)$raw_personality] ?? '';
                } else {
                    $img_personality_direct = '';
                }

                if ($output_type === 'image') {
                    $updated = [
                        'status'          => 'done',
                        'success'         => true,
                        'output_type'     => 'image',
                        'image_url'       => $result_url,
                        'preview_url'     => '',
                        'img-personality' => $img_personality_direct,
                        'sibling_job'     => $data['sibling_job'] ?? null,
                        'completed_at'    => $now,
                        '_source'         => 'dispatcher_direct',
                    ];
                } else {
                    $updated = [
                        'status'          => 'done',
                        'success'         => true,
                        'output_type'     => 'video',
                        'video_url'       => $result_url,
                        'img-personality' => $img_personality_direct,
                        'sibling_job'     => $data['sibling_job'] ?? null,
                        'completed_at'    => $now,
                        '_source'         => 'dispatcher_direct',
                    ];
                }
            } else {
                // failed
                $updated = [
                    'status'      => 'error',
                    'success'     => false,
                    'output_type' => $output_type,
                    'reason'      => $dispatcher_data['error'] ?? 'Job failed on dispatcher',
                    'sibling_job' => $data['sibling_job'] ?? null,
                    'completed_at'=> $now,
                    '_source'     => 'dispatcher_direct',
                ];
            }

            // Lưu lại transient để các poll tiếp theo không cần query Dispatcher nữa
            set_transient('lhfs_job_' . $job_id, $updated, JOB_TTL);
            $data = $updated;
        }
    }

    return new WP_REST_Response(array_merge(['success' => true], $data), 200);
}

// ================== HELPER: QUERY DISPATCHER ==================
// Query GET {DISPATCHER_URL}/jobs/{job_id} và trả về array kết quả.
// Trả null nếu lỗi hoặc job chưa done.
function lh_query_dispatcher_job($job_id) {
    if (!defined('DISPATCHER_URL') || empty(DISPATCHER_URL)) {
        return null;
    }

    $url = rtrim(DISPATCHER_URL, '/') . '/jobs/' . urlencode($job_id);
    $headers = ['User-Agent: WordPress-LH-Proxy-v2.1'];
    if (defined('DISPATCHER_API_KEY') && DISPATCHER_API_KEY !== '') {
        $headers[] = 'Authorization: Bearer ' . DISPATCHER_API_KEY;
    }

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_TIMEOUT        => 5,
        CURLOPT_CONNECTTIMEOUT => 5,
        CURLOPT_HTTPHEADER     => $headers,
    ]);
    $response  = curl_exec($ch);
    $http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    if ($http_code !== 200 || !$response) {
        return null;
    }
    $json = json_decode($response, true);
    if (json_last_error() !== JSON_ERROR_NONE || !is_array($json)) {
        return null;
    }
    return $json;
}

// ================== HANDLER: N8N CALLBACK ==================
// n8n POST về đây khi swap xong, kèm job_id + kết quả
// Trong n8n: thêm HTTP Request node cuối workflow gọi về callback_url
function lh_handle_n8n_callback($request) {
    // Xác thực secret từ n8n
    $secret = $request->get_header('X-Secret-Key');
    if ($secret !== N8N_SECRET) {
        return new WP_Error('unauthorized', 'Unauthorized.', ['status' => 401]);
    }

    $body   = $request->get_json_params();
    $job_id = sanitize_text_field($body['job_id'] ?? '');
    $valid_prefix = str_starts_with($job_id, 'lhfs_') || str_starts_with($job_id, 'job_');
    if (empty($job_id) || !$valid_prefix) {
        return new WP_Error('invalid_job', 'Job ID không hợp lệ.', ['status' => 400]);
    }

    // Kiểm tra job tồn tại
    $existing = get_transient('lhfs_job_' . $job_id);
    if ($existing === false) {
        return new WP_REST_Response(['success' => false, 'reason' => 'Job không tồn tại hoặc hết hạn.'], 404);
    }

    // n8n cũ gửi success = true|false, n8n mới có thể gửi error = null
    $is_success = isset($body['success'])
        ? (bool)$body['success']
        : empty($body['error']);

    // Lấy output_type: n8n mới gửi "output_type", n8n cũ không gửi → detect từ job_id prefix
    $output_type_in_body = $body['output_type'] ?? null;
    if ($output_type_in_body) {
        $output_type = sanitize_text_field($output_type_in_body);
    } elseif (str_starts_with($job_id, 'lhfs_img_')) {
        $output_type = 'image';
    } elseif (str_starts_with($job_id, 'lhfs_vid_')) {
        $output_type = 'video';
    } else {
        // n8n cũ: job_id là lhfs_xxx không có prefix img/vid → là video (legacy)
        $output_type = 'video';
    }

    $video_url       = sanitize_url($body['result_url'] ?? $body['video_url'] ?? '');
    $img_personality = sanitize_url($body['img-personality'] ?? '');
    $reason          = sanitize_text_field($body['reason'] ?? '');
    $now             = time();
    $sibling_job     = $existing['sibling_job'] ?? null;

    if ($output_type === 'image') {
        $result = [
            'status'          => $is_success ? 'done' : 'error',
            'success'         => $is_success,
            'output_type'     => 'image',
            'image_url'       => sanitize_url($body['result_url'] ?? $body['image_url'] ?? ''),
            'preview_url'     => sanitize_url($body['preview_url'] ?? ''),
            'img-personality' => $img_personality,
            'reason'          => $reason,
            'completed_at'    => $now,
        ];
        set_transient('lhfs_job_' . $job_id, $result, JOB_TTL);

    } else {
        // output_type = video (hoặc n8n cũ không gửi output_type)
        $video_result = [
            'status'          => $is_success ? 'done' : 'error',
            'success'         => $is_success,
            'output_type'     => 'video',
            'video_url'       => $video_url,
            'img-personality' => $img_personality,
            'reason'          => $reason,
            'completed_at'    => $now,
        ];
        set_transient('lhfs_job_' . $job_id, $video_result, JOB_TTL);

        // ── BACKWARD COMPAT (n8n cũ, single-job) ──────────────────────────────
        // n8n callback 1 lần duy nhất với video_url.
        // Cập nhật sibling image_job transient để frontend image-poller cũng resolve.
        // Frontend showQuickImageResult() sẽ dùng img-personality làm preview image tạm.
        if ($sibling_job && !$output_type_in_body) {
            $image_preview = [
                'status'          => $is_success ? 'done' : 'error',
                'success'         => $is_success,
                'output_type'     => 'image',
                'image_url'       => '',           // chưa có ảnh riêng từ n8n cũ
                'preview_url'     => '',
                'img-personality' => $img_personality,
                'video_url'       => $video_url,   // truyền luôn để frontend có thể preload
                'reason'          => $reason,
                'completed_at'    => $now,
            ];
            set_transient('lhfs_job_' . $sibling_job, $image_preview, JOB_TTL);
        }
    }

    return new WP_REST_Response(['success' => true, 'message' => 'Kết quả đã được lưu.'], 200);
}

// ================== HANDLER: PHONE (sync) ==================
function lh_handle_phone_submit($request) {
    $check = lh_check_token($request);
    if (is_wp_error($check)) return $check;

    set_time_limit(60);
    $body = $request->get_body_params();
    $post_fields = [];
    foreach ($body as $k => $v) $post_fields[$k] = $v;

    [$response, $http_code, $curl_error] = lh_curl_forward(N8N_PHONE_URL, $post_fields, 55);

    if ($curl_error) {
        return new WP_Error('curl_error', 'Lỗi kết nối: ' . $curl_error, ['status' => 500]);
    }
    $data = json_decode($response, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        return new WP_REST_Response(['success' => false, 'reason' => 'Lỗi parse JSON từ n8n'], 500);
    }
    return new WP_REST_Response($data, $http_code);
}

// ================== INJECT TOKEN & CORS ==================
add_action('wp_head', 'lh_faceswap_inject_token', 5);
function lh_faceswap_inject_token() {
    $time_window = floor(time() / 3600);
    $token = hash_hmac('sha256', NONCE_ACTION . ':' . $time_window, N8N_SECRET);
    echo '<script>window.LH_FACESWAP_NONCE = "' . esc_js($token) . '";</script>' . "\n";
}

add_filter('rest_allowed_cors_headers', function($headers) {
    $headers[] = 'X-LH-Faceswap-Nonce';
    return $headers;
});
