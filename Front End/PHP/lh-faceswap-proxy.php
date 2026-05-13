<?php
/**
 * Plugin Name: LH FaceSwap Proxy
 * Description: Async proxy cho Face Check & Face Swap & Phone (v1.8 - Fire & Poll)
 * Version: 1.8
 * Author: LHGenai + Claude
 *
 * KIẾN TRÚC MỚI (fix 500 timeout):
 *   Cũ: Browser → PHP chờ n8n (7 phút) → trả kết quả  ← PHP timeout → 500
 *   Mới: Browser → PHP forward file → trả job_id ngay  ← không timeout
 *         Browser poll /status?job=xxx mỗi 5 giây
 *         n8n callback /result khi xong → lưu vào transient
 */

if (!defined('ABSPATH')) exit;

// ================== CONFIG ==================
define('N8N_CHECK_URL', 'https://57234.vpsvinahost.vn/webhook/f874428a-dde1-4e00-8fba-61870384dc75');
define('N8N_SWAP_URL',  'https://57234.vpsvinahost.vn/webhook/faceswap-submit');
define('N8N_PHONE_URL', 'https://57234.vpsvinahost.vn/webhook/94d27113-3fc4-4ddd-830d-8116ecd3e5d9');

define('N8N_SECRET',       'matkhau_bimat_2026_random_8f3k9x2m7pQvL9wZxYvT5rN2bPqR8sT');
define('NONCE_ACTION',     'lh_faceswap_upload');
define('MAX_FILE_SIZE',    20 * 1024 * 1024); // 20MB
define('JOB_TTL',          1800);             // Lưu kết quả 30 phút
define('POLL_TIMEOUT',     30);               // PHP chờ n8n tối đa 30s/poll cycle

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

// ================== HANDLER: SWAP ASYNC ==================
// Bước 1: Nhận file từ frontend
// Bước 2: Tạo job_id, lưu trạng thái "pending"
// Bước 3: Gửi file + job_id + callback_url sang n8n (fire & forget)
// Bước 4: Trả job_id về frontend ngay lập tức (< 2s)
function lh_handle_face_swap_async($request) {
    $check = lh_check_token($request);
    if (is_wp_error($check)) return $check;

    $files = $request->get_file_params();
    $validate = lh_validate_uploaded_file($files['file'] ?? null);
    if (is_wp_error($validate)) return $validate;

    // Tạo job ID duy nhất
    $job_id = 'lhfs_' . bin2hex(random_bytes(12));

    // Lưu trạng thái pending vào WordPress transient
    set_transient('lhfs_job_' . $job_id, [
        'status'     => 'pending',
        'created_at' => time(),
    ], JOB_TTL);

    // URL callback để n8n gọi về khi xong
    $callback_url = rest_url('faceswap/v1/result');

    // Build post fields — thêm job_id và callback_url
    $post_fields = [
        'file'         => new CURLFile(
            $files['file']['tmp_name'],
            $files['file']['type'] ?? 'application/octet-stream',
            $files['file']['name']
        ),
        'job_id'       => $job_id,
        'callback_url' => $callback_url,
        'callback_secret' => N8N_SECRET,  // n8n dùng để xác thực khi callback
    ];
    foreach ($request->get_body_params() as $k => $v) $post_fields[$k] = $v;

    // Gửi sang n8n — không chờ, PHP trả về ngay
    ignore_user_abort(true);
    lh_curl_fire_and_forget(N8N_SWAP_URL, $post_fields);

    // Trả job_id về frontend ngay lập tức
    return new WP_REST_Response([
        'success' => true,
        'status'  => 'pending',
        'job_id'  => $job_id,
        'message' => 'Đang xử lý, vui lòng chờ...',
        'poll_interval' => 5000,  // JS poll mỗi 5 giây (ms)
    ], 202);
}

// ================== HANDLER: POLL STATUS ==================
// Frontend GET /wp-json/faceswap/v1/status?job=lhfs_xxx
function lh_handle_poll_status($request) {
    $job_id = sanitize_text_field($request->get_param('job'));
    // Chấp nhận cả job_id cũ (lhfs_) lẫn job_id mới từ Railway Dispatcher (job_)
    if (empty($job_id) || (!str_starts_with($job_id, 'lhfs_') && !str_starts_with($job_id, 'job_'))) {
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

    return new WP_REST_Response(array_merge(['success' => true], $data), 200);
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
    // Chấp nhận cả job_id cũ (lhfs_) lẫn job_id mới từ Railway Dispatcher (job_)
    if (empty($job_id) || (!str_starts_with($job_id, 'lhfs_') && !str_starts_with($job_id, 'job_'))) {
        return new WP_Error('invalid_job', 'Job ID không hợp lệ.', ['status' => 400]);
    }

    // Kiểm tra job tồn tại
    $existing = get_transient('lhfs_job_' . $job_id);
    if ($existing === false) {
        return new WP_REST_Response(['success' => false, 'reason' => 'Job không tồn tại hoặc hết hạn.'], 404);
    }

    // Lưu kết quả vào transient — frontend sẽ lấy qua /status
    $result = [
        'status'      => $body['success'] ? 'done' : 'error',
        'success'     => (bool)($body['success'] ?? false),
        'video_url'   => sanitize_url($body['video_url'] ?? ''),
        'img-personality' => sanitize_url($body['img-personality'] ?? ''),
        'reason'      => sanitize_text_field($body['reason'] ?? ''),
        'completed_at'=> time(),
    ];

    set_transient('lhfs_job_' . $job_id, $result, JOB_TTL);

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
