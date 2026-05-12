# HALIDA_Faceswap_Dispatcher Rules & Guidelines

## Rules
1. Maintain clean architecture and strict separation of concerns.
2. Adhere to environment variables strictly through `config.py`.
3. Dispatcher must remain highly available, all long running tasks must be asynchronous.
4. Keep logs formatted properly using `loguru`.
5. Sau khi hoàn thành task lớn, phải cập nhật phần "Recent Changes".

## Commands
- `/push-code`: Push current changes to git repository.

## Recent Changes
- [2026-05-12] Setup Dispatcher Service trên Railway với FastAPI, Redis và RunPod API integration (Giai đoạn 6).
