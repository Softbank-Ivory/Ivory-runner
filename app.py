import os
import json
import logging
import tempfile
import shutil
import subprocess
import sys
import threading
from typing import Any, Dict, Optional

import boto3
from flask import Flask, request, jsonify

# WebSocket 클라이언트 (websocket-client 패키지 필요)
try:
    from websocket import create_connection, WebSocketException
except ImportError:
    create_connection = None

    class WebSocketException(Exception):
        pass

# ------------------------------------------------------------------------------
# 설정 / 초기화
# ------------------------------------------------------------------------------

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get("S3_BUCKET_NAME")
if not S3_BUCKET:
    raise RuntimeError("환경변수 S3_BUCKET_NAME 가 설정되어 있지 않습니다.")

AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
RUNNER_ID = os.environ.get("RUNNER_ID", "runner-unknown")
RUNNER_HTTP_PORT = int(os.environ.get("RUNNER_HTTP_PORT", "8080"))

# BE WebSocket 엔드포인트 (예: ws://<BE_HOST>/internal/ws/runner)
BACKEND_WS_URL = os.environ.get("BACKEND_WS_URL")

s3 = boto3.client("s3", region_name=AWS_REGION)

# runner_exec.py 경로 (app.py와 같은 디렉터리 기준)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RUNNER_EXEC_PATH = os.path.join(SCRIPT_DIR, "runner_exec.py")


# ------------------------------------------------------------------------------
# 유틸 함수
# ------------------------------------------------------------------------------

def get_field(data: Dict[str, Any], *names: str, required: bool = True) -> Optional[Any]:
    """
    여러 후보 키 중 먼저 발견되는 값을 반환.
    예: get_field(data, "code_s3_key", "codeS3Key", "codeKey")
    """
    for name in names:
        if name in data:
            return data[name]
    if required:
        raise KeyError(f"필수 필드 없음: candidates={names}, received_keys={list(data.keys())}")
    return None


def download_s3_file(key: str, dest_path: str) -> None:
    app.logger.info(f"[Runner:{RUNNER_ID}] S3 download: bucket={S3_BUCKET}, key={key}, dest={dest_path}")
    s3.download_file(S3_BUCKET, key, dest_path)


# -------------------- WebSocket 관련 유틸 --------------------

def open_backend_ws():
    """
    BE WebSocket 연결 생성. 실패해도 Invocation 자체는 계속 진행.
    """
    if not BACKEND_WS_URL:
        app.logger.info("[Runner] BACKEND_WS_URL 이 설정되어 있지 않아 WebSocket 전송은 생략합니다.")
        return None

    if create_connection is None:
        app.logger.warning("[Runner] websocket-client 패키지가 없어 WebSocket 전송은 생략합니다.")
        return None

    try:
        ws = create_connection(BACKEND_WS_URL, timeout=5)
        app.logger.info(f"[Runner] BE WebSocket 연결 성공: {BACKEND_WS_URL}")
        return ws
    except Exception as e:
        app.logger.warning(f"[Runner] BE WebSocket 연결 실패: url={BACKEND_WS_URL}, error={e}")
        return None


def ws_send(ws, message: Dict[str, Any]):
    """
    RunnerEventMessageDto JSON 을 WebSocket으로 전송
    실패해도 그냥 로그만 찍고 진행
    """
    if ws is None:
        return
    try:
        ws.send(json.dumps(message, ensure_ascii=False))
    except Exception as e:
        app.logger.warning(f"[Runner] WebSocket send 실패: {e}")


def ws_close(ws):
    if ws is None:
        return
    try:
        ws.close()
    except Exception:
        pass


def send_status_ws(ws, invocation_id: str, status: str):
    """
    type: STATUS
    payload: { "status": "CODE_FETCHING" | "SANDBOX_PREPARING" | "EXECUTING" | ... }
    """
    msg = {
        "type": "STATUS",
        "invocationId": invocation_id,
        "payload": {
            "status": status
        }
    }
    ws_send(ws, msg)


def send_log_ws(ws, invocation_id: str, line: str):
    """
    type: LOG
    payload: { "line": "<한 줄 로그>" }
    """
    msg = {
        "type": "LOG",
        "invocationId": invocation_id,
        "payload": {
            "line": line
        }
    }
    ws_send(ws, msg)


def send_complete_success_ws(ws, invocation_id: str, duration_ms: int, status_code: int, body: str):
    """
    type: COMPLETE (성공)
    payload: {
      "status": "COMPLETED",
      "durationMs": <long>,
      "result": { "statusCode": <int>, "body": "<string>" }
    }
    """
    msg = {
        "type": "COMPLETE",
        "invocationId": invocation_id,
        "payload": {
            "status": "COMPLETED",
            "durationMs": duration_ms,
            "result": {
                "statusCode": status_code,
                "body": body,
            },
        },
    }
    ws_send(ws, msg)


def send_complete_failed_ws(ws, invocation_id: str, duration_ms: int, error_message: str):
    """
    type: COMPLETE (실패)
    payload: {
      "status": "FAILED",
      "durationMs": <long>,
      "errorMessage": "<string>"
    }
    """
    msg = {
        "type": "COMPLETE",
        "invocationId": invocation_id,
        "payload": {
            "status": "FAILED",
            "durationMs": duration_ms,
            "errorMessage": error_message,
        },
    }
    ws_send(ws, msg)


# ------------------------------------------------------------------------------
# Flask 엔드포인트
# ------------------------------------------------------------------------------

@app.route("/internal/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "runnerId": RUNNER_ID}), 200


@app.route("/internal/invocations", methods=["POST"])
def run_invocation():
    data = request.get_json(silent=True)
    app.logger.info(f"[Runner:{RUNNER_ID}] /internal/invocations 요청 바디: {data}")

    if not data:
        return jsonify({"error": "Invalid or empty JSON body"}), 400

    try:
        # 현재 BE가 보내는 키 구조:
        # ['invocationId', 'codeKey', 'runtime', 'handler', 'payload']
        invocation_id = get_field(data, "invocationId", "id")
        runtime = get_field(data, "runtime")
        handler = get_field(data, "handler")
        # codeKey, code_s3_key, codeS3Key 전부 허용
        code_key = get_field(data, "codeKey", "code_s3_key", "codeS3Key")

    except KeyError as e:
        app.logger.warning(f"[Runner:{RUNNER_ID}] 필드 부족: {e}")
        return jsonify({"error": str(e), "received_keys": list(data.keys())}), 400

    work_dir = tempfile.mkdtemp(prefix=f"inv_{invocation_id}_")
    app.logger.info(f"[Runner:{RUNNER_ID}] 작업 디렉터리 생성: {work_dir}")

    ws = None
    try:
        # BE WebSocket 연결
        ws = open_backend_ws()

        # STATUS: CODE_FETCHING
        send_status_ws(ws, invocation_id, "CODE_FETCHING")

        # 1) code.py 다운로드
        code_path = os.path.join(work_dir, "code.py")
        download_s3_file(code_key, code_path)

        # STATUS: SANDBOX_PREPARING
        send_status_ws(ws, invocation_id, "SANDBOX_PREPARING")

        # 2) payload.json 생성 (BE가 body에 payload 직접 전달)
        payload = data.get("payload")
        payload_path = os.path.join(work_dir, "payload.json")
        with open(payload_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)

        # 3) runner_exec.py 호출
        timeout_ms = 15000

        cmd = [
            sys.executable,
            RUNNER_EXEC_PATH,
            "--workdir",
            work_dir,
            "--handler",
            handler,
            "--timeout",
            str(timeout_ms),
        ]

        app.logger.info(f"[Runner:{RUNNER_ID}] runner_exec 호출: {' '.join(cmd)}")

        # STATUS: EXECUTING
        send_status_ws(ws, invocation_id, "EXECUTING")

        # -----------------------
        # 실행 + 실시간 로그 + 타임아웃
        # -----------------------
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        combined_stdout = []

        def _reader():
            """runner_exec stdout을 실시간으로 읽어서 LOG 이벤트로 쏘는 스레드"""
            try:
                for line in proc.stdout:
                    line = line.rstrip("\n")
                    combined_stdout.append(line)
                    app.logger.info(f"[Runner:{RUNNER_ID}] runner_exec log: {line}")
                    send_log_ws(ws, invocation_id, line)
            except Exception as e:
                app.logger.warning(f"[Runner:{RUNNER_ID}] log reader thread error: {e}")

        # 로그 읽기 스레드 시작
        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()

        try:
            # 여기서만 타임아웃을 감시
            proc.wait(timeout=timeout_ms / 1000.0)
        except subprocess.TimeoutExpired:
            # 프로세스 강제 종료
            proc.kill()
            app.logger.exception(f"[Runner:{RUNNER_ID}] runner_exec 타임아웃")
            duration_ms = timeout_ms
            error_message = "runner_exec timeout"

            # reader 스레드 정리 (최대 1초 정도만 기다리고 넘김)
            try:
                reader_thread.join(timeout=1.0)
            except Exception:
                pass

            # WebSocket COMPLETE (FAILED - timeout)
            send_complete_failed_ws(ws, invocation_id, duration_ms, error_message)

            return jsonify(
                {
                    "invocationId": invocation_id,
                    "runnerId": RUNNER_ID,
                    "status": "FAILED",
                    "reason": error_message,
                }
            ), 500

        # 정상 종료 시 남은 로그 스레드 정리
        try:
            reader_thread.join(timeout=1.0)
        except Exception:
            pass

        exec_stdout = "\n".join(combined_stdout)
        exec_stderr = ""  # stderr 를 stdout 에 합쳤으므로 별도 없음

        app.logger.info(
            f"[Runner:{RUNNER_ID}] runner_exec 종료 "
            f"(invocation_id={invocation_id}, exit_code={proc.returncode})"
        )

        # 4) runner_exec 가 만든 result.json 읽기
        result_path = os.path.join(work_dir, "result.json")
        if not os.path.exists(result_path):
            app.logger.error(f"[Runner:{RUNNER_ID}] result.json 없음: {result_path}")
            # duration 정보가 없으니 0 으로 보냄
            send_complete_failed_ws(ws, invocation_id, 0, "result.json not found")

            return (
                jsonify(
                    {
                        "invocationId": invocation_id,
                        "runnerId": RUNNER_ID,
                        "status": "FAILED",
                        "reason": "result.json not found",
                        "runnerExitCode": proc.returncode,
                        "stdout": exec_stdout,
                        "stderr": exec_stderr,
                    }
                ),
                500,
            )

        with open(result_path, "r", encoding="utf-8") as f:
            result_data = json.load(f)

        # result.json 구조:
        # {
        #   "status": "COMPLETED" | "FAILED",
        #   "result": ...,
        #   "duration": <seconds>,
        #   "error": { ... } (옵션)
        # }

        runner_status = result_data.get("status", "FAILED")
        user_result = result_data.get("result")
        duration_sec = result_data.get("duration", 0.0)
        error_info = result_data.get("error")

        duration_ms = int(duration_sec * 1000)

        # 유저 함수 반환값을 HTTP 스타일로 매핑
        if runner_status == "COMPLETED":
            # 기본값: 200 / JSON 문자열화
            if isinstance(user_result, dict) and "statusCode" in user_result and "body" in user_result:
                http_status_code = int(user_result.get("statusCode", 200))
                body = user_result.get("body")
                # body 가 dict 이면 문자열로 직렬화
                if not isinstance(body, str):
                    body = json.dumps(body, ensure_ascii=False)
            else:
                http_status_code = 200
                body = json.dumps(user_result, ensure_ascii=False)

            # WebSocket COMPLETE (SUCCESS)
            send_complete_success_ws(ws, invocation_id, duration_ms, http_status_code, body)

            status_code = 200
            response_body = {
                "invocationId": invocation_id,
                "runnerId": RUNNER_ID,
                "exitCode": proc.returncode,
                "stdout": exec_stdout,
                "stderr": exec_stderr,
                **result_data,
                "httpStatusCode": http_status_code,
                "httpBody": body,
            }
            return jsonify(response_body), status_code

        else:
            # FAILED
            if error_info and isinstance(error_info, dict):
                error_message = error_info.get("errorMessage") or str(error_info)
            else:
                error_message = "Function execution failed"

            # WebSocket COMPLETE (FAILED)
            send_complete_failed_ws(ws, invocation_id, duration_ms, error_message)

            status_code = 500
            response_body = {
                "invocationId": invocation_id,
                "runnerId": RUNNER_ID,
                "exitCode": proc.returncode,
                "stdout": exec_stdout,
                "stderr": exec_stderr,
                **result_data,
            }
            return jsonify(response_body), status_code

    except Exception as e:  # noqa: BLE001
        app.logger.exception(f"[Runner:{RUNNER_ID}] run_invocation 예외")
        # WebSocket COMPLETE (FAILED) – duration 정보가 없으니 0
        send_complete_failed_ws(ws, invocation_id, 0, str(e))

        return jsonify(
            {
                "invocationId": invocation_id,
                "runnerId": RUNNER_ID,
                "status": "FAILED",
                "reason": str(e),
            }
        ), 500

    finally:
        ws_close(ws)
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
            app.logger.info(f"[Runner:{RUNNER_ID}] 작업 디렉터리 삭제: {work_dir}")
        except Exception:  # noqa: BLE001
            app.logger.warning(f"[Runner:{RUNNER_ID}] 작업 디렉터리 삭제 실패: {work_dir}", exc_info=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=RUNNER_HTTP_PORT)
