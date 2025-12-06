#!/usr/bin/env python3
import json
import argparse
import importlib.util
import sys
import os
import time
import traceback

parser = argparse.ArgumentParser()
parser.add_argument("--workdir", required=True)
parser.add_argument("--handler", required=True)          # 예: main.handler / main.main
parser.add_argument("--timeout", type=int, required=True)  # ms 단위
args = parser.parse_args()

workdir = args.workdir
handler = args.handler
timeout_sec = args.timeout / 1000.0

# code.py 가 있는 경로를 import path 에 추가
sys.path.insert(0, workdir)

# handler 형식: "<module>.<function>" (예: main.handler, main.main)
module_name, func_name = handler.split(".")

# 항상 workdir/code.py 를 로드 (module_name은 모듈 이름용으로만 사용)
code_file = os.path.join(workdir, "code.py")
if not os.path.exists(code_file):
    raise FileNotFoundError(f"code.py not found in workdir: {code_file}")

spec = importlib.util.spec_from_file_location(module_name, code_file)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# handler 함수 가져오기 (func_name 이 handler 이든 main 이든 상관 없음)
func = getattr(module, func_name)

# payload 읽기 (workdir/payload.json)
payload_path = os.path.join(workdir, "payload.json")
if os.path.exists(payload_path):
    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
else:
    payload = None

start = time.time()
try:
    # timeout_sec 는 현재 미사용 (실제 타임아웃 제어는 외부 프로세스/OS 에서)
    result = func(payload)
    status = "COMPLETED"
    error_info = None
except Exception as e:
    status = "FAILED"
    result = None
    error_info = {
        "errorType": e.__class__.__name__,
        "errorMessage": str(e),
        "stackTrace": traceback.format_exc(),
    }
end = time.time()

duration = end - start

# 실행 결과 저장 (workdir/result.json)
result_path = os.path.join(workdir, "result.json")
output = {
    "status": status,
    "result": result,
    "duration": duration,
}
if error_info is not None:
    output["error"] = error_info

with open(result_path, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False)
