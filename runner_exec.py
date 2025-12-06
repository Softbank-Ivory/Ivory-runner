#!/usr/bin/env python3
import json
import argparse
import importlib.util
import sys
import os
import time
import traceback
import ast


# 호출이 금지된 내장 함수의 이름들.
BLACKLISTED_BUILTINS = {
    "open",
    "eval",
    "exec",
    "__import__",
}

# 접근이 금지된 속성들의 이름 (예: os.system).
BLACKLISTED_ATTRIBUTES = {
    # os 모듈의 위험한 속성들
    "system", "popen", "spawn", "fork", "exec", "execl", "execlp", "execle",
    "execv", "execve", "execvp", "execvpe", "kill", "killpg", "putenv",
    # 파일시스템 관련 위험한 속성들
    "listdir", "remove", "removedirs", "rename", "renames", "rmdir", "symlink",
    "unlink",
}

class CodeVisitor(ast.NodeVisitor):
    """
    AST를 순회하며 금지된 구문이 있는지 확인합니다.
    위반 사항이 발견되면 `is_safe` 플래그를 False로 설정합니다.
    """
    def __init__(self):
        self.is_safe = True
        self.violations = []

    def visit(self, node):
        # 이미 위반 났으면 더 이상 안 봄
        if not self.is_safe:
            return

        # 1) 금지된 내장 함수 호출 (예: open())
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in BLACKLISTED_BUILTINS:
                self.is_safe = False
                self.violations.append(
                    f"금지된 내장 함수 '{node.func.id}' 호출이 {node.lineno}번째 줄에서 발견되었습니다"
                )
                return

        # 2) 금지된 속성 접근 (예: os.system)
        if isinstance(node, ast.Attribute):
            if node.attr in BLACKLISTED_ATTRIBUTES:
                self.is_safe = False
                self.violations.append(
                    f"금지된 속성 '{node.attr}' 사용이 {node.lineno}번째 줄에서 발견되었습니다"
                )
                return

        # 3) 문자열 리터럴에서 URL 패턴 검사 (파이썬 3.8+ Constant)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            self._check_url_in_string(node.value, node.lineno)
            if not self.is_safe:
                return

        # 나머지 계속 순회
        super().generic_visit(node)

    def _check_url_in_string(self, s, lineno):
        """
        문자열 내에 금지된 URL 패턴이 있는지 확인합니다.
        """
        forbidden_patterns = ["http://", "https://", "ftp://", "file://"]
        for pattern in forbidden_patterns:
            if pattern in s:
                self.is_safe = False
                self.violations.append(
                    f"금지된 URL 패턴 '{pattern}' 이 {lineno}번째 줄의 문자열에서 발견되었습니다"
                )
                return True
        return False



def analyze_code_safety(source: str) -> (bool, list):
    """
    소스 코드를 AST로 파싱하고 금지된 작업이 있는지 확인합니다.
    (is_safe, list_of_violations) 튜플을 반환합니다.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return False, [f"사용자 코드에 문법 오류가 있습니다: {e}"]

    visitor = CodeVisitor()
    visitor.visit(tree)
    return visitor.is_safe, visitor.violations

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
    raise FileNotFoundError(f"code.py 가 workdir 에 없습니다: {code_file}")

# ======================= 보안 검사 =======================
try:
    with open(code_file, "r", encoding="utf-8") as f:
        user_code = f.read()
    
    is_safe, violations = analyze_code_safety(user_code)
    
    if not is_safe:
        # 모든 위반 메시지를 하나의 문자열로 합칩니다.
        error_message = "보안 검사 실패: " + ", ".join(violations)
        raise PermissionError(error_message)

except PermissionError:
    # 보안 예외는 아래의 메인 예외 핸들러에서 처리되도록 다시 발생시킵니다.
    raise
except Exception as e:
    # 파일 읽기 오류 등 예기치 못한 문제를 처리합니다.
    raise IOError(f"보안 검사를 위해 코드 파일을 읽거나 분석하는 데 실패했습니다: {e}")
# ===================== 보안 검사 종료 =====================

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

