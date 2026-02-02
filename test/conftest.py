"""
pytest 設定檔：測試共用工具與 hook。

提供模組依賴檢查（require_module、require_any_module）與測試結果輸出格式（pytest_terminal_summary）。
確保專案根目錄在 sys.path 中，測試可直接 import 專案模組。
"""

import importlib
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def require_module(module_name: str, install_hint: str | None = None) -> None:
    """
    檢查模組是否存在，缺少時拋出 RuntimeError 並提示安裝指令。

    Args:
        module_name: 要檢查的模組名稱。
        install_hint: 安裝提示（未給時使用預設 pip install {module_name}）。

    Raises:
        RuntimeError: 模組不存在時，含 Python 版本與安裝提示。
    """
    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        hint = install_hint or f"pip install {module_name}"
        python_info = f"{sys.executable} (Python {sys.version.split()[0]})"
        raise RuntimeError(
            "缺少相依套件："
            f"{module_name}。請先安裝：{hint}。"
            f"目前 pytest 使用的 Python：{python_info}"
        ) from exc


def require_any_module(module_names: list[str], install_hint: str | None = None) -> None:
    """
    檢查模組列表中任一個是否存在，全部缺少時拋出 RuntimeError。

    實務：用於 parquet engine（pyarrow 或 fastparquet 任一可用即可）。

    Args:
        module_names: 要檢查的模組名稱列表。
        install_hint: 安裝提示（未給時使用預設 pip install {所有模組}）。

    Raises:
        RuntimeError: 所有模組都不存在時，含 Python 版本與安裝提示。
    """
    for module_name in module_names:
        try:
            importlib.import_module(module_name)
            return
        except ModuleNotFoundError:
            continue
    hint = install_hint or "pip install " + " ".join(module_names)
    python_info = f"{sys.executable} (Python {sys.version.split()[0]})"
    raise RuntimeError(
        "缺少相依套件（任一即可）："
        f"{', '.join(module_names)}。請先安裝：{hint}。"
        f"目前 pytest 使用的 Python：{python_info}"
    )


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """
    pytest hook：測試完成時輸出每個測試檔的成功訊息。

    實務：僅在全部通過時輸出，方便掃描哪些測試檔通過；失敗時不輸出避免干擾錯誤訊息。
    """
    if exitstatus != 0:
        return

    results_by_file: dict[str, bool] = {}
    for report in terminalreporter.stats.get("passed", []):
        if report.when != "call":
            continue
        path = getattr(report, "fspath", None)
        if path is None:
            continue
        results_by_file[str(path)] = True

    for path in sorted(results_by_file):
        filename = Path(path).name
        terminalreporter.write_line(f"測試成功:  {filename}")
