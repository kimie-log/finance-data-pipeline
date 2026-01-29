import importlib
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def require_module(module_name: str, install_hint: str | None = None) -> None:
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
        terminalreporter.write_line(f"測試成功：  {filename}")
