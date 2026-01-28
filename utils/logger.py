import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional


def _get_log_level() -> int:
    """從環境變數 LOG_LEVEL 取得日誌等級，預設為 INFO。"""
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, level, logging.INFO)


def _get_log_dir() -> Path:
    """
    取得 log 輸出目錄：
    - 環境變數 LOG_DIR 優先
    - 否則預設為專案根目錄下的 logs/
    """
    log_dir_env = os.getenv("LOG_DIR")
    if log_dir_env:
        log_dir = Path(log_dir_env)
    else:
        # 預設：utils/logger.py -> 專案根目錄 / logs
        root_dir = Path(__file__).resolve().parents[1]
        log_dir = root_dir / "logs"

    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def configure_logger(name: Optional[str] = "finance_pipeline") -> logging.Logger:
    """
    建立並回傳一個實務化的 logger。

    特點：
    - 只在第一次呼叫時配置 handler，避免重複輸出
    - Console handler：輸出到 stderr，預設 INFO 等級
    - RotatingFileHandler：寫入 logs/finance_pipeline.log，自動滾動
    - 支援環境變數：
      - LOG_LEVEL：DEBUG / INFO / WARNING / ERROR / CRITICAL
      - LOG_DIR：自訂 log 目錄
    """
    logger = logging.getLogger(name)

    # 若已設定過 handler，直接回傳，避免重複新增 handler
    if logger.handlers:
        return logger

    logger.setLevel(_get_log_level())

    # 共用 formatter：加入模組名稱與行號，方便除錯
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s"
    )

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(_get_log_level())
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler (Rotating)
    try:
        log_dir = _get_log_dir()
        log_file = log_dir / "finance_pipeline.log"

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(_get_log_level())
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        # 若檔案 handler 發生錯誤，不阻擋程式執行，只保留 console logging
        logger.warning("Failed to initialize file logging. Falling back to console only.")

    # 不讓 log 冒泡到 root，避免被外部程式重複處理
    logger.propagate = False

    return logger


# 專案中大多數情境可直接使用此預設 logger
logger = configure_logger()

