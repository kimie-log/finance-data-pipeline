"""
Alphalens 相關共用工具函式。

提供：
    - prepare_prices_for_alphalens: 價量資料轉為 Alphalens 價格矩陣 (date × stock_id)
    - find_local_parquet_files: 在 data/processed 底下尋找最新的價量／因子 parquet
    - ensure_factor_datetime_asset_value: 將因子表統一為 datetime, asset, value 欄位
    - factor_series_for_alphalens: 將 datetime, asset, value 轉為 Alphalens 所需 MultiIndex Series
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Callable
from datetime import datetime
from contextlib import redirect_stdout

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from .logger import logger


def prepare_prices_for_alphalens(df_price: pd.DataFrame) -> pd.DataFrame:
    """將價量資料轉為 Alphalens 所需格式：index=date, columns=stock_id, values=close。"""
    prices = df_price.pivot(index="date", columns="stock_id", values="close")
    prices.index = pd.to_datetime(prices.index)
    prices = prices.sort_index()
    logger.info(
        "價格資料準備完成: %d 個交易日，%d 檔股票", prices.shape[0], prices.shape[1]
    )
    return prices


def find_local_parquet_files(
    root_dir: Path,
    dataset_id: str,
    start_date: str,
    end_date: str,
    data_type: str = "price",
) -> Optional[Path]:
    """在 data/processed 下尋找符合條件的 parquet 檔案（目前以檔案名稱 pattern + 修改時間為主）。"""
    processed_dir = root_dir / "data" / "processed"
    if not processed_dir.exists():
        return None
    pattern = "fact_price*.parquet" if data_type == "price" else "fact_factor*.parquet"
    parquet_files = list(processed_dir.rglob(pattern))
    if not parquet_files:
        return None
    latest = max(parquet_files, key=lambda p: p.stat().st_mtime)
    logger.info("找到本地 %s 檔案: %s", data_type, latest)
    return latest


def ensure_factor_datetime_asset_value(
    df: pd.DataFrame,
    factor_name: str,
) -> pd.DataFrame:
    """
    將因子 DataFrame 統一為 datetime, asset, value 欄位（供排名／Alphalens 使用）。

    支援輸入格式：
        - 已含 datetime, asset, value
        - datetime, asset + 單一數值欄位（例如因子名）
        - date, stock_id + 單一數值欄位
    """
    if "datetime" in df.columns and "asset" in df.columns:
        if "value" not in df.columns and factor_name in df.columns:
            out = df[["datetime", "asset"]].copy()
            out["value"] = df[factor_name].values
            return out
        return df[["datetime", "asset", "value"]].copy()

    # date, stock_id 格式
    out = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()
    out = out.rename(columns={"date": "datetime", "stock_id": "asset"})
    value_cols = [c for c in out.columns if c not in ("datetime", "asset")]
    if value_cols:
        out["value"] = out[value_cols[0]]
    else:
        raise ValueError("因子資料缺少數值欄位")
    return out[["datetime", "asset", "value"]]


def factor_series_for_alphalens(df_factor: pd.DataFrame) -> pd.Series:
    """
    將 datetime, asset, value 因子資料轉成 Alphalens 所需 MultiIndex Series。

    Index: (date, asset)
    """
    df = df_factor.copy()
    df["date"] = pd.to_datetime(df["datetime"]).dt.normalize()
    df["asset"] = df["asset"].astype(str)
    df = df[["date", "asset", "value"]].dropna()
    s = df.set_index(["date", "asset"])["value"]
    s.index = s.index.rename(["date", "asset"])
    return s


def save_single_factor_tear_sheet(
    alphalens_data: pd.DataFrame,
    label: str,
    params: dict,
    root_dir: Path,
    create_full_tear_sheet_func: Callable[[pd.DataFrame], None],
) -> Optional[Path]:
    """
    產生「單因子」 Alphalens tear sheet，並輸出到：
        data/single_factor_analysis_reports/s{開始}_e{結束}_mv{市值日}/single_{因子名}_{時間戳}/
    """
    from matplotlib.backends.backend_pdf import PdfPages

    run_ts = params.get("run_timestamp")
    if not run_ts:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        params["run_timestamp"] = run_ts

    safe_label = label.replace("/", "_").replace("\\", "_").replace(",", "_")
    start_s = (params["start"] or "").replace("-", "")
    end_s = (params["end"] or "").replace("-", "")
    mv = params.get("market_value_date") or params.get("start") or ""
    mv_s = mv.replace("-", "") if isinstance(mv, str) else ""
    range_dir = f"s{start_s}_e{end_s}_mv{mv_s}"
    folder_name = f"single_{safe_label}_{run_ts}"
    report_dir = (
        root_dir
        / "data"
        / "single_factor_analysis_reports"
        / range_dir
        / folder_name
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    report_base = f"single_{run_ts}"
    report_path = report_dir / report_base

    pdf_path = report_path.with_suffix(".pdf")
    pdf_file = PdfPages(pdf_path)
    saved_count = [0]

    def _save_on_show(*args, **kwargs):
        fig = plt.gcf()
        if fig.axes:
            try:
                fig.canvas.draw()
                pdf_file.savefig(fig, bbox_inches="tight", facecolor="white")
                saved_count[0] += 1
                png_path = (
                    report_path.parent
                    / f"{report_base}_page_{saved_count[0]:02d}.png"
                )
                fig.savefig(
                    png_path, dpi=150, bbox_inches="tight", facecolor="white"
                )
                logger.info("  已保存圖表 %d: %s", saved_count[0], png_path.name)
            except Exception as e:
                logger.warning("保存圖表時失敗: %s", e)

    _original_show = plt.show
    plt.show = _save_on_show

    summary_path = report_path.parent / f"{report_base}_summary.txt"
    try:
        with open(summary_path, "w", encoding="utf-8") as summary_file, redirect_stdout(
            summary_file
        ):
            create_full_tear_sheet_func(alphalens_data)
    finally:
        plt.show = _original_show
        pdf_file.close()

    if saved_count[0] > 0:
        logger.info("報表已保存: %s", report_dir)
    else:
        logger.warning("未偵測到 Alphalens 產生的圖表")
        report_dir = None

    return report_dir


def save_multi_factor_tear_sheet(
    alphalens_data: pd.DataFrame,
    label: str,
    params: dict,
    root_dir: Path,
    create_full_tear_sheet_func: Callable[[pd.DataFrame], None],
) -> Optional[Path]:
    """
    產生「多因子」 Alphalens tear sheet，並輸出到：
        data/multi_factor_analysis_reports/
          s{開始}_e{結束}_mv{市值日}/
            multi_{mode}_{timestamp}/{label}/
    """
    from matplotlib.backends.backend_pdf import PdfPages

    run_ts = params.get("run_timestamp")
    if not run_ts:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        params["run_timestamp"] = run_ts

    safe_label = label.replace("/", "_").replace("\\", "_").replace(",", "_")
    start_s = (params["start"] or "").replace("-", "")
    end_s = (params["end"] or "").replace("-", "")
    mv = params.get("market_value_date") or params.get("start") or ""
    mv_s = mv.replace("-", "") if isinstance(mv, str) else ""
    range_dir = f"s{start_s}_e{end_s}_mv{mv_s}"

    outer_folder = f"multi_{params['mode']}_{run_ts}"
    outer_dir = (
        root_dir / "data" / "multi_factor_analysis_reports" / range_dir / outer_folder
    )
    report_dir = outer_dir / safe_label
    report_dir.mkdir(parents=True, exist_ok=True)
    report_base = f"multi_{params['mode']}_{run_ts}"
    report_path = report_dir / report_base

    pdf_path = report_path.with_suffix(".pdf")
    pdf_file = PdfPages(pdf_path)
    saved_count = [0]

    def _save_on_show(*args, **kwargs):
        fig = plt.gcf()
        if fig.axes:
            try:
                fig.canvas.draw()
                pdf_file.savefig(fig, bbox_inches="tight", facecolor="white")
                saved_count[0] += 1
                png_path = report_path.parent / f"{report_base}_page_{saved_count[0]:02d}.png"
                fig.savefig(png_path, dpi=150, bbox_inches="tight", facecolor="white")
                logger.info(f"  已保存圖表 {saved_count[0]}: {png_path.name}")
            except Exception as e:
                logger.warning(f"保存圖表時失敗: {e}")

    _original_show = plt.show
    plt.show = _save_on_show

    summary_path = report_dir / f"{report_base}_summary.txt"
    try:
        with open(summary_path, "w", encoding="utf-8") as summary_file, redirect_stdout(
            summary_file
        ):
            create_full_tear_sheet_func(alphalens_data)
    finally:
        plt.show = _original_show
        pdf_file.close()

    if saved_count[0] > 0:
        logger.info(f"報表已保存: {report_dir}")
    else:
        logger.warning("未偵測到 Alphalens 產生的圖表")
        report_dir = None

    return report_dir

