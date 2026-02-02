"""
CLI 參數解析與設定檔合併工具。

預設全部使用 config/settings.yaml；僅在 CLI 有加參數時才覆寫該項。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

import yaml


def parse_args(config: Optional[dict] = None) -> argparse.Namespace:
    """
    解析 ETL pipeline 的命令列參數。若傳入 config，未加參數時預設取自 config。

    Returns:
        argparse.Namespace；未傳的參數為 None（由 resolve_params 以 config 補齊）。

    Note:
        - 預設使用 config/settings.yaml，僅在 CLI 有加該參數時覆寫。
        - --excluded-industry 可重複指定（action="append"）。
        - 布林旗標（--skip-gcs 等）：有加則為 True，未加則用 config。
    """
    etl = (config or {}).get("etl", {})
    yf = etl.get("yfinance", {})
    top = etl.get("top_stocks", {})
    bq = etl.get("bigquery", {})
    fac = etl.get("factors", {})

    parser = argparse.ArgumentParser(
        description="ETL pipeline for Taiwan stock data (FinLab + yfinance). 預設使用 config/settings.yaml。"
    )
    parser.add_argument(
        "--market-value-date",
        default=etl.get("market_value_date") if config else None,
        help="單一市值基準日期 (YYYY-MM-DD)；未指定時用 config.etl.market_value_date",
    )
    parser.add_argument(
        "--market-value-dates",
        default=None,
        help="多個市值基準日期，逗號分隔；未指定時用 config.etl.market_value_dates",
    )
    parser.add_argument(
        "--start",
        default=yf.get("start") if config else None,
        help="價量區間起始日 (YYYY-MM-DD)；未指定時用 config.etl.yfinance.start",
    )
    parser.add_argument(
        "--end",
        default=yf.get("end") if config else None,
        help="價量區間結束日 (YYYY-MM-DD)；未指定時用 config.etl.yfinance.end",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=top.get("top_n") if config else None,
        help="市值前 N 大；未指定時用 config.etl.top_stocks.top_n",
    )
    parser.add_argument(
        "--excluded-industry",
        action="append",
        dest="excluded_industry",
        default=None,
        help="排除產業（可重複）；未指定時用 config.etl.top_stocks.excluded_industry",
    )
    parser.add_argument(
        "--pre-list-date",
        default=top.get("pre_list_date") if config else None,
        help="上市日期須早於此日期；未指定時用 config.etl.top_stocks.pre_list_date",
    )
    parser.add_argument(
        "--dataset",
        default=bq.get("dataset") if config else None,
        help="BigQuery dataset ID；未指定時用 config.etl.bigquery.dataset",
    )
    parser.add_argument(
        "--skip-gcs",
        action="store_true",
        help="略過上傳 GCS；未加則用 config.etl.skip_gcs",
    )
    parser.add_argument(
        "--with-factors",
        action="store_true",
        help="一併寫入財報因子；未加則用 config.etl.with_factors",
    )
    parser.add_argument(
        "--skip-benchmark",
        action="store_true",
        help="略過基準指數；未加則用 config.etl.skip_benchmark",
    )
    parser.add_argument(
        "--skip-calendar",
        action="store_true",
        help="略過交易日曆；未加則用 config.etl.skip_calendar",
    )
    parser.add_argument(
        "--factor-table-suffix",
        default=fac.get("factor_table_suffix") if config else None,
        help="因子表名後綴；未指定時用 config.etl.factors.factor_table_suffix",
    )
    parser.add_argument(
        "--factor-names",
        default=None,
        help="因子名稱，逗號分隔；未指定時用 config.etl.factors.factor_names 或 factors_list.json",
    )
    parser.add_argument(
        "--factors-list",
        default=None,
        help="factors_list.json 路徑；給定則載入 fundamental_features 作為 factor_names",
    )
    return parser.parse_args()


def load_config(root_dir: Path) -> dict:
    """
    從 config/settings.yaml 讀取設定檔並回傳 dict。

    Args:
        root_dir: 專案根目錄，用於定位 config/settings.yaml。

    Returns:
        設定檔內容的 dict；檔案不存在或格式錯誤時可能拋出 yaml 相關例外。

    Note:
        使用 yaml.safe_load 避免執行任意程式碼；建議設定檔結構對齊 README 說明。
    """
    config_path = root_dir / "config/settings.yaml"
    return yaml.safe_load(open(config_path))


def _load_factor_names_from_json(path: Path) -> list:
    """從 factors_list.json 載入 fundamental_features，失敗則回傳空列表。"""
    if not path or not Path(path).exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("fundamental_features", []) or []
    except Exception:
        return []


def resolve_params(config: dict, args: argparse.Namespace, root_dir: Optional[Path] = None) -> dict:
    """
    合併設定檔與 CLI 參數：預設使用 config，僅在 CLI 有傳入該項時覆寫；處理 dataset 模板與市值日。

    Args:
        config: 設定檔 dict（來自 load_config）。
        args: CLI 參數（來自 parse_args）。

    Returns:
        合併後的參數 dict，包含 market_value_dates、market_value_date、start_date、end_date、
        top_n、excluded_industry、pre_list_date、dataset_id、skip_gcs、with_factors、
        factor_names、benchmark_index_ids、backtest_config、skip_benchmark、skip_calendar、
        factor_table_suffix。

    Note:
        - market_value_dates 為列表（單一日期時為 [date]），market_value_date 為第一個元素。
        - dataset_id 支援 {top_n} / {_top_n} 模板代換，用於動態命名。
        - factor_table_suffix：CLI 優先於設定檔。
    """
    etl_cfg = config.get("etl", {})
    top_stocks_cfg = etl_cfg.get("top_stocks", {})
    yfinance_cfg = etl_cfg.get("yfinance", {})
    bigquery_cfg = etl_cfg.get("bigquery", {})

    # market_value_dates: CLI 優先，否則 config.etl
    if getattr(args, "market_value_dates", None) and args.market_value_dates.strip():
        market_value_dates = [d.strip() for d in args.market_value_dates.split(",") if d.strip()]
    elif getattr(args, "market_value_date", None) and args.market_value_date:
        market_value_dates = [args.market_value_date]
    elif etl_cfg.get("market_value_dates"):
        market_value_dates = (
            etl_cfg["market_value_dates"]
            if isinstance(etl_cfg["market_value_dates"], list)
            else [etl_cfg["market_value_dates"]]
        )
    elif etl_cfg.get("market_value_date"):
        market_value_dates = [etl_cfg["market_value_date"]]
    else:
        market_value_dates = []

    excluded_industry = (
        args.excluded_industry
        if args.excluded_industry is not None
        else top_stocks_cfg.get("excluded_industry", [])
    )
    pre_list_date = args.pre_list_date or top_stocks_cfg.get("pre_list_date")
    top_n = args.top_n if args.top_n is not None else top_stocks_cfg.get("top_n", 50)

    start_date = args.start or yfinance_cfg.get("start")
    end_date = args.end or yfinance_cfg.get("end")

    dataset_id = args.dataset or bigquery_cfg.get("dataset")
    if isinstance(dataset_id, str):
        dataset_id = dataset_id.replace("{top_n}", str(top_n)).replace("{_top_n}", str(top_n))

    factors_cfg = etl_cfg.get("factors", {})
    benchmark_cfg = etl_cfg.get("benchmark", {})
    backtest_cfg = etl_cfg.get("backtest_config", {})

    # 布林旗標：有加該參數則用 True，未加則用 config.etl
    skip_gcs = args.skip_gcs if args.skip_gcs else etl_cfg.get("skip_gcs", False)
    with_factors = args.with_factors if args.with_factors else etl_cfg.get("with_factors", False)
    skip_benchmark = args.skip_benchmark if args.skip_benchmark else etl_cfg.get("skip_benchmark", False)
    skip_calendar = args.skip_calendar if args.skip_calendar else etl_cfg.get("skip_calendar", False)
    factor_table_suffix = (
        getattr(args, "factor_table_suffix", None) or factors_cfg.get("factor_table_suffix")
    )

    # factor_names: CLI --factor-names > config > factors_list.json（--factors-list 或預設 factors/factors_list.json）
    factor_names_raw = getattr(args, "factor_names", None)
    if factor_names_raw and isinstance(factor_names_raw, str) and factor_names_raw.strip():
        factor_names = [f.strip() for f in factor_names_raw.split(",") if f.strip()]
    elif factors_cfg.get("factor_names"):
        factor_names = factors_cfg["factor_names"] if isinstance(factors_cfg["factor_names"], list) else [factors_cfg["factor_names"]]
    else:
        factors_list_path = getattr(args, "factors_list", None) or factors_cfg.get("factors_list")
        if not factors_list_path and root_dir:
            default_path = root_dir / "factors" / "factors_list.json"
            if default_path.exists():
                factors_list_path = str(default_path)
        factor_names = _load_factor_names_from_json(Path(factors_list_path) if factors_list_path else None)

    return {
        "market_value_dates": market_value_dates,
        "market_value_date": market_value_dates[0] if market_value_dates else None,
        "excluded_industry": excluded_industry,
        "pre_list_date": pre_list_date,
        "top_n": top_n,
        "start_date": start_date,
        "end_date": end_date,
        "dataset_id": dataset_id,
        "skip_gcs": skip_gcs,
        "with_factors": with_factors,
        "factor_names": factor_names,
        "benchmark_index_ids": benchmark_cfg.get("index_ids", ["^TWII"]),
        "backtest_config": backtest_cfg,
        "skip_benchmark": skip_benchmark,
        "skip_calendar": skip_calendar,
        "factor_table_suffix": factor_table_suffix,
    }


def resolve_multi_factor_params(config: dict, args: argparse.Namespace) -> dict:
    """
    合併設定檔與 CLI 參數（多因子分析）：預設使用 config，僅在 CLI 有傳入該項時覆寫。

    Args:
        config: 設定檔 dict（來自 load_config）。
        args: CLI 參數（來自 run_multi_factor_analysis 的 parser）。

    Returns:
        合併後的參數 dict，鍵為 snake_case（dataset, start, end, local_price, quantiles,
        periods, factor_table, auto_find_local, from_finlab_api, mode, factors, combo_size,
        weights, positive_corr, pcs, n_components）。
    """
    cfg = config.get("multi_factor_analysis", {})
    factors_raw = getattr(args, "factors", None)
    if factors_raw is not None and isinstance(factors_raw, str) and factors_raw.strip():
        factors = [f.strip() for f in factors_raw.split(",") if f.strip()]
    else:
        factors = cfg.get("factors") or []

    weights_raw = getattr(args, "weights", None)
    if weights_raw is not None and isinstance(weights_raw, str) and weights_raw.strip():
        weights = [float(w.strip()) for w in weights_raw.split(",") if w.strip()]
    else:
        weights = cfg.get("weights")

    pcs_raw = getattr(args, "pcs", None)
    if pcs_raw is not None and isinstance(pcs_raw, str) and pcs_raw.strip():
        pcs = [int(x.strip()) for x in pcs_raw.split(",") if x.strip()]
    else:
        pcs_str = cfg.get("pcs") or "2,4"
        pcs = [int(x.strip()) for x in pcs_str.split(",") if x.strip()]

    market_value_date = getattr(args, "market_value_date", None) or cfg.get("market_value_date")
    if not market_value_date and getattr(args, "start", None):
        market_value_date = getattr(args, "start")

    return {
        "dataset": getattr(args, "dataset", None) or cfg.get("dataset"),
        "start": getattr(args, "start", None) or cfg.get("start"),
        "end": getattr(args, "end", None) or cfg.get("end"),
        "market_value_date": market_value_date,
        "local_price": getattr(args, "local_price", None) or cfg.get("local_price"),
        "quantiles": getattr(args, "quantiles", None) if getattr(args, "quantiles", None) is not None else (cfg.get("quantiles") or 5),
        "periods": getattr(args, "periods", None) or cfg.get("periods") or "1,5,10",
        "factor_table": getattr(args, "factor_table", None) or cfg.get("factor_table") or "fact_factor",
        "auto_find_local": getattr(args, "auto_find_local", False) or cfg.get("auto_find_local", False),
        "from_finlab_api": getattr(args, "from_finlab_api", False) or cfg.get("from_finlab_api", False),
        "mode": getattr(args, "mode", None) or cfg.get("mode") or "weighted_rank",
        "factors": factors,
        "combo_size": getattr(args, "combo_size", None) if getattr(args, "combo_size", None) is not None else (cfg.get("combo_size") or 5),
        "weights": weights,
        "positive_corr": getattr(args, "positive_corr", None) if getattr(args, "positive_corr", None) is not None else cfg.get("positive_corr", True),
        "pcs": pcs,
        "n_components": getattr(args, "n_components", None) or cfg.get("n_components"),
        "skip_gcs": getattr(args, "skip_gcs", False) or cfg.get("skip_gcs", False),
    }


def resolve_multi_factor_backtest_params(config: dict, args: argparse.Namespace) -> dict:
    """
    合併設定檔與 CLI 參數（多因子回測）：預設使用 config.multi_factor_backtest，CLI 有傳則覆寫。

    Returns:
        合併後的參數 dict：dataset, factors, start, end, weights, local_price, local_factor,
        factor_table, positive_corr, buy_n, sell_n, initial_cash, commission。
    """
    cfg = config.get("multi_factor_backtest", {})

    factors_raw = getattr(args, "factors", None)
    if factors_raw is not None and isinstance(factors_raw, str) and factors_raw.strip():
        factors = [f.strip() for f in factors_raw.split(",") if f.strip()]
    else:
        factors = cfg.get("factors") or []
    if isinstance(factors, str):
        factors = [f.strip() for f in factors.split(",") if f.strip()]

    weights_raw = getattr(args, "weights", None)
    if weights_raw is not None and isinstance(weights_raw, str) and weights_raw.strip():
        weights = [float(w.strip()) for w in weights_raw.split(",") if w.strip()]
    else:
        weights = cfg.get("weights")
    if isinstance(weights, list) and not weights:
        weights = None

    return {
        "dataset": getattr(args, "dataset", None) or cfg.get("dataset"),
        "factors": factors,
        "start": getattr(args, "start", None) or cfg.get("start"),
        "end": getattr(args, "end", None) or cfg.get("end"),
        "weights": weights,
        "local_price": getattr(args, "local_price", None) or cfg.get("local_price"),
        "local_factor": getattr(args, "local_factor", None) or cfg.get("local_factor"),
        "auto_find_local": getattr(args, "auto_find_local", False) or cfg.get("auto_find_local", False),
        "factor_table": getattr(args, "factor_table", None) or cfg.get("factor_table") or "fact_factor",
        "positive_corr": False if getattr(args, "negative_corr", False) else cfg.get("positive_corr", True),
        "buy_n": getattr(args, "buy_n", None) if getattr(args, "buy_n", None) is not None else (cfg.get("buy_n") if cfg.get("buy_n") is not None else 20),
        "sell_n": getattr(args, "sell_n", None) if getattr(args, "sell_n", None) is not None else (cfg.get("sell_n") if cfg.get("sell_n") is not None else 20),
        "initial_cash": getattr(args, "initial_cash", None) if getattr(args, "initial_cash", None) is not None else (cfg.get("initial_cash") if cfg.get("initial_cash") is not None else 20_000_000),
        "commission": getattr(args, "commission", None) or cfg.get("commission") or 0.001,
    }
