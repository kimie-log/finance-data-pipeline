"""
列出可用因子工具

自動從 FinLab API 抓取因子列表並生成 JSON 檔案到 factors/ 資料夾。

執行：
    # 自動從 API 抓取並保存到 factors/factors_list.json（覆蓋）
    python -m factors.list_factors

    # 查詢特定類型的因子
    python -m factors.list_factors --type fundamental_features

    # 只顯示不保存
    python -m factors.list_factors --no-save

    # 從本地 JSON 檔案讀取（不抓取 API）
    python -m factors.list_factors --from-local
"""
import sys
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

load_dotenv()

try:
    from factors.finlab_factor_fetcher import FinLabFactorFetcher
    from ingestion.finlab_fetcher import FinLabFetcher
except ImportError as e:
    print(f"錯誤：無法匯入模組 - {e}")
    sys.exit(1)


def list_factors_from_json(json_path: Path) -> dict:
    """
    從本地 JSON 檔案讀取因子列表

    Args:
        json_path: JSON 檔案路徑

    Returns:
        因子字典，key 為類型，value 為因子名稱列表（已清理前綴）
    """
    if not json_path.exists():
        print(f"警告：找不到檔案 {json_path}")
        return {}

    with open(json_path, "r", encoding="utf-8") as f:
        factors_dict = json.load(f)

    # 清理因子名稱，移除前綴
    cleaned_dict = {}
    for factor_type, factors in factors_dict.items():
        if isinstance(factors, list):
            cleaned_dict[factor_type] = [
                clean_factor_name(factor, factor_type) for factor in factors
            ]
        else:
            cleaned_dict[factor_type] = factors

    return cleaned_dict


def clean_factor_name(factor_name: str, data_type: str = "fundamental_features") -> str:
    """
    清理因子名稱，移除前綴（如 "fundamental_features:"）

    Args:
        factor_name: 原始因子名稱
        data_type: 資料類型關鍵字

    Returns:
        清理後的因子名稱
    """
    # 移除 "fundamental_features:" 或類似的前綴
    prefix = f"{data_type}:"
    if factor_name.startswith(prefix):
        return factor_name[len(prefix):]
    return factor_name


def list_factors_from_api(data_type: str = "fundamental_features") -> list:
    """
    從 FinLab API 查詢因子列表

    Args:
        data_type: 資料類型關鍵字（預設 "fundamental_features"）

    Returns:
        因子名稱列表（已清理前綴）
    """
    try:
        FinLabFetcher.finlab_login()
        factors = FinLabFactorFetcher.list_factors_by_type(data_type)
        # 清理因子名稱，移除前綴
        cleaned_factors = [clean_factor_name(factor, data_type) for factor in factors]
        return cleaned_factors
    except Exception as e:
        print(f"錯誤：無法從 FinLab API 查詢因子 - {e}")
        print("請確認已設定 FINLAB_API_TOKEN 環境變數")
        return []


def print_factors(factors_dict: dict, source: str = "JSON"):
    """
    格式化輸出因子列表

    Args:
        factors_dict: 因子字典
        source: 資料來源（用於顯示）
    """
    print(f"\n{'='*60}")
    print(f"可用因子列表（來源：{source}）")
    print(f"{'='*60}\n")

    if not factors_dict:
        print("未找到任何因子")
        return

    total_count = 0
    for factor_type, factors in factors_dict.items():
        if isinstance(factors, list):
            count = len(factors)
            total_count += count
            print(f"【{factor_type}】({count} 個因子)")
            print("-" * 60)
            for i, factor in enumerate(factors, 1):
                print(f"  {i:3d}. {factor}")
            print()

    print(f"{'='*60}")
    print(f"總計：{total_count} 個因子")
    print(f"{'='*60}\n")


def save_factors_to_json(factors_dict: dict, json_path: Path) -> bool:
    """
    將因子字典保存到 JSON 檔案

    Args:
        factors_dict: 因子字典
        json_path: JSON 檔案路徑

    Returns:
        True 成功；False 失敗
    """
    try:
        # 確保目錄存在
        json_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存 JSON 檔案（格式化輸出，中文不轉義）
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(factors_dict, f, ensure_ascii=False, indent=4)
        
        print(f"\n✅ 因子列表已保存至：{json_path}")
        return True
    except Exception as e:
        print(f"\n❌ 保存失敗：{e}")
        return False


def main() -> int:
    """
    主函數：自動從 API 抓取因子列表並生成 JSON 檔案

    Returns:
        0 成功；1 失敗
    """
    parser = argparse.ArgumentParser(description="列出可用因子並自動保存到 JSON")
    parser.add_argument(
        "--type",
        default="fundamental_features",
        help="因子類型關鍵字（預設：fundamental_features）",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="只顯示不保存 JSON 檔案",
    )
    parser.add_argument(
        "--from-local",
        action="store_true",
        help="從本地 JSON 檔案讀取（不抓取 API）",
    )
    parser.add_argument(
        "--json-path",
        type=str,
        default=str(ROOT_DIR / "factors" / "factors_list.json"),
        help="JSON 檔案路徑（預設：factors/factors_list.json）",
    )

    args = parser.parse_args()
    json_path = Path(args.json_path)

    if args.from_local:
        # 從本地 JSON 讀取
        factors_dict = list_factors_from_json(json_path)
        if factors_dict:
            print_factors(factors_dict, "本地 JSON")
        else:
            print(f"❌ 未找到本地 JSON 檔案：{json_path}")
            return 1
    else:
        # 自動從 API 查詢
        print("正在從 FinLab API 查詢因子...")
        factors_list = list_factors_from_api(args.type)
        if not factors_list:
            print("❌ 無法從 API 取得因子列表")
            # 嘗試從本地讀取作為備用
            factors_dict = list_factors_from_json(json_path)
            if factors_dict:
                print("\n⚠️  使用本地備用資料：")
                print_factors(factors_dict, "本地 JSON（備用）")
            return 1
        
        factors_dict = {args.type: factors_list}
        print_factors(factors_dict, "FinLab API")
        
        # 保存到 JSON 檔案（除非指定 --no-save）
        if not args.no_save:
            if not save_factors_to_json(factors_dict, json_path):
                return 1

    # 顯示使用建議
    print("\n💡 使用建議：")
    print("  1. 因子列表已自動更新到 factors/factors_list.json")
    print("  2. 在多因子分析時使用：")
    print("     python -m scripts.run_multi_factor_analysis \\")
    print("         --dataset <dataset_id> \\")
    print("         --factors <因子名稱逗號分隔> \\")
    print("         --start <start_date> \\")
    print("         --end <end_date>")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
