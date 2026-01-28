import sys
from pathlib import Path
from utils.logger import logger

def check_gcp_environment(root_dir: Path):
    key_dir = root_dir / "gcp_keys"
    
    # 確保資料夾存在
    key_dir.mkdir(parents=True, exist_ok=True)
    
    # 安全性檢查：防止金鑰被 Git 追蹤
    gitignore = key_dir / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("*.json\n")

    # 檢查金鑰是否存在
    json_keys = list(key_dir.glob("*.json"))
    
    if not json_keys:
        logger.warning(f"⚠️  尚未偵測到金鑰。請將 GCP 服務帳戶 JSON 檔案放入：{key_dir}")
        # 如果是互動式環境，可以在這裡暫停或提示
        sys.exit(1) 
    
    # 取得最新的金鑰路徑（按修改時間排序，確保拿最新的一張）
    latest_key = max(json_keys, key=lambda p: p.stat().st_mtime)
    
    return latest_key.name