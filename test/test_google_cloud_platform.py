import os
import time

import pytest

from utils.google_cloud_platform import check_gcp_environment


def test_check_gcp_environment_exits_when_missing_keys(tmp_path):
    with pytest.raises(SystemExit):
        check_gcp_environment(tmp_path)


def test_check_gcp_environment_returns_latest_key(tmp_path):
    key_dir = tmp_path / "gcp_keys"
    key_dir.mkdir(parents=True, exist_ok=True)

    first_key = key_dir / "first.json"
    second_key = key_dir / "second.json"
    first_key.write_text("{}")
    second_key.write_text("{}")

    now = time.time()
    os.utime(first_key, (now - 10, now - 10))
    os.utime(second_key, (now, now))

    result = check_gcp_environment(tmp_path)

    assert result == "second.json"
    gitignore = key_dir / ".gitignore"
    assert gitignore.exists()
    assert gitignore.read_text() == "*.json\n"
