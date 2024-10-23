import json
import os
from pathlib import Path

LOCAL_SAVE_DIR = Path("./results")
LOCAL_RAW_DIR = Path("./raw_data")


def save_result(result: dict, save_name: str) -> None:
    if os.environ.get("ENV") == "CF":
        pass
    else:
        LOCAL_SAVE_DIR.mkdir(exist_ok=True)
        save_path = save_name + ".json"
        with open(file=LOCAL_SAVE_DIR / save_path, mode="w") as f:
            json.dump(result, f)


def load_result(save_name: str) -> dict:  # type: ignore
    if os.environ.get("ENV") == "CF":
        pass
    else:
        save_path = save_name + ".json"
        with open(file=LOCAL_SAVE_DIR / save_path, mode="r") as f:
            return json.load(f)


def save_as_jsonl(list_of_dict: list[dict], save_name: str) -> None:
    """JSONL形式で保存"""
    if os.environ.get("ENV") == "CF":
        pass
    else:
        LOCAL_RAW_DIR.mkdir(exist_ok=True)
        save_path = save_name + ".jsonl"
        with open(file=LOCAL_RAW_DIR / save_path, mode="w") as f:
            for d in list_of_dict:
                json.dump(d, f)
                f.write("\n")
