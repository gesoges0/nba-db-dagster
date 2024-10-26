import json
import os
from pathlib import Path

LOCAL_SAVE_DIR = Path("./results")
LOCAL_RAW_DIR = Path("./raw_data")


def save_result(result: dict, save_name: str) -> None:
    if os.environ.get("ENV") == "CF":
        pass
    else:
        save_path = LOCAL_SAVE_DIR / (save_name + ".json")
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, mode="w") as f:
            json.dump(result, f)


def load_result(save_name: str) -> dict:  # type: ignore
    if os.environ.get("ENV") == "CF":
        pass
    else:
        save_path = LOCAL_SAVE_DIR / (save_name + ".json")
        with open(save_path, mode="r") as f:
            return json.load(f)


def save_as_jsonl(list_of_dict: list[dict], save_name: str) -> None:
    """JSONL形式で保存"""
    if os.environ.get("ENV") == "CF":
        pass
    else:
        save_path = LOCAL_RAW_DIR / (save_name + ".jsonl")
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, mode="w") as f:
            for d in list_of_dict:
                json.dump(d, f)
                f.write("\n")


def get_jsonl(save_name: str) -> list[dict]:
    with open(LOCAL_RAW_DIR / (save_name + ".jsonl"), mode="r") as f:
        return [json.loads(line) for line in f]
