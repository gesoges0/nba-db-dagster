import json
import os
from pathlib import Path

LOCAL_SAVE_DIR = Path("./results")


def save_result(result: dict, save_name: str) -> None:
    if os.environ.get("ENV") == "CF":
        pass
    else:
        LOCAL_SAVE_DIR.mkdir(exist_ok=True)
        save_path = save_name + ".json"
        with open(file=LOCAL_SAVE_DIR / save_path, mode="w") as f:
            json.dump(result, f)
