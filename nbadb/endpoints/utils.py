import json
import os
from pathlib import Path

STATS_BEGINNING_YEAR = 1946
SEASON_TYPES = {"regular": "Regular Season", "playoffs": "Playoffs"}  # preseason, all_starは除外
PLAYER_OR_TEAM_ABBREVIATIONS = ["T", "P"]  # T: team, P: player


LOCAL_SAVE_DIR = Path("./results")
LOCAL_RAW_DIR = Path("./raw_data")


def _get_result_local_save_path(save_name: str) -> Path:
    return LOCAL_SAVE_DIR / (save_name + ".json")


def _get_raw_local_save_path(save_name: str) -> Path:
    return LOCAL_RAW_DIR / (save_name + ".jsonl")


def save_result(result: dict, save_name: str) -> None:
    if os.environ.get("ENV") == "CF":
        pass
    else:
        save_path = _get_result_local_save_path(save_name)
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
    # FIXME: dataflowに移植したほうがよい
    if os.environ.get("ENV") == "CF":
        pass
    else:
        save_path = _get_raw_local_save_path(save_name)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, mode="w") as f:
            for d in list_of_dict:
                json.dump(d, f)
                f.write("\n")


def get_jsonl(save_name: str) -> list[dict]:
    with open(LOCAL_RAW_DIR / (save_name + ".jsonl"), mode="r") as f:
        return [json.loads(line) for line in f]


def is_exists(file_name, file_type):
    assert file_type in ["raw", "result"], "file_type must be 'raw' or 'result'"

    if os.environ.get("ENV") == "CF":
        pass

    else:
        if file_type == "raw":
            target_path = _get_raw_local_save_path(file_name)
        else:
            target_path = _get_result_local_save_path(file_name)

        return target_path.exists()


def build_season_name(year: int) -> str:
    return f"{year}-{str(year+1)[2:]}"
