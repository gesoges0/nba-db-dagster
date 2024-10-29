from datetime import datetime

from nba_api.live.nba.endpoints import scoreboard


def get_nba_season(date_str):
    # 日付の入力 (例: "2024-10-28")
    date = datetime.strptime(date_str, "%Y-%m-%d")

    # 10月以降であれば、その年がシーズンの開始年
    if date.month >= 10:
        season_start = date.year
    else:
        season_start = date.year - 1

    # シーズンの文字列を作成 (例: "2024-25")
    season = f"{season_start}-{str(season_start + 1)[-2:]}"
    return season


def format_season_year(year):
    """
    シーズン年を正しい形式に変換する関数

    Args:
        year (int): シーズン開始年

    Returns:
        str: フォーマットされたシーズン表記 (例: "1999-00", "2023-24")
    """
    start_year = str(year)
    end_year = str((year + 1) % 100).zfill(2)
    return f"{start_year}-{end_year}"


def get_current_nba_season():
    """
    現在のNBAシーズンを取得する関数

    Returns:
        str: 現在のシーズン (例: "1999-00", "2023-24")
    """
    try:
        # スコアボードから現在のシーズン情報を取得
        board = scoreboard.ScoreBoard()
        data = board.get_dict()

        # まずはゲーム情報を取得
        if "games" in data and len(data["games"]) > 0:
            current_season = int(data["games"][0]["gameStatus"]["season"])
        else:
            # ゲーム情報が取得できない場合は現在の日付から推定
            current_date = datetime.now()
            # 7月以前なら前年をシーズン開始年とする
            if current_date.month <= 7:
                current_season = current_date.year - 1
            else:
                current_season = current_date.year

        # シーズン表記を整形
        formatted_season = format_season_year(current_season)

        return formatted_season

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        # エラー時のデバッグ情報
        print("\nデバッグ情報:")
        try:
            print("API Response:", data)
        except:
            pass
        return None


def main():
    # メイン処理
    season = get_current_nba_season()
    if season:
        print(f"現在のNBAシーズン: {season}")

    # テスト用のコード
    test_years = [1999, 2000, 2009, 2010, 2023]
    print("\nテスト出力:")
    for year in test_years:
        formatted = format_season_year(year)
        print(f"{year}年開始シーズン: {formatted}")


if __name__ == "__main__":
    main()
