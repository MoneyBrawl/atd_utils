from typing import List, Dict, Callable  # , Optional, Union
import pandas as pd
import numpy as np

# from tqdm import tqdm


def pull_over_season_types_and_weeks(
    request_func: Callable[[Dict, str, bool, bool], List[Dict]],  # noqa
    api_name: str,  # noqa
    endpoint_name: str,  # noqa
    request_params: Dict,
) -> List[Dict]:  # noqa
    results = []
    print(f"year:{request_params['year']}")
    for season_type in ["regular", "postseason"]:
        for week in range(1, 17):
            request_params["week"] = week
            request_params["season_type"] = season_type
            result = request_func(api_name, endpoint_name, request_params)
            if result:
                results += result
                print(f"\tWeek:{week}\tseason_type:{season_type}")
    return results


def pull_over_season_types(
    request_func: Callable[[Dict, str, bool, bool], List[Dict]],  # noqa
    api_name: str,  # noqa
    endpoint_name: str,  # noqa
    request_params: Dict,
) -> List[Dict]:  # noqa
    results = []
    for season_type in ["regular", "postseason"]:
        request_params["season_type"] = season_type
        result = request_func(api_name, endpoint_name, request_params)
        if result:
            results += result
    return results


# def process_get_team_game_stats(data):
#     df = None
#     for game in data:
#         for team in game['teams']:
#             for stat in team['stats']:
#                 key = stat['category']
#                 value = stat['stat']
#                 team[key] = value or 0
#             team.pop('stats')
#         game_df = pd.json_normalize(game, 'teams', ['id'])
#         # game_df.set_index('id', inplace=True)
#         if df is None:
#             df = game_df
#         else:
#             df = pd.concat([df, game_df])
#     cols_to_fillna = [x for x in df.columns if x in ['points', 'rushingTDs',
#        'puntReturnYards', 'puntReturnTDs',
# 'puntReturns', 'passingTDs',  # noqa
#        'kickingPoints', 'fumblesRecovered',
# 'totalFumbles', 'tacklesForLoss',  # noqa
#        'defensiveTDs', 'tackles', 'sacks',
# 'qbHurries', 'passesDeflected',  # noqa
#        'possessionTime', 'interceptions',
# 'fumblesLost', 'turnovers',  # noqa
#        'totalPenaltiesYards', 'yardsPerRushAttempt',
# 'rushingAttempts',  # noqa
#        'rushingYards', 'yardsPerPass',
#  'completionAttempts', 'netPassingYards',  # noqa
#        'totalYards', 'fourthDownEff', 'thirdDownEff',
# 'firstDowns', 'id',  # noqa
#        'kickReturnYards', 'kickReturnTDs', 'kickReturns',
# 'interceptionYards',  # noqa
#        'interceptionTDs', 'passesIntercepted']]  # noqa
#     df[cols_to_fillna] = df[cols_to_fillna].fillna(0)
#     return df.reset_index(drop=True)


def load_games_load_process(df: pd.DataFrame) -> pd.DataFrame:
    home_line_scores_cols = [
        "home_q1_points",
        "home_q2_points",
        "home_q3_points",
        "home_q4_points",
    ]
    df[home_line_scores_cols] = df["home_line_scores"].apply(
        lambda x: pd.Series([np.nan] * 4)
        if x is None or len(x) == 0
        else pd.Series(x[:4])
    )
    df["home_ot_total_points"] = df["home_line_scores"].apply(
        lambda x: sum(x[4:]) if x is not None and len(x) > 4 else None
    )
    df["home_ot_points_per_period"] = df["home_line_scores"].apply(
        lambda x: str(x[4:]) if x is not None and len(x) > 4 else None
    )
    df["home_num_ot_periods"] = df["home_line_scores"].apply(
        lambda x: len(x[4:]) if x is not None and len(x) > 4 else None
    )
    away_line_score_cols = [
        "away_q1_points",
        "away_q2_points",
        "away_q3_points",
        "away_q4_points",
    ]
    df[away_line_score_cols] = df["away_line_scores"].apply(
        lambda x: pd.Series([np.nan] * 4)
        if x is None or len(x) == 0
        else pd.Series(x[:4])
    )
    df["away_ot_total_points"] = df["away_line_scores"].apply(
        lambda x: sum(x[4:]) if x is not None and len(x) > 4 else None
    )
    df["away_ot_points_per_period"] = df["away_line_scores"].apply(
        lambda x: str(x[4:]) if x is not None and len(x) > 4 else None
    )
    df["away_num_ot_periods"] = df["away_line_scores"].apply(
        lambda x: len(x[4:]) if x is not None and len(x) > 4 else None
    )
    df.drop("home_line_scores", axis=1, inplace=True)
    df.drop("away_line_scores", axis=1, inplace=True)

    df["game_date"] = pd.to_datetime(df.start_date)
    df["season_year"] = df.game_date.apply(
        lambda x: pd.to_datetime(x).year - 1
        if pd.to_datetime(x).month < 6
        else pd.to_datetime(x).year
    )
    df.drop("start_date", axis=1, inplace=True)
    return df


def get_player_game_stats_pre_pandas_load_process(
    data: List[Dict],
) -> List[Dict]:  # noqa
    rows = []
    for game in data:
        game_id = game["id"]
        teams = game["teams"]
        for team in teams:
            categories = team["categories"]
            if "home_away" in team.keys():
                home_away = team["home_away"]
                team_name = team["school"]["name"]
                conference = team["school"]["conference"]
            else:
                home_away = team["homeAway"]
                team_name = team["school"]
                conference = team["conference"]
            categories = team["categories"]
            for category in categories:
                name = category["name"]
                types = category["types"]
                for typ in types:
                    typ_name = typ["name"]
                    athletes = typ["athletes"]
                    for athlete in athletes:
                        athlete_id = athlete["id"]
                        athlete_name = athlete["name"]
                        stat = athlete["stat"]
                        row = {
                            "game_id": game_id,
                            "home_away": "home" if home_away else "away",
                            "team_name": team_name,
                            "conference": conference,
                            "category": name,
                            "type": typ_name,
                            "athlete_id": athlete_id,
                            "athlete_name": athlete_name,
                            "stat": stat,
                        }
                        rows.append(row)
    return rows


# def pull_get_player_game_stats(
#                 request_func: Callable[[Dict, str, bool, bool], List[Dict]], # noqa
#                 api_name: str,  # noqa
#                 endpoint_name: str,  # noqa
#                 request_params: Dict,
#                 teams: pd.DataFrame) -> List[Dict]:  # noqa
#     results = []
#     for season_type in ['regular', 'postseason']:
#         for week in tqdm(range(1, 17)):
#             request_params['season_type'] = season_type
#             request_params['week'] = week
#             # request_params['team'] = team
#             print(request_params)
#             result = request_func(api_name, endpoint_name, request_params)
#             if result:
#                 results += result
#     return results


def get_rankings_load_process(df):
    # Extract the common keys
    exploded_first = df.explode("polls")
    normalized_polls = pd.json_normalize(exploded_first["polls"])
    recombined = pd.concat(
        [
            exploded_first.reset_index(drop=True),
            normalized_polls.reset_index(drop=True),
        ],
        axis=1,
    )
    exploded_ranks = recombined.explode("ranks")
    normalized_ranks = pd.json_normalize(exploded_ranks["ranks"])
    final_recombined = pd.concat(
        [
            exploded_ranks.reset_index(drop=True),
            normalized_ranks.reset_index(drop=True),
        ],
        axis=1,
    )
    return_cols = [
        col for col in final_recombined.columns if col not in ["polls", "ranks"]  # noqa
    ]  # noqa
    return final_recombined[return_cols]


ENDPOINTS_DICT = {
    "get_recruiting_players": {"pull": {"api": "RecruitingApi"}},
    "get_team_game_stats": {
        "pull": {
            "api": "GamesApi",
            "pull_func": pull_over_season_types_and_weeks,
        }  # noqa
    },
    "get_draft_picks": {"pull": {"api": "DraftApi"}},
    "get_coaches": {
        "pull": {
            "api": "CoachesApi",
        },
        "load": {
            "record_path": ["seasons"],
            "meta": ["first_name", "last_name", "hire_date"],
        },
    },
    "get_games": {
        "pull": {
            "api": "GamesApi",
            "pull_func": pull_over_season_types,
        },
        "load": {"df_load_process": load_games_load_process},
    },
    "get_player_game_stats": {
        "pull": {
            "api": "GamesApi",
            "pull_func": pull_over_season_types_and_weeks,
        },
        "load": {
            "file_name_fields": {
                "year": (-3, int),
                "season_type": (-2, str),
                "week": (-1, int),
            },
            "pre_pandas_load_process": get_player_game_stats_pre_pandas_load_process,  # noqa
        },
    },
    "get_fbs_teams": {
        "pull": {
            "api": "TeamsApi",
        },
        "load": {"stringify_lists": ["logos"]},
    },
    "get_rankings": {
        "pull": {
            "api": "RankingsApi",
            "pull_func": pull_over_season_types,
        },
        "load": {"df_load_process": get_rankings_load_process},
    },
    # #########################################################################
    # Nothing implemented
    # #########################################################################
    "get_lines": {"api": "BettingApi"},
    "get_conferences": {"api": "ConferencesApi"},
    "get_nfl_positions": {"api": "DraftApi"},
    "get_nfl_teams": {"api": "DraftApi"},
    "get_drives": {"api": "DrivesApi"},
    "get_advanced_box_score": {"api": "GamesApi"},
    "get_calendar": {"api": "GamesApi"},
    "get_game_media": {"api": "GamesApi"},
    "get_game_weather": {"api": "GamesApi"},
    "get_scoreboard": {"api": "GamesApi"},
    "get_team_records": {"api": "GamesApi"},
    "get_game_ppa": {"api": "MetricsApi"},
    "get_player_game_ppa": {"api": "MetricsApi"},
    "get_player_season_ppa": {"api": "MetricsApi"},
    "get_predicted_points": {"api": "MetricsApi"},
    "get_pregame_win_probabilities": {"api": "MetricsApi"},
    "get_team_ppa": {"api": "MetricsApi"},
    "get_win_probability_data": {"api": "MetricsApi"},
    "get_player_season_stats": {"api": "PlayersApi"},
    "get_player_usage": {"api": "PlayersApi"},
    "get_returning_production": {"api": "PlayersApi"},
    "get_transfer_portal": {"api": "PlayersApi"},
    "player_search": {"api": "PlayersApi"},
    "get_live_plays": {"api": "PlaysApi"},
    "get_play_stat_types": {"api": "PlaysApi"},
    "get_play_stats": {"api": "PlaysApi"},
    "get_play_types": {"api": "PlaysApi"},
    "get_plays": {"api": "PlaysApi"},
    "get_conference_sp_ratings": {"api": "RatingsApi"},
    "get_elo_ratings": {"api": "RatingsApi"},
    "get_sp_ratings": {"api": "RatingsApi"},
    "get_srs_ratings": {"api": "RatingsApi"},
    "get_recruiting_groups": {"api": "RecruitingApi"},
    "get_recruiting_teams": {"api": "RecruitingApi"},
    "get_advanced_team_game_stats": {"api": "StatsApi"},
    "get_advanced_team_season_stats": {"api": "StatsApi"},
    "get_stat_categories": {"api": "StatsApi"},
    "get_team_season_stats": {"api": "StatsApi"},
    "get_roster": {"api": "TeamsApi"},
    "get_talent": {"api": "TeamsApi"},
    "get_team_matchup": {"api": "TeamsApi"},
    "get_teams": {"api": "TeamsApi"},
    "get_venues": {"api": "VenuesApi"},
}
