# -*- coding: utf-8 -*-
import os
import cfbd
from cfbd.rest import ApiException

ENDPOINT_DICT = {
    "get_lines": "BettingApi",
    "get_coaches": "CoachesApi",
    "get_conferences": "ConferencesApi",
    "get_draft_picks": "DraftApi",
    "get_nfl_positions": "DraftApi",
    "get_nfl_teams": "DraftApi",
    "get_drives": "DrivesApi",
    "get_advanced_box_score": "GamesApi",
    "get_calendar": "GamesApi",
    "get_game_media": "GamesApi",
    "get_game_weather": "GamesApi",
    "get_games": "GamesApi",
    "get_player_game_stats": "GamesApi",
    "get_scoreboard": "GamesApi",
    "get_team_game_stats": "GamesApi",
    "get_team_records": "GamesApi",
    "get_game_ppa": "MetricsApi",
    "get_player_game_ppa": "MetricsApi",
    "get_player_season_ppa": "MetricsApi",
    "get_predicted_points": "MetricsApi",
    "get_pregame_win_probabilities": "MetricsApi",
    "get_team_ppa": "MetricsApi",
    "get_win_probability_data": "MetricsApi",
    "get_player_season_stats": "PlayersApi",
    "get_player_usage": "PlayersApi",
    "get_returning_production": "PlayersApi",
    "get_transfer_portal": "PlayersApi",
    "player_search": "PlayersApi",
    "get_live_plays": "PlaysApi",
    "get_play_stat_types": "PlaysApi",
    "get_play_stats": "PlaysApi",
    "get_play_types": "PlaysApi",
    "get_plays": "PlaysApi",
    "get_rankings": "RankingsApi",
    "get_conference_sp_ratings": "RatingsApi",
    "get_elo_ratings": "RatingsApi",
    "get_sp_ratings": "RatingsApi",
    "get_srs_ratings": "RatingsApi",
    "get_recruiting_groups": "RecruitingApi",
    "get_recruiting_players": "RecruitingApi",
    "get_recruiting_teams": "RecruitingApi",
    "get_advanced_team_game_stats": "StatsApi",
    "get_advanced_team_season_stats": "StatsApi",
    "get_stat_categories": "StatsApi",
    "get_team_season_stats": "StatsApi",
    "get_fbs_teams": "TeamsApi",
    "get_roster": "TeamsApi",
    "get_talent": "TeamsApi",
    "get_team_matchup": "TeamsApi",
    "get_teams": "TeamsApi",
    "get_venues": "VenuesApi"
}

# Configure API key authorization: ApiKeyAuth
CONFIGURATION = cfbd.Configuration()

class APIKeyError(Exception):
    pass

class EndpointNotValid(Exception):
    pass

class CfbdClient(object):
    def __init__(self, api_key=None, data_dir=None):
        if api_key:
            CONFIGURATION.api_key['Authorization'] = api_key
        elif 'CFBD_API_KEY' in os.environ:
            CONFIGURATION.api_key['Authorization'] = os.environ['CFBD_API_KEY']
        else:
            raise APIKeyError('API key not provided and CFBD_API_KEY environment variable not set.')
        
        CONFIGURATION.api_key_prefix['Authorization'] = 'Bearer'
        
        self.data_dir = data_dir if data_dir else "data"
	
    
    def save_data(self, sub_dir, filename, data):
        """
        Save data to a file within a subdirectory.

        Args:
            sub_dir (str): The subdirectory to append to self.data_dir to save the data in.
            filename (str): The name of the file to save the data to.
            data (str): The data to save.
        """
        path = os.path.join(self.data_dir, sub_dir)
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f'{path}/{filename}', 'w') as f:
            f.write(data) 

    def pull_data(year, endpoint_name):
        """
        Retrieve data from a specified College Football Data API endpoint for a given year.

        Args:
            year (int): The year for which to fetch data.
            endpoint_name (str): The name of the endpoint method to call on the API instance.
                                This should match one of the keys in ENDPOINT_DICT.

        Returns:
            list: A list of dictionaries containing the fetched data for the specified year and endpoint.

        Examples:
            >>> pull_data(2021, "get_games")
        """
        try:
            client_name = ENDPOINT_DICT[endpoint_name]
        except KeyError:
            raise EndpointNotValid('This endpoint does not exist or has not been setup yet.')
        api_instance = getattr(cfbd, client_name)(cfbd.ApiClient(CONFIGURATION))
        endpoint_method = getattr(api_instance, endpoint_name)
        api_response = endpoint_method(year=year)
        results = [x.to_dict() for x in api_response]
        return results