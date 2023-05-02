# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
import cfbd
import sqlite3
from typing import List, Dict, Optional

with open("config/endpoints.json", "r") as f:
    ENDPOINT_DICT = json.load(f)


class APIKeyError(Exception):
    pass


class EndpointNotValid(Exception):
    pass


class EndpointLoadingNotImplemented(Exception):
    pass


class CfbdClient(object):
    def __init__(
        self,  # noqa
        api_key: Optional[str] = None,  # noqa
        data_dir: str = "data",  # noqa
        db: str = "cfbd.db",
    ) -> None:  # noqa
        self.cfbd_configuration = cfbd.Configuration()
        if api_key:
            self.cfbd_configuration.api_key["Authorization"] = api_key
        elif "CFBD_API_KEY" in os.environ:
            key = os.environ["CFBD_API_KEY"]
            self.cfbd_configuration.api_key["Authorization"] = key
        else:
            raise APIKeyError(
                """API key not provided and CFBD_API_KEY environment variable
                not set."""
            )

        self.cfbd_configuration.api_key_prefix["Authorization"] = "Bearer"

        self.data_dir = data_dir
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        self.conn = sqlite3.connect(os.path.join(data_dir, db))

    def save_data(self, sub_dir: str, filename: str, data: str) -> None:
        """
        Save data to a file within a subdirectory.

        Args:
            sub_dir (str): The subdirectory to append to self.data_dir to save
                            the data in.
            filename (str): The name of the file to save the data to.
            data (str): The data to save.
        """
        path = os.path.join(self.data_dir, sub_dir)
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}/{filename}", "w") as f:
            f.write(data)

    # TODO add years option, so you can pass a tuple of years and it will try
    # until failure on that range.
    def pull_data(self, year: int, endpoint_name: str) -> List[Dict]:
        """
        Retrieve data from a specified College Football Data API endpoint for
        a given year.

        Args:
            year (int): The year for which to fetch data.
            endpoint_name (str): The name of the endpoint method to call on
                                the API instance. This should match one of
                                the keys in ENDPOINT_DICT.

        Returns:
            list: A list of dictionaries containing the fetched data for the
            specified year and endpoint.

        Examples:
            >>> pull_data(2021, "get_games")
        """
        try:
            api_name = ENDPOINT_DICT["implemented"][endpoint_name]["api"]
        except KeyError:
            msg = "This endpoint does not exist or has not been setup yet."
            raise EndpointNotValid(msg)
        api = getattr(cfbd, api_name)
        api_instance = api(cfbd.ApiClient(self.cfbd_configuration))
        endpoint_method = getattr(api_instance, endpoint_name)
        api_response = endpoint_method(year=year)
        results = [x.to_dict() for x in api_response]
        self.save_data(
            endpoint_name,  # noqa
            f"{endpoint_name}_{year}.json",  # noqa
            json.dumps(results),
        )
        return results

    def load_to_df(
        self, endpoint_name: str, save_to_db: bool = False  # noqa
    ) -> pd.DataFrame:  # noqa
        try:
            endpoint_config = ENDPOINT_DICT["implemented"][endpoint_name]
        except KeyError:
            raise EndpointLoadingNotImplemented(
                f"""Either {endpoint_name} is not an endpoint, or I need to
                test and document in config/endpoints.json. whether
                json_normalize needs record_path and/or meta.
                """
            )
        record_path = endpoint_config.get("record_path", None)
        meta = endpoint_config.get("meta", None)
        endpoint_path = os.path.join(self.data_dir, endpoint_name)
        dir_list = os.listdir(endpoint_path)
        df = None
        for file in dir_list:
            if file[0] == ".":
                continue
            with open(os.path.join(endpoint_path, file), "r") as f:
                year = json.loads(f.read())
            year_df = pd.json_normalize(
                year, record_path=record_path, meta=meta  # noqa  # noqa
            )
            year_df["year"] = int(file.split("_")[-1].split(".")[0])
            if df is None:
                df = year_df.copy()
            else:
                df = pd.concat([df, year_df])

        df.reset_index(drop=True, inplace=True)
        if save_to_db:
            df.to_sql(endpoint_name, self.conn, if_exists="replace")
        return df
