# -*- coding: utf-8 -*-
import os
from os.path import join
import json
import pandas as pd
import cfbd
from cfbd.rest import ApiException
import sqlite3
from retrying import retry
from typing import List, Dict, Optional, Union
from .cfbd_endpoint_configs import ENDPOINTS_DICT

dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


# Define the maximum number of retries and the backoff interval between retries
MAX_RETRIES = 3
BACKOFF_FACTOR = 2


@retry(
    wait_exponential_multiplier=BACKOFF_FACTOR * 1000,
    stop_max_attempt_number=MAX_RETRIES,
)
def get_api_response(api_instance, request_params):
    try:
        # Call the API function
        api_response = api_instance(**request_params)
    except ApiException as e:
        # Raise a custom exception for API errors
        raise Exception(f"API Error: {e}")

    return api_response


class APIKeyError(Exception):
    pass


class EndpointNotValid(Exception):
    pass


class EndpointLoadingNotImplemented(Exception):
    pass


class CfbdClient(object):
    def __init__(
        self,  # noqa
        data_dir: str = join(os.path.expanduser("~"), ".atd_data"),  # noqa
        scratch_dir: str = join(os.path.expanduser("~"), ".atd_data/scratch"),  # noqa
        api_key: Optional[str] = None,  # noqa
        db: str = "atd.db",
        scratch_db: str = "scratch.db",
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
        self.conn = sqlite3.connect(join(data_dir, db))
        self.scratch_dir = scratch_dir
        if not os.path.exists(scratch_dir):
            os.makedirs(scratch_dir)
        self.scratch_conn = sqlite3.connect(join(scratch_dir, scratch_db))

    def is_table(self, table_name):
        """This method seems to be working now"""
        query = f"""SELECT
                        name
                    FROM sqlite_master
                    WHERE type='table'
                        AND name='{table_name}';"""
        cursor = self.conn.execute(query)
        result = cursor.fetchone()
        if result is None:
            return False
        else:
            return True

    def save_data(self, sub_dir: str, filename: str, data: str) -> None:
        """
        Save data to a file within a subdirectory.

        Args:
            sub_dir (str): The subdirectory to append to self.data_dir to save
                            the data in.
            filename (str): The name of the file to save the data to.
            data (str): The data to save.
        """
        path = join(self.data_dir, sub_dir)
        if not os.path.exists(path):
            os.makedirs(path)
        with open(f"{path}/{filename}", "w") as f:
            f.write(data)

    def hit_endpoint(
        self,  # noqa
        api_name: str,  # noqa
        endpoint_name: str,  # noqa
        request_params: Dict,
    ) -> List[Dict]:  # noqa
        api = getattr(cfbd, api_name)
        api_instance = api(cfbd.ApiClient(self.cfbd_configuration))
        endpoint_method = getattr(api_instance, endpoint_name)
        api_response = get_api_response(endpoint_method, request_params)
        results = [x.to_dict() for x in api_response]
        return results

    def pull_data(
        self,  # :noqa
        years: Union[List[int], range, int],  # noqa
        endpoint_name: str,  # noqa
        request_params: Dict = None,  # noqa
        save: bool = True,  # noqa
        force: bool = False,
        iter_teams: bool = False,
    ) -> List[Dict]:  # noqa
        """
        Pulls data from an API endpoint for the given years, and returns a
        list of dictionaries.

        Args:
            years: A single year or list of years for which to pull data. If
                an iterable of any type is passed, the method will always
                iterate over every year from max to min.
            endpoint_name: The name of the API endpoint to pull data from.
            request_params: Optional dictionary of request parameters to
                include in the API request (default: None).
            save: Optional boolean flag indicating whether to save the data to
                disk (default: True).
            force: Optional boolean flag indicating whether to check the
                unimplemented endpoints. Setting this to True will
                automatically overwrite your choice of save with False.
                (default: False).

        Returns:
            A list of dictionaries representing the data pulled from the API.
        """
        if isinstance(years, int):
            results = self.pull_year(
                years, endpoint_name, request_params, save, force
            )  # noqa
            print(f"Pulled {len(results)} records from {endpoint_name}.")
        else:
            results = []
            for ix, year in enumerate(range(max(years), min(years) - 1, -1)):
                if ix % 5 == 0:
                    print(
                        f"""Pulling {year}.\n\tPulled {len(results)} records
                    from {endpoint_name}."""
                    )
                year_results = self.pull_year(
                    year, endpoint_name, request_params, save, force, iter_teams  # noqa
                )  # noqa
                results += year_results
        return results

    def pull_year(
        self,  # :noqa
        year: int,  # noqa
        endpoint_name: str,  # noqa
        request_params: Dict = None,  # noqa
        save: bool = True,  # noqa
        force: bool = False,  # noqa
        iter_teams: bool = False,
    ) -> List[Dict]:  # noqa
        """
        Retrieve data from a specified College Football Data API endpoint for
        a given year.

        Args:
            year (int): The year for which to fetch data.
            endpoint_name (str): The name of the endpoint method to call on
                                the API instance. This should match one of
                                the keys in ENDPOINTS_DICT.

        Returns:
            list: A list of dictionaries containing the fetched data for the
            specified year and endpoint.

        Examples:
            >>> pull_data(2021, "get_games")
        """
        try:
            endpoint_config = ENDPOINTS_DICT[endpoint_name]["pull"]
            api_name = endpoint_config["api"]
            if not iter_teams:
                iter_teams = endpoint_config.setdefault("iter_teams", False)
        except KeyError:
            if force:
                endpoint_config = ENDPOINTS_DICT[endpoint_name]  # noqa
                api_name = endpoint_config["api"]  # noqa
                save = False
                print("NOT SAVING - Endpoint not fullimplemented.")
            else:
                msg = """Did you mean force=True? This endpoint does not exist
                or has not been setup yet."""
                raise EndpointNotValid(msg)
        func = endpoint_config.setdefault("pull_func", None)
        if not request_params:
            request_params = {"year": year}
        else:
            request_params["year"] = year

        args = [api_name, endpoint_name, request_params]
        if iter_teams:
            args.append(self.load_to_df("get_fbs_teams"))
        if func:
            args = [self.hit_endpoint] + args
            results = func(*args)  # noqa
        else:
            results = self.hit_endpoint(*args)  # noqa
        if save:
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
            endpoint_config = ENDPOINTS_DICT[endpoint_name]["load"]
        except KeyError:
            raise EndpointLoadingNotImplemented(
                f"""Either {endpoint_name} is not an endpoint, or I need to
                test and document in config/endpoints.json. whether
                json_normalize needs record_path and/or meta.
                """
            )
        record_path = endpoint_config.get("record_path", None)
        meta = endpoint_config.get("meta", None)
        endpoint_path = join(self.data_dir, endpoint_name)
        dir_list = os.listdir(endpoint_path)
        df = None
        for file in dir_list:
            if file[0] == ".":
                continue
            with open(join(endpoint_path, file), "r") as f:
                contents = json.loads(f.read())
            fields_to_stringify = endpoint_config.setdefault(
                "stringify_lists", False
            )  # noqa
            if fields_to_stringify:
                for ix, row in enumerate(contents):
                    for field in fields_to_stringify:
                        contents[ix][field] = json.dumps(row[field])

            file_name_fields_config = endpoint_config.setdefault(
                "file_name_fields", False
            )
            file_name_fields = {}
            if file_name_fields_config:
                for key, (value, value_type) in file_name_fields_config.items():  # noqa
                    file_name_fields[key] = value_type(
                        file.split(".")[0].split("_")[value]
                    )
            else:
                file_name_fields["year"] = int(
                    file.split("_")[-1].split(".")[0]
                )  # noqa

            pre_pandas_load_process = endpoint_config.setdefault(
                "pre_pandas_load_process", False
            )
            if pre_pandas_load_process:
                contents = pre_pandas_load_process(contents)

            contents_df = pd.json_normalize(
                contents, record_path=record_path, meta=meta  # noqa  # noqa
            )
            for key, value in file_name_fields.items():
                contents_df[key] = value
            if df is None:
                df = contents_df.copy()
            else:
                df = pd.concat([df, contents_df])

        df.reset_index(drop=True, inplace=True)
        if "df_load_process" in endpoint_config.keys():
            df = endpoint_config["df_load_process"](df)
        if save_to_db or not self.is_table(endpoint_name):
            df.to_sql(endpoint_name, self.conn, if_exists="replace")
        return df

    def help(self):
        methods = [
            method_name
            for method_name in dir(self)
            if callable(getattr(self, method_name))  # noqa
            and not method_name.startswith("__")  # noqa
            and not method_name == "help"  # noqa
        ]  # noqa
        attrs = [
            attr_name
            for attr_name in dir(self)
            if not callable(getattr(self, attr_name))  # noqa
            and not attr_name.startswith("__")  # noqa
        ]  # noqa
        print("Methods:")
        print("\t" + "\n\t".join(methods))
        print("\n")
        print("Attributes:")
        print("\t" + "\n\t".join(attrs))
        return True
