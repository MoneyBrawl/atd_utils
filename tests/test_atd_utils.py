#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_atd_utils
----------------------------------

Tests for `atd_utils` module.
"""

import unittest
import shutil
import os
from os.path import join
import sys
import json
import pandas as pd
import sqlite3
from atd_utils import data_utils
from atd_utils.data_utils import (
    CfbdClient,
    APIKeyError,  # noqa
    EndpointNotValid,
    EndpointLoadingNotImplemented,
)  # noqa
from unittest.mock import patch, MagicMock


class TestCfbdClient(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.data_dir = "test_data123"
        self.db = "test_db123.db"
        self.client = CfbdClient(
            api_key="your_api_key_here", data_dir=self.data_dir, db=self.db
        )

        self.conn = sqlite3.connect(join(self.data_dir, self.db))

        self.year_1 = 2021
        self.year_2 = 2020

        self.test_endpoint = "test_endpoint"
        data_path = join(self.data_dir, self.test_endpoint)
        if not os.path.exists(data_path):
            os.makedirs(data_path)

        filename_1 = f"{self.test_endpoint}_{self.year_1}.json"
        self.filepath_1 = join(data_path, filename_1)
        with open(self.filepath_1, "w") as f:
            payload_1 = [{"cola": 1, "colb": 2}, {"cola": 3, "colb": 4}]
            f.write(json.dumps(payload_1))
        filename_2 = f"{self.test_endpoint}_{self.year_2}.json"
        self.filepath_2 = join(data_path, filename_2)
        with open(self.filepath_2, "w") as f:
            payload_2 = [{"cola": 5, "colb": 6}, {"cola": 7, "colb": 8}]
            f.write(json.dumps(payload_2))
        dot_file_path = join(self.data_dir, self.test_endpoint, ".test_file")
        print(dot_file_path)
        with open(dot_file_path, "w") as f:
            f.write("This is a dot file test.")

    def tearDown(self):
        """Tear down test fixtures"""
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_save_data(self):
        """
        Test that the directories and files get created and saved correctly.
        """
        test_sub_dir = "test_sub_dir"
        test_filename = "test_file.json"
        test_contents = '{"key": "value"}'

        self.client.save_data(test_sub_dir, test_filename, test_contents)

        # Check if the file was created and the contents were saved correctly
        file_path = join(self.data_dir, test_sub_dir, test_filename)
        self.assertTrue(os.path.exists(file_path))
        with open(file_path, "r") as f:
            contents = f.read()
        self.assertEqual(contents, test_contents)

        # Clean up the created file and directory
        os.remove(file_path)
        os.rmdir(join(self.data_dir, test_sub_dir))

    def test_pull_data(self):
        """
        Test that the pull_data function pulls data from the API and handles
        invalid endpoint errors correctly.
        """

        # Define a custom API response
        class FakeApiResponse:
            def to_dict(self):
                return {"field": "value"}

        def fake_endpoint_method(*args, **kwargs):
            return [FakeApiResponse()]

        # Create a MagicMock for cfbd library
        mock_cfbd = MagicMock()
        mock_cfbd.TESTApi.test_endpoint = fake_endpoint_method
        with patch("atd_utils.data_utils.cfbd", mock_cfbd):
            # Test pull_data function with the mocked cfbd library
            # result = pull_data(year, endpoint_name)
            # self.assertEqual(result, [{"field": "value"}])
            # Test pull_data with a valid endpoint and year

            mock_dict = {"implemented": {self.test_endpoint: {"api": "TEST"}}}
            with patch.dict(data_utils.__dict__, {"ENDPOINT_DICT": mock_dict}):
                data = self.client.pull_data(self.year_1, self.test_endpoint)
                self.assertIsInstance(data, list)

    def test_pull_data_invalid_endpoint(self):
        """Test the pull_data function with an invalid
        endpoint raises an `EndpointNotValid` exception.
        """
        # Test pull_data with an invalid endpoint
        with self.assertRaises(EndpointNotValid):
            _ = self.client.pull_data(self.year_1, "invalid_endpoint")

    def test_api_key_error(self):
        """
        Test that the CfbdClient raises an APIKeyError when initialized with
        an empty API key, either through the environment variable or by
        passing it explicitly.
        """
        # Save the current value of CFBD_API_KEY
        original_api_key = os.environ.get("CFBD_API_KEY")
        # Set the CFBD_API_KEY environment variable to an empty string
        os.environ.pop("CFBD_API_KEY")
        with self.assertRaises(APIKeyError):
            _ = CfbdClient()

        with self.assertRaises(APIKeyError):
            _ = CfbdClient(api_key="")

        os.environ["CFBD_API_KEY"] = "testing"
        os.environ.pop("CFBD_API_KEY")

        # Restore the original value of CFBD_API_KEY
        if original_api_key:
            os.environ["CFBD_API_KEY"] = original_api_key

    def test_api_key_env_set(self):
        """
        Test that the CfbdClient raises an APIKeyError when initialized with
        an empty API key, either through the environment variable or by
        passing it explicitly.
        """
        # Save the current value of CFBD_API_KEY
        original_api_key = os.environ.get("CFBD_API_KEY")
        os.environ["CFBD_API_KEY"] = "testing"

        test = CfbdClient()
        assert test.cfbd_configuration.api_key["Authorization"] == "testing"

        os.environ.pop("CFBD_API_KEY")

        # Restore the original value of CFBD_API_KEY
        if original_api_key:
            os.environ["CFBD_API_KEY"] = original_api_key

    def test_load_to_df_exception(self):
        """
        Test that the load_data function throws error when an endpoint hasn't
        been implemented yet.
        """
        with self.assertRaises(EndpointLoadingNotImplemented):
            _ = self.client.load_to_df("nonsense_endpoint")

    def test_load_to_df(self):
        """
        Test that the load_data function loads data with just json lines in it.
        """
        mock_dict = {"implemented": {self.test_endpoint: {"api": "ZApi"}}}
        with patch.dict(data_utils.__dict__, {"ENDPOINT_DICT": mock_dict}):
            data = self.client.load_to_df(self.test_endpoint, save_to_db=True)
            assert data.shape[0] == 4
            assert data.shape[1] == 3
            assert data.iloc[:, 0].sum() == 16
            assert data.iloc[:, 1].sum() == 20
            assert data.shape[0] == len(data.index.unique().tolist())
            assert sum(data.year == self.year_1) == 2
            assert sum(data.year == self.year_2) == 2
        # Check that the bd saved.
        df = pd.read_sql_query(f"SELECT * FROM {self.test_endpoint}", self.conn)  # noqa
        print(df)
        assert df.shape[0] == 4
        assert df.shape[1] == 4
        assert df.loc[:, "cola"].sum() == 16
        assert df.loc[:, "colb"].sum() == 20
        assert df.shape[0] == len(df.index.unique().tolist())
        assert sum(df.year == self.year_1) == 2
        assert sum(df.year == self.year_2) == 2


if __name__ == "__main__":
    sys.exit(unittest.main())
