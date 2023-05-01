#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_atd_utils
----------------------------------

Tests for `atd_utils` module.
"""

import unittest
import os
import json
from atd_utils.data_utils import CfbdClient, APIKeyError, EndpointNotValid
from unittest.mock import patch


class TestCfbdClient(unittest.TestCase):
    def setUp(self):
        self.client = CfbdClient(api_key='your_api_key_here', data_dir="test_data")

    def test_save_data(self):
        """
        Test that the directories and files get created and saved correctly.
        """
        test_sub_dir = "test_sub_dir"
        test_filename = "test_file.json"
        test_contents = '{"key": "value"}'

        self.client.save_data(test_sub_dir, test_filename, test_contents)

        # Check if the file was created and the contents were saved correctly
        file_path = os.path.join("test_data", test_sub_dir, test_filename)
        self.assertTrue(os.path.exists(file_path))
        with open(file_path, "r") as f:
            contents = f.read()
        self.assertEqual(contents, test_contents)

        # Clean up the created file and directory
        os.remove(file_path)
        os.rmdir(os.path.join("test_data", test_sub_dir))
        os.rmdir("test_data")

    def test_pull_data(self):
        """
        Test that the pull_data function pulls data from the API and handles 
        invalid endpoint errors correctly.
        """
        class TestGame:
            def to_dict(self):
                return {"a": 1}
        def _mock_cfbd_method(*args, **kwargs):
            return [TestGame()]  # Return a fake response

        # Mock the cfbd.ApiClient method
        with patch("cfbd.api.games_api.GamesApi.get_games", _mock_cfbd_method):
            # Test pull_data with a valid endpoint and year
            test_endpoint = "get_games"
            test_year = 2021
            data = CfbdClient.pull_data(test_year, test_endpoint)
            self.assertIsInstance(data, list)


    def test_pull_data_invalid_endpoint(self):
        """Test the pull_data function with an invalid 
        endpoint raises an `EndpointNotValid` exception.
        """
        # Test pull_data with an invalid endpoint
        with self.assertRaises(EndpointNotValid):
            data = CfbdClient.pull_data(test_year, "invalid_endpoint")

    def test_api_key_error(self):
        """
        Test that the CfbdClient raises an APIKeyError when initialized with an empty API key,
        either through the environment variable or by passing it explicitly.
        """
        # Save the current value of CFBD_API_KEY
        original_api_key = os.environ.get("CFBD_API_KEY")
        # Set the CFBD_API_KEY environment variable to an empty string
        os.environ.pop("CFBD_API_KEY")
        with self.assertRaises(APIKeyError):
            client = CfbdClient()
        # Restore the original value of CFBD_API_KEY
        if original_api_key:
            os.environ["CFBD_API_KEY"] = original_api_key

        with self.assertRaises(APIKeyError):
            client = CfbdClient(api_key="")


if __name__ == "__main__":
    sys.exit(unittest.main())
