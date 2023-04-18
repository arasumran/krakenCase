import pandas as pd
import unittest
from datetime import datetime
from OutagesOfSitesService import OutagesOfSitesService
from unittest.mock import patch


def concat_dataframes(df1, df2):
    """Concatenate two dataframes horizontally"""
    return pd.concat([df1, df2], axis=1)

class TestConcatDataFrames(unittest.TestCase):
    def setUp(self):
        self.df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        self.df2 = pd.DataFrame({'C': [7, 8, 9], 'D': [10, 11, 12]})
        self.service = OutagesOfSitesService()
        self.battery_list = [
            {
                "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                "name": "Battery 1"
            },
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2"
            }
        ]

    def test_concat_dataframes(self):
        result = concat_dataframes(self.df1, self.df2)
        expected = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9], 'D': [10, 11, 12]})
        pd.testing.assert_frame_equal(result, expected)
    def test_filter_outages_by_timestamp_with_valid_input(self):
        # Arrange

        outages_df = pd.DataFrame([
            {
                "id": "0088afee-81ce-49dd-8eda-b9f1267d03fd",
                "begin": "2022-11-17T05:59:52.457Z",
                "end": "2022-11-26T15:12:19.353Z"
            },
            {
                "id": "09576af4-2e94-4142-b3e1-27695adc7de9",
                "begin": "2022-03-25T02:41:29.793Z",
                "end": "2022-12-25T02:28:24.142Z"
            }
        ])
        last_timestamp = "2022-03-25T2:41:29.793Z"

        # Act
        filtered_outages = self.service.filter_outages_by_timestamp(outages_df, last_timestamp)

        # Assert
        self.assertIsInstance(filtered_outages, pd.DataFrame)
        self.assertEqual(len(filtered_outages), 1)
        self.assertEqual(filtered_outages.iloc[0]['id'], '0088afee-81ce-49dd-8eda-b9f1267d03fd')

    def test_filter_outages_by_timestamp_with_invalid_input(self):
        # Arrange
        outages_df = None
        last_timestamp = "2022 03-27T2:41:29.793Z"

        # Assert
        with self.assertRaises(TypeError):
            self.service.filter_outages_by_timestamp(outages_df, last_timestamp)

        # Arrange
        outages_df = pd.DataFrame([
            {
                "id": "0088afee-81ce-49dd-8eda-b9f1267d03fd",
                "end": "2022-11-26T15:12:19.353Z"
            },
            {
                "id": "09576af4-2e94-4142-b3e1-27695adc7de9",
                "begin": "2022-03-25T02:41:29.793Z",
                "end": "2022-12-25T02:28:24.142Z"
            }
        ])
        last_timestamp = datetime(2022, 3, 25, 2, 41, 29, 793000)

        # Assert
        with self.assertRaises(TypeError):
            self.service.filter_outages_by_timestamp(outages_df, last_timestamp)


    def test_create_battery_df(self):
        expected_df = pd.DataFrame( [
            {
                "id": "111183e7-fb90-436b-9951-63392b36bdd2",
                "name": "Battery 1"
            },
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2"
            }
        ])
        battery_df = pd.DataFrame(self.battery_list)
        battery_df = self.service.filter_site_id_by_not_null(battery_df)
        pd.testing.assert_frame_equal(battery_df, expected_df)

    def test_filter_id_not_null(self):
        expected_df = pd.DataFrame([
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2"
            }
        ])
        battery_df = pd.DataFrame([
            {
                "id": None,
                "name": "Battery 1"
            },
            {
                "id": "86b5c819-6a6c-4978-8c51-a2d810bb9318",
                "name": "Battery 2"
            }
        ])
        battery_df = self.service.filter_site_id_by_not_null(battery_df)
        self.assertEqual(battery_df.__len__(), expected_df.__len__())
    def test_transformation(self):
        outages_df = pd.DataFrame({
            "id": ["86b5c819-6a6c-4978-8c51-a2d810bb9318"],
            "begin": ["2022-01-01T00:00:00.000Z"],
            "end": ["2022-01-02T00:00:00.000Z"]
        })

        site_id_df ={
            "id":  "86b5c819-6a6c-4978-8c51-a2d810bb9318",
            "name": "Battery 2"
        }
        last_timestamp = "2021-01-01T12:00:00.000Z"
        expected_df = pd.DataFrame({
            "id": ["86b5c819-6a6c-4978-8c51-a2d810bb9318"],
            "name": ["Battery 2"]
        })
        result = self.service.transformation_site_outages(outages_df, {'devices':site_id_df}, last_timestamp)


if __name__ == '__main__':
    unittest.main()