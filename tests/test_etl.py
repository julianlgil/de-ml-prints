import unittest
from unittest.mock import MagicMock

import pandas as pd
from datetime import datetime, timedelta
from datasource import DataSource, DataSourcesSupported
from etl import ETL, ValuePropsEtl
from exceptions import TransformException, ExtractException


class TestETL2(unittest.TestCase):

    def setUp(self):
        # Configuración de datos de ejemplo
        self.data_sources = [
            DataSource(type=DataSourcesSupported.CSV, config={"path": "tests/test_csv.csv"}, domain="prints"),
            DataSource(type=DataSourcesSupported.JSON, config={"path": "tests/test_json.json"}, domain="taps")
        ]
        self.etl = ValuePropsEtl(data_sources=self.data_sources, days_delta=7)

    def test_execute_calls_extract_transform_load(self):
        self.etl._extract = MagicMock()
        self.etl._transform = MagicMock()
        self.etl._load = MagicMock()

        self.etl.execute()

        self.etl._extract.assert_called_once()
        self.etl._transform.assert_called_once()
        self.etl._load.assert_called_once()

    def test_extract_valid_data_sources(self):
        data = self.etl._extract()
        self.assertIn("prints", data)
        self.assertIn("taps", data)

    def test_transform_with_valid_data(self):
        data = {
            "prints": pd.DataFrame({
                "user_id": [1, 2],
                "day": [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
                        datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)],
                "event_data.value_prop": ["test", "test"],
                "event_data.position": [1, 2]
            }),
            "taps": pd.DataFrame({
                "user_id": [1],
                "day": [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)],
                "event_data.value_prop": ["test"],
                "event_data.position": [1]
            }),
            "pays": pd.DataFrame({
                "user_id": [1],
                "pay_date": [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)],
                "total": [100.0],
                "value_prop": ["test"]
            })
        }
        transformed_data = self.etl._transform(data)
        self.assertIn("previous_prints", transformed_data.columns)
        self.assertIn("previous_taps", transformed_data.columns)

    def test_load_creates_output_files(self):
        data = pd.DataFrame({
            "user_id": [1, 2],
            "day": [datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d')],
            "value_prop": ["test", "test"],
            "position": [1, 2],
            "clicked": [True, False]
        })
        self.etl._load(data)
        with open("output.csv", "r") as file:
            csv_content = file.read()
        with open("output.json", "r") as file:
            json_content = file.read()

        self.assertTrue("user_id" in csv_content)
        self.assertTrue("user_id" in json_content)

    def test_set_prints_clicked(self):
        prints_last_week = pd.DataFrame({
            "user_id": [1, 2],
            "day": [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
                    datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)],
            "position": [1, 2],
            "value_prop": ["test", "test"]
        })
        taps = pd.DataFrame({
            "user_id": [1],
            "day": [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)- timedelta(days=1)],
            "position": [1],
            "value_prop": ["test"]
        })
        date_delta = datetime.now() - timedelta(days=7)
        result = self.etl._ValuePropsEtl__set_prints_clicked(prints_last_week, taps, date_delta)
        self.assertIn("clicked", result.columns)
        self.assertTrue(result["clicked"].iloc[0])

    def test_transform_exception_on_empty_data(self):
        data = {"prints": pd.DataFrame(), "taps": pd.DataFrame(), "pays": pd.DataFrame()}
        with self.assertRaises(TransformException):
            self.etl._transform(data)

    def test_extract_exception_on_invalid_data_source(self):
        invalid_data_sources = [DataSource(type="XML", config={"path": "test.xml"}, domain="prints")]
        etl_invalid = ValuePropsEtl(data_sources=invalid_data_sources, days_delta=7)
        with self.assertRaises(ExtractException):
            etl_invalid._extract()


if __name__ == "__main__":
    unittest.main()
