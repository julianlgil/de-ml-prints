import logging
from datetime import datetime, timedelta
from typing import List

import pandas as pd

from datasource import DataSource, DataSourcesSupported
from exceptions import TransformException, ExtractException


class ETL:

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                            filename='logs.log', encoding='utf-8', level=logging.DEBUG)

    def execute(self):
        try:
            data = self._extract()
            transformed_data = self._transform(data)
            self._load(transformed_data)
        except Exception as e:
            self.logger.error(f"An exception occurred when ETL execution. Reason: {e}")

    def _extract(self):
        pass

    def _transform(self, data):
        pass

    def _load(self, data):
        pass


class ValuePropsEtl(ETL):

    def __init__(self, data_sources: List[DataSource], days_delta: int):
        self.data_sources = data_sources
        self.date_delta = days_delta
        super().__init__()


    def _extract(self):
        data = {}
        try:
            for data_source in self.data_sources:
                if data_source.type == DataSourcesSupported.CSV:
                    df = pd.read_csv(data_source.config["path"], sep=",", index_col=None)
                elif data_source.type == DataSourcesSupported.JSON:
                    df = pd.read_json(data_source.config["path"], lines=True)
                    df_dict = df.to_dict(orient='records')
                    df = pd.json_normalize(df_dict, record_path=None, meta=['day', 'user_id'])
                else:
                    self.logger.warning(f"The data source type: {data_source.type} is not supported")
                    raise Exception("The data source type: {data_source.type} is not supported")
                data.update({data_source.domain: df})
        except Exception as e:
            self.logger.error(f"An exception occurred when extracting data. Reason: {e}")
            raise ExtractException(e)
        return data

    def _transform(self, data):
        prints = self.__get_prints(data)
        date_delta = prints["day"].max() - timedelta(days=self.date_delta)
        taps = self.__get_taps(data)
        pays = self.__get_pays(data)

        prints_last_week = prints[prints['day'] >= date_delta]
        self.logger.info(
            f"{len(prints_last_week)} prints were found between {date_delta.strftime('%Y-%m-%d')} - {prints['day'].max().strftime('%Y-%m-%d')}")

        prints_last_week = self.__set_prints_clicked(prints_last_week, taps, date_delta)
        prints_last_week["previous_prints"] = self.__set_count_previous_prints(prints_last_week, prints, date_delta)
        prints_last_week["previous_taps"] = self.__set_count_previous_taps(prints_last_week, taps, date_delta)
        prints_last_week["previous_pays"] = self.__set_count_previous_pays(prints_last_week, pays, date_delta)
        prints_last_week["amount_previous_pays"] = self.__set_amount_previous_pays(prints_last_week, pays, date_delta)

        prints_last_week.loc[:, 'day'] = prints_last_week['day'].dt.strftime('%Y-%m-%d')

        return prints_last_week

    def __get_pays(self, data):
        pays = data["pays"]
        if not pays.empty:
            pays["user_id"] = pays["user_id"].astype("int32")
            pays['day'] = pd.to_datetime(pays['pay_date'])
            pays.drop(columns="pay_date", inplace=True)
        else:
            self.logger.warning("No pays data to process")
            raise TransformException("No pays data to process")
        return pays

    def __get_taps(self, data):
        taps = data["taps"]
        if not taps.empty:
            taps["user_id"] = taps["user_id"].astype("int32")
            taps['day'] = pd.to_datetime(taps['day'])
            taps.rename(columns={'event_data.value_prop': 'value_prop', 'event_data.position': 'position'},
                        inplace=True)
        else:
            self.logger.warning("No taps data to process")
            raise TransformException("No taps data to process")
        return taps

    def __get_prints(self, data):
        prints = data["prints"]
        if not prints.empty:
            prints["user_id"] = prints["user_id"].astype("int32")
            prints['day'] = pd.to_datetime(prints['day'])
            prints.rename(columns={'event_data.value_prop': 'value_prop', 'event_data.position': 'position'},
                          inplace=True)
        else:
            self.logger.warning("No prints data to process")
            raise TransformException("No prints data to process")
        return prints

    def __set_prints_clicked(self, prints_last_week: pd.DataFrame, taps: pd.DataFrame, date_delta: datetime):
        taps_last_week = taps[taps['day'] >= date_delta]
        prints_with_taps = prints_last_week.merge(taps_last_week,
                                                  on=["day", "position", "value_prop", "user_id"],
                                                  how="left",
                                                  indicator=True)
        clicked = prints_with_taps["_merge"] == "both"
        clicked.index = prints_last_week.index
        prints_last_week["clicked"] = clicked
        return prints_last_week

    def __set_count_previous_prints(self, prints_last_week: pd.DataFrame, prints: pd.DataFrame, date_delta: datetime):
        previous_prints = prints[prints["day"] < date_delta]
        prints_count = previous_prints.groupby(["value_prop", "user_id"]).size().reset_index(
            name="previous_prints")
        prints_last_week = prints_last_week.merge(prints_count, on=["value_prop", "user_id"], how="left")
        prints_last_week["previous_prints"] = prints_last_week["previous_prints"].fillna(0).astype("int32")
        return prints_last_week["previous_prints"].tolist()

    def __set_count_previous_taps(self, prints_last_week: pd.DataFrame, taps: pd.DataFrame, date_delta: datetime):
        previous_taps = taps[taps["day"] < date_delta]
        taps_count = previous_taps.groupby(["value_prop", "user_id"]).size().reset_index(
            name="previous_taps")
        prints_last_week = prints_last_week.merge(taps_count, on=["value_prop", "user_id"], how="left")
        prints_last_week["previous_taps"] = prints_last_week["previous_taps"].fillna(0).astype("int32")
        return prints_last_week["previous_taps"].tolist()

    def __set_count_previous_pays(self, prints_last_week: pd.DataFrame, pays: pd.DataFrame, date_delta: datetime):
        previous_pays = pays[pays["day"] < date_delta]
        pays_count = previous_pays.groupby(["value_prop", "user_id"]).size().reset_index(
            name="previous_pays")
        prints_last_week = prints_last_week.merge(pays_count, on=["value_prop", "user_id"], how="left")
        prints_last_week["previous_pays"] = prints_last_week["previous_pays"].fillna(0).astype("int32")
        return prints_last_week["previous_pays"].tolist()

    def __set_amount_previous_pays(self, prints_last_week: pd.DataFrame, pays: pd.DataFrame, date_delta: datetime):
        previous_pays = pays[pays["day"] < date_delta]
        pays_count = previous_pays.groupby(["value_prop", "user_id"])["total"].sum().reset_index(
            name="amount_previous_pays")
        prints_last_week = prints_last_week.merge(pays_count, on=["value_prop", "user_id"], how="left")
        prints_last_week["amount_previous_pays"] = prints_last_week["amount_previous_pays"].fillna(0)
        return prints_last_week["amount_previous_pays"].tolist()

    def _load(self, data):
        data.to_csv("output.csv", index=False, float_format="%.2f")
        data.to_json("output.json", orient="records", double_precision=2, lines=True)
