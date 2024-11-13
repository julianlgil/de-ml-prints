from datasource import DataSource, DataSourcesSupported
from etl import ETL

if __name__ == '__main__':
    prints_data_source = DataSource(type=DataSourcesSupported.JSON,
                                    domain="prints",
                                    config={"path": "data_sources/prints.json"}
                                    )
    taps_data_source = DataSource(type=DataSourcesSupported.JSON,
                                  domain="taps",
                                  config={"path": "data_sources/taps.json"}
                                  )
    pays_data_source = DataSource(type=DataSourcesSupported.CSV,
                                  domain="pays",
                                  config={"path": "data_sources/pays.csv"}
                                  )

    etl = ETL(
        data_sources=[prints_data_source, taps_data_source, pays_data_source],
        days_delta=7
    )
    etl.execute()