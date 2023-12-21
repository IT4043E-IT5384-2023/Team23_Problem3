# =============================================================================
# @Time    : 05-Feb-23
# @Author  : tiennh
# @File    : airflow/dags/crawl_raw_data.py
# =============================================================================


# =============================================================================
# Imports
# =============================================================================

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "..", ".."))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'tiennh',
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='crawl_raw_data',
    default_args=default_args,
    description='Crawl raw data',
    start_date=datetime.fromtimestamp(1685433464),
    catchup=False,
    schedule_interval='10 8 * * *'
) as dag:

    ##############################################################################################################
    ################################################ Binance data ################################################
    ##############################################################################################################

    @task(task_id='crawl_binance_data')
    def crawl_binance_data():
        from production.run.binance.crawl import run
        binance_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'MATICUSDT', 'SOLUSDT',
                        'DOTUSDT', 'LTCUSDT', 'SHIBUSDT', 'AVAXUSDT', 'TRXUSDT', 'LINKUSDT', 'UNIUSDT', 'ATOMUSDT', 
                        'ETCUSDT', 'XMRUSDT', 'XLMUSDT', 'BCHUSDT', 'FILUSDT', 'TUSDUSDT', 'APTUSDT', 'LDOUSDT', 
                        'HBARUSDT', 'NEARUSDT', 'VETUSDT', 'ARBUSDT', 'APEUSDT', 'ICPUSDT', 'ALGOUSDT', 'QNTUSDT', 
                        'FTMUSDT', 'EOSUSDT', 'GRTUSDT', 'STXUSDT', 'MANAUSDT', 'AAVEUSDT', 'CFXUSDT', 'THETAUSDT', 
                        'EGLDUSDT', 'XTZUSDT', 'FLOWUSDT', 'IMXUSDT', 'AXSUSDT', 'SANDUSDT', 'CHZUSDT', 'NEOUSDT', 
                        'RPLUSDT', 'CRVUSDT', 'KLAYUSDT', 'OPUSDT', 'LUNCUSDT', 'MKRUSDT', 'GMXUSDT', 'SNXUSDT', 
                        'CAKEUSDT', 'MINAUSDT', 'ZECUSDT', 'DASHUSDT', 'FXSUSDT', 'IOTAUSDT', 'XECUSDT', 'RNDRUSDT', 
                        'PAXGUSDT', 'INJUSDT', 'RUNEUSDT', 'TWTUSDT', 'AGIXUSDT', 'LRCUSDT', 'ZILUSDT', 'KAVAUSDT',
                            '1INCHUSDT', 'CVXUSDT', 'ENJUSDT', 'WOOUSDT', 'MASKUSDT', 'BATUSDT', 'DYDXUSDT']
        market_type = "spot"
        intervals = ["1h", "5m", "1m"]
        run(
            symbols=binance_symbols,
            market_type=market_type,
            crawl_data_root_path=os.environ.get("BINANCE_CRAWL_DATA_ROOT_PATH"),
            intervals=intervals
        )

    ##############################################################################################################
    ################################################ Coinalyze data ################################################
    ##############################################################################################################

    @task(task_id='crawl_coinalyze_data')
    def crawl_coinalyze_data():
        from production.run.coinalyze.crawl import run, get_exchanges_and_symbols

        exchanges = ["Binance", "Bitfinex", "BitMEX",
                 "Bybit", "Deribit", "Huobi", "OKX", "Kraken", "Gate.io"]
        fields = ["ohlcv", "long_short_ratio", "predicted_funding_rate",
                "funding_rate", "liquidation", "open_interest"]
        intervals = ["1h", "1d"]
        crawl_data_root_path = os.getenv("COINALYZE_CRAWL_DATA_ROOT_PATH")

        exchanges_and_symbols = get_exchanges_and_symbols(exchanges)

        run(
            fields=fields,
            exchanges_and_symbols=exchanges_and_symbols,
            intervals=intervals,
            crawl_data_root_path=os.getenv("COINALYZE_CRAWL_DATA_ROOT_PATH")
        )

    ##############################################################################################################
    ################################################ Uniswap data ################################################
    ##############################################################################################################

    # @task(task_id='crawl_uniswap_data')
    # def crawl_uniswap_data():
    #     from production.run.uniswap.crawl import run
    #     run(
    #         crawl_data_root_path=os.getenv("UNISWAP_V3_CRAWL_DATA_ROOT_PATH")
    #     )

    ##############################################################################################################
    ################################################ Polygon data ################################################
    ##############################################################################################################

    import pandas as pd
    from src.polygon.crawler import PolygonCrawler
    from production.run.polygon.crawl import run

    data_catalog_file_path = os.environ.get("POLYGON_DATA_CATALOG_FILE_PATH")
    api_key = os.environ.get("POLYGON_API_KEY")

    if not os.path.exists(data_catalog_file_path):
        PolygonCrawler.crawl_data_catalog(api_key, data_catalog_file_path)

    data_catalog = pd.read_parquet(data_catalog_file_path)

    @task
    def crawl_stocks_polygon_data_for_one_symbol(symbols_list: list):
        run(
            symbols=symbols_list,
            crawl_data_root_path=os.getenv("POLYGON_CRAWL_DATA_ROOT_PATH")
        )

    dow_jones_indices_symbols = data_catalog[(data_catalog.market == "indices") & (
            data_catalog.name.str.contains("Dow Jones"))]["ticker"].tolist()
    cboe_indices_symbols = data_catalog[(data_catalog.market == "indices") & (
        data_catalog.name.str.contains("Cboe"))]["ticker"].tolist()
    nasdaq_indices_symbols = data_catalog[(data_catalog.market == "indices") & (
            data_catalog.name.str.contains("Nasdaq"))]["ticker"].tolist()
    crypto_symbols = data_catalog[data_catalog.market ==
                                "crypto"]["ticker"].tolist()
    stock_symbols = data_catalog[data_catalog.market == "stocks"]["ticker"].tolist()
    raw_polygon_symbols = dow_jones_indices_symbols + cboe_indices_symbols + nasdaq_indices_symbols + crypto_symbols + stock_symbols
    run_polygon_symbols = sorted(raw_polygon_symbols)
    run_polygon_symbols_lists = [run_polygon_symbols[i:i+100] for i in range(0,len(run_polygon_symbols),100)]
    crawl_stocks_polygon_data_for_one_symbol.expand(symbols_list=run_polygon_symbols_lists)

    ##############################################################################################################
    ################################################# FRED data ##################################################
    ##############################################################################################################

    @task(task_id='crawl_fred_data')
    def crawl_fred_data():
        from production.run.fred.crawl import run
        run(
            crawl_data_root_path=os.getenv("FRED_CRAWL_DATA_ROOT_PATH")
        )

    crawl_binance_data()
    crawl_coinalyze_data()
    crawl_fred_data()