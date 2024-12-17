import os
import sys
import json
import time
import logging
import websocket
import requests as rq
import mysql.connector as mysql
import pandas as pd
import numpy as np
import datetime as dt
from collections import defaultdict

from logger_setup import LoggerSetup
from database_handler import DatabaseHandler

# FUNDING RATE MANAGER
class FundingRateManager:
    def __init__(self, db_handler: DatabaseHandler, logger: logging.Logger):
        self.db_handler = db_handler
        self.logger = logger

        self.futures_coinler = pd.DataFrame()
        self.coin_dict = defaultdict(lambda: pd.DataFrame([]))
        self.coin_dict_5m = defaultdict(lambda: pd.DataFrame([]))
        self.coin_dict_list_1m = []   # Will hold 1-minute dict data
        self.coin_dict_list_5m = [] # 5 mins
        self.coin_dict_1h = defaultdict(lambda: pd.DataFrame([]))
        self.coin_dict_1d = defaultdict(lambda: pd.DataFrame([]))
        self.db_rtfr_columns = [
            "timestamp", "datetime", "funding_rate", "funding_rate_mean",
            "mark_price_mean", "index_price_mean", "oi_transaction_timestamp",
            "oi_transaction_datetime", "open_interest"
        ] # columsn of our table. also has oi data

        # On startup, fetch coin list
        self.init_coin_list()

    # Initialize the coin list from DB.
    def init_coin_list(self):
        while True:
            df = self.db_handler.coin_list_database()
            if not df.empty:
                self.futures_coinler = df
                break
            else:
                time.sleep(1)  # Retry until we get a valid coin list

        for coin in self.futures_coinler["parite"]:
            self.coin_dict[coin] = pd.DataFrame([])

    # Check for newly added or removed coins, create necessary tables for new coins.
    def check_and_create_new_coin_tables(self):
        silinen_coinler, eklenen_coinler = self.db_handler.check_coins()
        if len(eklenen_coinler) > 0:
            self.logger.info(f"New coins: {eklenen_coinler}")
            self.db_handler.db_yeni_coin_ekle(eklenen_coinler)
            # Refresh the coin list
            self.futures_coinler = self.db_handler.coin_list_database()

    # Round the timestamp up or down to the nearest minute.
    # Returns (roundedtimestamp, [intervals]) where intervals is a list of intervals
    def timestamp_yuvarla(self, timestamp:float):
        intervals = ["1m"]
        temp_time = dt.datetime.fromtimestamp(timestamp)
        if temp_time.second >= 30:
            temp_time += dt.timedelta(seconds=(60 - temp_time.second))
        else:
            temp_time -= dt.timedelta(seconds=temp_time.second)

        new_ts = int(temp_time.timestamp())
        # Check for 5m, 1h, 1d intervals
        if new_ts % 300 == 0:
            intervals.append("5m")
        if new_ts % 3600 == 0:
            intervals.append("1h")
        if new_ts % 86400 == 0:
            intervals.append("1d")

        return new_ts, intervals

    # Merges 3-second data into 1-minute intervals for each coin.
    # Returns a dictionary with the 'final' row for each coin, along with mean calculations.
    def veri_duzenle(self, ham_veri:dict) -> dict:
        son_veri = {}
        for coin, df_data in ham_veri.items():
            if df_data.empty:
                continue
            last_event_time = df_data["E"].values[-1]
            rounded_ts, interval = self.timestamp_yuvarla(last_event_time)

            mean_fr = df_data["r"].astype(float).mean().round(16)
            mean_mark = df_data["p"].astype(float).mean()
            mean_index = df_data["i"].astype(float).mean()
            # The final row funding_rate is the last row
            final_fr = df_data["r"].values[-1]

            son_veri[coin] = {
                "timestamp": str(rounded_ts),
                "datetime": f'"{dt.datetime.fromtimestamp(rounded_ts).strftime("%Y-%m-%d %H:%M:%S")}"',
                "funding_rate_mean": str(mean_fr),
                "interval": interval,
                "symbol": coin,
                "mark_price_mean": str(mean_mark),
                "index_price_mean": str(mean_index),
                "funding_rate": final_fr
            }
        return son_veri

    # For 5-minute, 1-hour, 1-day intervals, merges multiple smaller interval data for final averages.
    def veri_duzenle_5dk(self, ham_veri:dict) -> dict:
        son_veri = {}
        for coin, df_data in ham_veri.items():
            if df_data.empty:
                continue

            # last row
            last_ts = df_data["timestamp"].values[-1]
            last_dt = df_data["datetime"].values[-1]

            # Mean FR, mark, index
            mean_fr = df_data["funding_rate_mean"].astype(float).mean().round(16)
            mean_mark = df_data["mark_price_mean"].astype(float).mean()
            mean_index = df_data["index_price_mean"].astype(float).mean()

            final_fr = df_data["funding_rate"].values[-1]

            son_veri[coin] = {
                "timestamp": str(last_ts),
                "datetime": last_dt,
                "funding_rate_mean": str(mean_fr),
                "symbol": coin,
                "mark_price_mean": str(mean_mark),
                "index_price_mean": str(mean_index),
                "funding_rate": final_fr
            }
        return son_veri


#%% WEBSOCKET CALLBACKS
def on_open(ws):
    print("Opened connection:", dt.datetime.now())

def on_ping(ws, a):
    print("FUNDING_RATE Ping signal received, sending pong ->: ", a, " - ", dt.datetime.now())

def on_close(ws, close_status_code, close_msg):
    print('Connection closed. Code:', close_status_code, " - Msg:", close_msg, dt.datetime.now())
    logger.warning(f"Connection closed. Code: {close_status_code}, Msg: {close_msg}")
    # Attempt to reconnect unless user manually closed
    if not isinstance(sys.exc_info()[1], KeyboardInterrupt):
        ws.run_forever(reconnect=0)

def on_error(ws, err):
    print("WEBSOCKET ERROR OCCURRED: ", err, dt.datetime.now())
    logger.warning(f"{dt.datetime.now()} WEBSOCKET ERROR: {err}")
    # Attempt to reconnect
    if not isinstance(sys.exc_info()[1], KeyboardInterrupt):
        ws.run_forever(reconnect=0)

def on_message(ws, message:str):
    global manager
    try:
        msg = json.loads(message)
        msg = msg["data"]  # This is a list of markPriceUpdate events
    except Exception as e:
        logger.warning("Failed to parse JSON message: " + str(e))
        return

    # The code below references the manager's coin_dict etc.
    coin_dict = defaultdict(lambda: pd.DataFrame([]))

    # Convert the raw data into DataFrames for each coin
    for item in msg:
        item["E"] = int(item["E"] / 1000)
        item["T"] = int(item["T"] / 1000)
        coin = item["s"]

        # Build a 1-row DataFrame and append to coin_dict[coin]
        row_df = pd.DataFrame([item])
        coin_dict[coin] = pd.concat([coin_dict[coin], row_df], ignore_index=True)

    # For each coin, if we get 20+ data points or a full minute (E % 60 == 0), we process & write to DB
    # We'll replicate the logic from the original code
    if len(coin_dict["BTCUSDT"]) >= 20 or (not coin_dict["BTCUSDT"].empty and coin_dict["BTCUSDT"]["E"].iloc[-1] % 60 == 0):
        # Convert 3-second data to 1-minute data
        try:
            duzenlenmis_veri = manager.veri_duzenle(coin_dict)
            manager.coin_dict_list_1m.append(duzenlenmis_veri)
        except Exception as e:
            logger.warning("Error during 1m data preparation: " + str(e))

        # Fill manager.coin_dict_5m for future 5m average
        for coin, final_dict in duzenlenmis_veri.items():
            if coin not in manager.coin_dict_5m:
                manager.coin_dict_5m[coin] = pd.DataFrame([])
            row_df = pd.DataFrame([final_dict])
            manager.coin_dict_5m[coin] = pd.concat([manager.coin_dict_5m[coin], row_df], ignore_index=True)

        # Write the collected 1-minute data in manager.coin_dict_list_1m
        try:                
            for dict_batch in manager.coin_dict_list_1m:
                for coin, data_dict in dict_batch.items():
                    intervals = data_dict.pop("interval", ["1m"])
                    symbol = data_dict.pop("symbol", coin)
                    table_1m = f"oi_{symbol.upper()}"
                    manager.db_handler.insert_row(table_1m, list(data_dict.keys()), list(data_dict.values()))                        
                    # Check intervals
                    if "5m" in intervals:
                        manager_5m_write = True
                    else:
                        manager_5m_write = False
                    if "1h" in intervals:
                        manager_1h_write = True
                        manager.saatlik_timestamp = int(data_dict["timestamp"])
                    else:
                        manager_1h_write = False
                    if "1d" in intervals:
                        manager_1d_write = True
                    else:
                        manager_1d_write = False
            manager.coin_dict_list_1m = []
        except Exception as e:
            logger.warning("Error writing 1m data to DB: " + str(e))
            manager.db_handler.reset_connection()

        # 5m data is to be written
        try:
            if 'manager_5m_write' in locals() and manager_5m_write:
                duzenlenmis_veri_5m = manager.veri_duzenle_5dk(manager.coin_dict_5m)
                manager.coin_dict_list_5m.append(duzenlenmis_veri_5m)
                manager.coin_dict_5m = defaultdict(lambda: pd.DataFrame([]))

                for dict_batch in manager.coin_dict_list_5m:
                    for coin, data_dict in dict_batch.items():
                        data_dict.pop("symbol", coin)
                        table_5m = f"oi5m_{coin.upper()}"
                        manager.db_handler.insert_row(table_5m, list(data_dict.keys()), list(data_dict.values()))                            
                manager.coin_dict_list_5m = []
        except Exception as e:
            logger.warning("Error writing 5m data to DB: " + str(e))

        # If 1h data is to be written
        try:
            if 'manager_1h_write' in locals() and manager_1h_write:
                manager_1h_write = False
                # For each coin, fetch last 12 rows from oi_{coin} to average
                for coin in manager.futures_coinler["parite"]:
                    last_rows_5m = manager.db_handler.get_last_row(table_name=f"oi1h_{coin.upper()}", satir_sayisi=12)
                    if not last_rows_5m:
                        continue
                    temp_data = pd.DataFrame(last_rows_5m).fillna(0)
                    temp_data.columns = manager.db_rtfr_columns
                    if temp_data.empty:
                        continue

                    # Build data to write
                    yazilacak_temp_data = {}
                    try:
                        last_ts = int(temp_data.loc[0, "timestamp"])
                        if last_ts % 3600 == 0:
                            yazilacak_temp_data['timestamp'] = str(last_ts)
                            yazilacak_temp_data['datetime'] = f'"{temp_data.loc[0, "datetime"]}"'
                        else:
                            # Fallback
                            st = manager.saatlik_timestamp if manager.saatlik_timestamp % 3600 == 0 else int(dt.datetime.now().timestamp())
                            yazilacak_temp_data['timestamp'] = str(st)
                            yazilacak_temp_data['datetime'] = f'"{dt.datetime.fromtimestamp(int(st)).strftime("%Y-%m-%d %H:%M:%S")}"'

                        yazilacak_temp_data['funding_rate'] = str(temp_data.loc[0, "funding_rate"])
                        yazilacak_temp_data['funding_rate_mean'] = str(temp_data["funding_rate_mean"].astype(float).mean().round(16))
                        yazilacak_temp_data['mark_price_mean'] = str(temp_data["mark_price_mean"].astype(float).mean())
                        yazilacak_temp_data['index_price_mean'] = str(temp_data["index_price_mean"].astype(float).mean())

                        manager.db_handler.insert_row(
                            table_name=f"oi1h_{coin.upper()}",
                            columns=list(yazilacak_temp_data.keys()),
                            values=list(yazilacak_temp_data.values())
                        )
                    except Exception as e:
                        logger.warning("Error computing 1h average: " + str(e))

        except Exception as e:
            logger.warning("Error in 1h writing block: " + str(e))

        # If 1d data is to be written
        try:
            if 'manager_1d_write' in locals() and manager_1d_write:
                manager_1d_write = False
                for coin in manager.futures_coinler["parite"]:
                    last_rows_1h = manager.db_handler.get_last_row(table_name=f"oi1d_{coin.upper()}", satir_sayisi=24)
                    if not last_rows_1h:
                        continue
                    temp_data = pd.DataFrame(last_rows_1h).fillna(0)
                    temp_data.columns = manager.db_rtfr_columns

                    yazilacak_temp_data = {}
                    try:
                        last_ts = int(temp_data.loc[0, "timestamp"])
                        if last_ts % 86400 == 0:
                            yazilacak_temp_data['timestamp'] = str(last_ts)
                            yazilacak_temp_data['datetime'] = f'"{temp_data.loc[0, "datetime"]}"'
                        else:
                            st = manager.saatlik_timestamp if manager.saatlik_timestamp % 86400 == 0 else int(dt.datetime.now().timestamp())
                            yazilacak_temp_data['timestamp'] = str(st)
                            yazilacak_temp_data['datetime'] = f'"{dt.datetime.fromtimestamp(int(st)).strftime("%Y-%m-%d %H:%M:%S")}"'

                        yazilacak_temp_data['funding_rate'] = str(temp_data.loc[0, "funding_rate"])
                        yazilacak_temp_data['funding_rate_mean'] = str(temp_data["funding_rate_mean"].astype(float).mean().round(16))
                        yazilacak_temp_data['mark_price_mean'] = str(temp_data["mark_price_mean"].astype(float).mean())
                        yazilacak_temp_data['index_price_mean'] = str(temp_data["index_price_mean"].astype(float).mean())

                        manager.db_handler.insert_row(
                            table_name=f"oi1d_{coin.upper()}",
                            columns=list(yazilacak_temp_data.keys()),
                            values=list(yazilacak_temp_data.values())
                        )
                    except Exception as e:
                        logger.warning("Error computing 1d average: " + str(e))

        except Exception as e:
            logger.warning("Error in 1d writing block: " + str(e))


# Main
if __name__ == "__main__":
    # Global usage for websocket callbacks
    global logger
    global manager


    logger_setup = LoggerSetup()
    logger = logger_setup.get_logger()
    db_handler = DatabaseHandler(logger)
    manager = FundingRateManager(db_handler, logger)
    manager.check_and_create_new_coin_tables()

    # Websocket
    link = "wss://fstream.binance.com/stream?streams="
    markPrice_all_link = link + "!markPrice@arr"

    while True:
        try:
            ws_app = websocket.WebSocketApp(
                markPrice_all_link,
                on_open=on_open,
                on_close=on_close,
                on_message=on_message,
                on_error=on_error,
                on_ping=on_ping
            )
            ws_app.run_forever(reconnect=0)
        except Exception as e:
            logger.warning("WebSocket connection issue: " + str(e))
            ws_app.run_forever(reconnect=0)
