import tinkoff.invest
from tinkoff.invest import PortfolioRequest, PortfolioPosition, Client, RequestError, CandleInterval, HistoricCandle, \
    OrderType, OrderDirection, Quotation, InstrumentIdType, InstrumentStatus
from tinkoff.invest.services import InstrumentsService
from datetime import datetime
import pandas as pd
import os
import requests
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

url = "https://invest-public-api.tinkoff.ru/history-data"
token = "YOUR_TOKEN"
your_directory_path = "YOUR_FOLDER"
year_from = "NEEDED_YEAR"
wish_instruments_tickers = ["ALL", "TICKERS", "NEEDED"]

with Client(token=token) as client:
    instruments: InstrumentsService = client.instruments
    df_screener_all = pd.DataFrame(
        instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_ALL).instruments,
        columns=["name", "ticker", "uid", "figi", "isin", "lot", "currency"])
    df_screener_rub = df_screener_all[df_screener_all["currency"] == "rub"]


def create_ticker_folder(ticker):
    ticker_folder_path = os.path.join(your_directory_path, ticker)

    if not os.path.exists(ticker_folder_path):
        os.makedirs(ticker_folder_path)

    return ticker_folder_path


def get_ticker_folder(ticker):
    ticker_folder_path = os.path.join(your_directory_path, ticker)

    return ticker_folder_path


def create_year_helper(ticker, year_from):
    ticker_folder_path = get_ticker_folder(ticker)
    year_helper_path = os.path.join(ticker_folder_path, "year_helper.txt")

    with open(year_helper_path, 'w') as file:
        file.write(year_from)
    return year_helper_path


def get_year_helper(ticker):
    ticker_folder_path = get_ticker_folder(ticker)
    year_helper_path = os.path.join(ticker_folder_path, "year_helper.txt")
    return year_helper_path


def create_figi_txt(ticker, year):
    base_path = get_ticker_folder(ticker)
    figi_helper_path = os.path.join(base_path, f"{ticker}_figi.txt")

    open(figi_helper_path, 'w').close()
    return figi_helper_path


def get_figi_txt(ticker):
    base_path = get_ticker_folder(ticker)
    figi_helper_path = os.path.join(base_path, f"{ticker}_figi.txt")
    return figi_helper_path


def get_figi(ticker) -> list:
    if (not df_screener_rub[df_screener_rub["ticker"] == ticker].empty):
        ticker_figi = df_screener_rub[df_screener_rub["ticker"] == ticker]

    if (ticker_figi.shape[0] > 1):
        ticker_FIGI = list(ticker_figi["figi"])
    else:
        ticker_FIGI = [ticker_figi["figi"].iloc[0]]

    return ticker_FIGI


def get_uid(ticker) -> list:
    if (not df_screener_rub[df_screener_rub["ticker"] == ticker].empty):
        ticker_figi = df_screener_rub[df_screener_rub["ticker"] == ticker]

    if (ticker_figi.shape[0] > 1):
        ticker_UID = list(ticker_figi["uid"])
    else:
        ticker_UID = [ticker_figi["uid"].iloc[0]]

    return ticker_UID


def get_isin(ticker) -> list:
    if (not df_screener_rub[df_screener_rub["ticker"] == ticker].empty):
        ticker_figi = df_screener_rub[df_screener_rub["ticker"] == ticker]

    if (ticker_figi.shape[0] > 1):
        ticker_ISIN = list(ticker_figi["isin"])
    else:
        ticker_ISIN = [ticker_figi["isin"].iloc[0]]

    return ticker_ISIN


def is_figi_correct(figi) -> bool:
    correct_figi = False
    with open(f"{your_directory_path}/figi.txt", 'r') as file:
        for line_number, line in enumerate(file, 1):
            if figi in line:
                correct_figi = True
    return correct_figi


def get_correct_figi(figi) -> str:
    for f in figi:
        if is_figi_correct(f) == True:
            return f

    return "0"


def download(ticker, figi, year):
    download_folder = f"{your_directory_path}/{ticker}"
    year_helper_txt = f"{your_directory_path}/{ticker}/year_helper.txt"
    minimum_year = int(open(year_helper_txt).read().strip())

    if year < minimum_year:
        return

    file_name = f"{download_folder}/{figi}_{year}.zip"

    print(f"downloading {figi} for year {year}")

    params = {"figi": figi, "year": year}
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")
        return

    response_code = response.status_code
    if response_code == 429:
        print("rate limit exceed. sleep 5")
        time.sleep(5)
        download(ticker, figi, year)
        return
    elif response_code in [401, 500]:
        print("invalid token")
        exit(1)
    elif response_code == 404:
        print(f"data not found for figi={figi}, year={year}, removing empty file")
        if os.path.exists(file_name):
            os.remove(file_name)
    elif response_code != 200:
        print(f"unspecified error with code: {response_code}")
        exit(1)

    with open(file_name, 'wb') as file:
        file.write(response.content)

    year -= 1
    download(ticker, figi, year)


def run_script(ticker):
    current_year = datetime.now().year

    ticker_folder_path = get_ticker_folder(ticker)
    figi_helper_path = get_figi_txt(ticker)

    ticker_figi = get_figi(ticker)
    ticker_uid = get_uid(ticker)
    ticker_isib = get_isin(ticker)

    response_code = 0
    correct_figi = get_correct_figi(ticker_figi)

    if (correct_figi != "0"):
        print(f"Congratulations! {correct_figi} is a correct figi for {ticker}")
        print()
        download(ticker, correct_figi.strip(), current_year)
    else:
        for instrument in [ticker_figi, ticker_isib, ticker_uid]:
            for code in instrument:
                download(ticker, code.strip(), current_year)
                if (len(os.listdir(ticker_folder_path)) > 3):
                    break
                else:
                    with open(figi_helper_path, 'w') as file:
                        pass
                    continue

    return response_code


def clean_ticker_folder(ticker_folder_path):
    for root, dirs, files in os.walk(ticker_folder_path):
        for file in files:
            if file.endswith(".zip"):
                zip_file_path = os.path.join(root, file)
                extract_folder = os.path.join(root, file.replace(".zip", ""))

                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_folder)

                os.remove(zip_file_path)


def get_paths_list(ticker_folder_path):
    paths_list = []

    for root, dirs, files in os.walk(ticker_folder_path):
        for file in files:
            if file.endswith(".csv"):
                csv_file_path = os.path.join(root, file)
                paths_list.append(csv_file_path)

    return paths_list


def get_dataframe(ticker):
    ticker_folder_path = create_ticker_folder(ticker)
    dfs = []

    for root, dirs, files in os.walk(ticker_folder_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                temp_df = pd.read_csv(file_path, sep=";",
                                      names=["date", "open", "close", "min", "max", "volume", "hui"], header=None)
                dfs.append(temp_df)

    ticker_TS = pd.concat(dfs, ignore_index=True)
    ticker_TS.drop(ticker_TS.columns[-1], axis=1, inplace=True)

    ticker_TS["date"] = pd.to_datetime(ticker_TS["date"], format="%Y-%m-%dT%H:%M:%SZ")
    ticker_TS.sort_values(by='date', ascending=False, inplace=True, ignore_index=True)

    return ticker_TS


def get_historical_data(ticker, year_from):
    ticker_folder_path = create_ticker_folder(ticker)
    year_helper_path = create_year_helper(ticker=ticker, year_from=year_from)
    figi_helper_path = create_figi_txt(ticker=ticker, year=year_from)

    result = run_script(ticker)

    if (result == 0):
        clean_ticker_folder(ticker_folder_path=ticker_folder_path)
        return get_dataframe(ticker_folder_path)
    else:
        return "No Data found"


def get_historical_data_multithreaded(tickers, year_from):
    with ThreadPoolExecutor(max_workers=5) as executor:
        args_list = [(ticker, year_from) for ticker in tickers]

        results = list(executor.map(get_historical_data_worker, args_list))

    return results


def get_historical_data_worker(args):
    ticker, year_from = args
    return ticker, get_historical_data(ticker, year_from)


results = get_historical_data_multithreaded(wish_instruments_tickers, year_from)

for ticker, historical_data in results:
    print(f"{ticker} data:\n{historical_data}")
