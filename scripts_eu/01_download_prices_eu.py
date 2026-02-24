import os
import yfinance as yf
import pandas as pd


def download_prices(tickers, start_date, end_date, output_dir):
    """
    Downloads daily OHLCV price data from Yahoo Finance
    and saves one CSV per ticker.
    """

    os.makedirs(output_dir, exist_ok=True)

    for ticker in tickers:
        print(f"Downloading prices for {ticker}")
        data = yf.download(ticker, start=start_date, end=end_date)

        if data.empty:
            print(f"No data found for {ticker}")
            continue

        data.reset_index(inplace=True)
        data["ticker"] = ticker

        data.to_csv(os.path.join(output_dir, f"{ticker}_prices.csv"), index=False)


if __name__ == "__main__":

    eu_tickers = [
        "KRZ.IR",     # Kerry Group
        "A5G.IR",     # AIB
        "BIRG.IR",    # Bank of Ireland
        "ASML.AS",
        "SAP.DE",
        "MC.PA",
        "NOVO-B.CO",
        "SIE.DE",
        "OR.PA",
        "NESN.SW"
    ]

    download_prices(
        tickers=eu_tickers,
        start_date="2005-01-01",
        end_date="2026-01-01",
        output_dir="../data_eu/prices"
    )