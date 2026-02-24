import os
import yfinance as yf
import pandas as pd


def download_fundamentals(tickers, output_dir):
    """
    Downloads annual financial statement data
    and stores simplified fundamental metrics.
    """

    os.makedirs(output_dir, exist_ok=True)

    for ticker in tickers:

        print(f"Downloading fundamentals for {ticker}")

        stock = yf.Ticker(ticker)

        income = stock.financials.T
        balance = stock.balance_sheet.T

        if income.empty or balance.empty:
            print(f"No fundamentals for {ticker}")
            continue

        df = pd.DataFrame(index=income.index)

        df["revenue"] = income.get("Total Revenue")
        df["net_income"] = income.get("Net Income")
        df["ebit"] = income.get("Ebit")
        df["total_assets"] = balance.get("Total Assets")
        df["cash"] = balance.get("Cash")

        df["ticker"] = ticker

        df.reset_index(inplace=True)
        df.rename(columns={"index": "report_date"}, inplace=True)

        df.to_csv(
            os.path.join(output_dir, f"{ticker}_fundamentals.csv"),
            index=False
        )


if __name__ == "__main__":

    eu_tickers = [
        "KRZ.IR",
        "A5G.IR",
        "BIRG.IR",
        "ASML.AS",
        "SAP.DE",
        "MC.PA",
        "NOVO-B.CO",
        "SIE.DE",
        "OR.PA",
        "NESN.SW"
    ]

    download_fundamentals(
        tickers=eu_tickers,
        output_dir="../data_eu/fundamentals"
    )