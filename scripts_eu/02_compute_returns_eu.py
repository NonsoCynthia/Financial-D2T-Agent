import os
import pandas as pd


def compute_returns(input_dir, output_dir):
    """
    Computes daily returns and monthly returns
    from downloaded daily price data.
    """

    os.makedirs(output_dir, exist_ok=True)

    for file in os.listdir(input_dir):

        if not file.endswith("_prices.csv"):
            continue

        df = pd.read_csv(os.path.join(input_dir, file), parse_dates=["Date"])

        df.sort_values("Date", inplace=True)

        df["daily_return"] = df["Adj Close"].pct_change()

        monthly = df.resample("M", on="Date").last()
        monthly["monthly_return"] = monthly["Adj Close"].pct_change()

        ticker = df["ticker"].iloc[0]
        monthly["ticker"] = ticker

        monthly.reset_index(inplace=True)

        monthly.to_csv(
            os.path.join(output_dir, f"{ticker}_monthly_returns.csv"),
            index=False
        )


if __name__ == "__main__":

    compute_returns(
        input_dir="../data_eu/prices",
        output_dir="../data_eu/monthly_returns"
    )