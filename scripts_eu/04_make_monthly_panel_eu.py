import os
import pandas as pd


def make_monthly_panel(price_dir, fundamentals_dir, output_path):
    """
    Merges monthly returns with fundamentals.
    Forward fills fundamentals to monthly frequency.
    """

    all_data = []

    for file in os.listdir(price_dir):

        if not file.endswith("_monthly_returns.csv"):
            continue

        ticker = file.replace("_monthly_returns.csv", "")

        price_df = pd.read_csv(
            os.path.join(price_dir, file),
            parse_dates=["Date"]
        )

        fund_path = os.path.join(
            fundamentals_dir,
            f"{ticker}_fundamentals.csv"
        )

        if not os.path.exists(fund_path):
            continue

        fund_df = pd.read_csv(fund_path, parse_dates=["report_date"])

        fund_df.sort_values("report_date", inplace=True)

        price_df = price_df.sort_values("Date")

        merged = pd.merge_asof(
            price_df,
            fund_df,
            left_on="Date",
            right_on="report_date",
            direction="backward"
        )

        all_data.append(merged)

    final_df = pd.concat(all_data)
    final_df.to_csv(output_path, index=False)


if __name__ == "__main__":

    make_monthly_panel(
        price_dir="../data_eu/monthly_returns",
        fundamentals_dir="../data_eu/fundamentals",
        output_path="../data_eu/eu_monthly_panel.csv"
    )