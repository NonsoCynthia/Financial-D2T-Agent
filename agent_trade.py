import argparse
import asyncio
import json
import sys
from pathlib import Path

import pandas as pd
from agents import Agent, Runner
from agents.mcp import MCPServerStdio


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["sample", "all"], default="sample")
    p.add_argument("--ticker", default="TSLA")
    p.add_argument("--date", default="2025-06-03")
    p.add_argument("--server", default="mcp_trading_server.py")
    p.add_argument("--panel", default="data/processed/panel/daily_panel_prices_returns_fundamentals.csv")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--q_buy", type=float, default=0.7)
    p.add_argument("--q_sell", type=float, default=0.3)
    p.add_argument("--thresh_start", default="2022-01-03")
    p.add_argument("--thresh_end", default="2024-12-31")
    p.add_argument("--method", choices=["fixed", "percentile"], default="percentile")
    p.add_argument("--buy_th", type=float, default=0.25)
    p.add_argument("--sell_th", type=float, default=-0.25)
    return p.parse_args()


def build_queries_sample(args) -> list[tuple[str, str, str]]:
    ticker = args.ticker.upper()
    date = args.date

    if args.method == "percentile":
        q = (
            f"Use predict_action for {ticker} on {date} "
            "with threshold_method='percentile', "
            f"q_buy={args.q_buy}, q_sell={args.q_sell}, "
            f"start_date='{args.thresh_start}', end_date='{args.thresh_end}' "
            "and return only the tool result."
        )
    else:
        q = (
            f"Use predict_action for {ticker} on {date} "
            "with threshold_method='fixed', "
            f"buy_threshold={args.buy_th}, sell_threshold={args.sell_th} "
            "and return only the tool result."
        )

    return [(ticker, date, q)]



def build_queries_all(args) -> list[tuple[str, str, str]]:
    df = pd.read_csv(args.panel, usecols=["ticker", "date"])
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna().drop_duplicates().sort_values(["ticker", "date"]).reset_index(drop=True)

    queries = []
    for _, r in df.head(args.limit).iterrows():
        t = r["ticker"]
        d = str(r["date"])

        if args.method == "percentile":
            q = (
                f"Use predict_action for {t} on {d} "
                "with threshold_method='percentile', "
                f"q_buy={args.q_buy}, q_sell={args.q_sell}, "
                f"start_date='{args.thresh_start}', end_date='{args.thresh_end}' "
                "and return only the tool result."
            )
        else:
            q = (
                f"Use predict_action for {t} on {d} "
                "with threshold_method='fixed', "
                f"buy_threshold={args.buy_th}, sell_threshold={args.sell_th} "
                "and return only the tool result."
            )

        queries.append((t, d, q))

    return queries



async def run_queries(queries: list[tuple[str, str, str]], server_script: Path) -> list[dict]:
    results = []

    async with MCPServerStdio(
        name="TradingTools",
        params={"command": sys.executable, "args": ["-u", str(server_script)]},
        cache_tools_list=True,
    ) as mcp_server:
        agent = Agent(
            name="TradeDecisionAgent",
            instructions=(
                "You are a trading agent. "
                "Always call predict_action first. "
                "Return only the tool JSON output."
            ),
            mcp_servers=[mcp_server],
        )

        for ticker, date, query in queries:
            out = await Runner.run(agent, query)
            try:
                parsed = json.loads(out.final_output)
            except Exception:
                parsed = {"ok": False, "ticker": ticker, "date": date, "raw": out.final_output}
            results.append(parsed)

    return results


async def main() -> None:
    args = parse_args()
    server_script = Path(args.server).resolve()

    if args.mode == "sample":
        queries = queries = build_queries_sample(args)
    else:
        queries = build_queries_all(args)

    results = await run_queries(queries, server_script)

    if args.mode == "sample":
        print(json.dumps(results[0], indent=2))
    else:
        out_path = Path("outputs")
        out_path.mkdir(parents=True, exist_ok=True)
        out_file = out_path / "trade_actions.jsonl"
        with out_file.open("w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")
        print(f"Wrote {len(results)} rows to {out_file}")


if __name__ == "__main__":
    asyncio.run(main())


# python agent_trade.py --mode sample --ticker TSLA --date 2025-04-03 --method percentile --q_buy 0.85 --q_sell 0.15
# python agent_trade.py --mode all --limit 200 --method percentile --q_buy 0.7 --q_sell 0.3
