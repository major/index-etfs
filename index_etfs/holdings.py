"""Get the holdings of the FFTY IBD 50 ETF."""

import pandas as pd


def get_spy_holdings():
    """Get the holdings of the SPY ETF."""
    holdings_url = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    df = pd.read_excel(holdings_url, header=4)

    df = df[df["Local Currency"] == "USD"]
    df = df[df["Ticker"] != "-"]

    # Select the columns we care about
    df = df[["Ticker", "Name", "Weight"]].sort_values(by="Ticker")

    df.to_csv("spy.csv", index=False)
    df.to_markdown("spy.md", index=False)


def get_qqq_holdings():
    """Get the holdings of the QQQ ETF."""
    holdings_url = "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=QQQ"
    df = pd.read_csv(holdings_url, thousands=",")

    df = df[["Holding Ticker", "Name", "Weight"]].sort_values(by="Holding Ticker")

    df.to_csv("qqq.csv", index=False)
    df.to_markdown("qqq.md", index=False)


def get_iwm_holdings():
    """Get the holdings of the IWM ETF."""
    iwm_holdings_url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    df = pd.read_csv(iwm_holdings_url, header=9, thousands=",")

    df = df[df["Market Currency"] == "USD"]
    df = df[df["Ticker"] != "-"]

    df = df[["Ticker", "Name", "Weight (%)"]].sort_values(by="Ticker")

    df.to_csv("iwm.csv", index=False)
    df.to_markdown("iwm.md", index=False)


def main():
    """Main function to execute the script."""
    get_spy_holdings()
    get_qqq_holdings()
    get_iwm_holdings()


if __name__ == "__main__":
    main()
