"""Get the holdings of the FFTY IBD 50 ETF."""

from pathlib import Path

import polars as pl


def get_spy_holdings():
    """Get the holdings of the SPY ETF."""
    holdings_url = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    df_dict = pl.read_excel(holdings_url, sheet_id=0, read_options={"header_row": 4})

    if isinstance(df_dict, dict):
        df = list(df_dict.values())[0]
    else:
        df = df_dict

    df = df.filter((pl.col("Local Currency") == "USD") & (pl.col("Ticker") != "-"))

    df = df.select(["Ticker", "Name", "Weight"]).sort("Weight", descending=True)

    df.with_row_index("Rank").write_csv("spy.csv")
    Path("spy.md").write_text(df.to_pandas().to_markdown())


def get_mdy_holdings():
    """Get the holdings of the MDY ETF."""
    mdy_holdings_url = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx"
    df_dict = pl.read_excel(
        mdy_holdings_url, sheet_id=0, read_options={"header_row": 4}
    )

    if isinstance(df_dict, dict):
        df = list(df_dict.values())[0]
    else:
        df = df_dict

    df = df.filter((pl.col("Local Currency") == "USD") & (pl.col("Ticker") != "-"))

    df = df.select(["Ticker", "Name", "Weight"]).sort("Weight", descending=True)

    df.with_row_index("Rank").write_csv("mdy.csv")
    Path("mdy.md").write_text(df.to_pandas().to_markdown())


def get_spsm_holdings():
    """Get the holdings of the SPSM ETF."""
    spsm_holdings_url = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spsm.xlsx"
    df_dict = pl.read_excel(
        spsm_holdings_url, sheet_id=0, read_options={"header_row": 4}
    )

    if isinstance(df_dict, dict):
        df = list(df_dict.values())[0]
    else:
        df = df_dict

    df = df.filter((pl.col("Local Currency") == "USD") & (pl.col("Ticker") != "-"))

    df = df.select(["Ticker", "Name", "Weight"]).sort("Weight", descending=True)

    df.with_row_index("Rank").write_csv("spsm.csv")
    Path("spsm.md").write_text(df.to_pandas().to_markdown())


def get_qqq_holdings():
    """Get the holdings of the QQQ ETF."""
    holdings_url = "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=QQQ"
    df = pl.read_csv(holdings_url)

    df = df.rename({"Holding Ticker": "Ticker"})
    df = df.select(["Ticker", "Name", "Weight"]).sort("Weight", descending=True)

    df.with_row_index("Rank").write_csv("qqq.csv")
    Path("qqq.md").write_text(df.to_pandas().to_markdown())


def get_iwm_holdings():
    """Get the holdings of the IWM ETF."""
    iwm_holdings_url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    df = pl.read_csv(
        iwm_holdings_url,
        skip_rows=9,
        columns=["Ticker", "Name", "Weight (%)", "Market Currency"],
    )
    df = df.rename({"Weight (%)": "Weight"})
    df = df.filter((pl.col("Market Currency") == "USD") & (pl.col("Ticker") != "-"))

    df = df.select(["Ticker", "Name", "Weight"]).sort("Weight", descending=True)

    df.with_row_index("Rank").write_csv("iwm.csv")
    Path("iwm.md").write_text(df.to_pandas().to_markdown())


def main():
    """Main function to execute the script."""
    get_spy_holdings()
    get_mdy_holdings()
    get_spsm_holdings()
    get_qqq_holdings()
    get_iwm_holdings()


if __name__ == "__main__":
    main()
