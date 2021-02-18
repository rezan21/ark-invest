from tqdm import tqdm
import pandas as pd
import concurrent.futures
import pandas_datareader.data as web

class UtilsIO:
    def read_df(config):
        try:
            return pd.read_csv(config["path"], usecols=config["usecols"], nrows=config["nrows"])
        except Exception as err:
            print(f"file-level error: {err} for file: {config['path']}")

            
    def parallel_read_df(files, usecols=None, nrows=None, max_workers=2, returns="df"):
        configs = [{"path":f, "usecols":usecols, "nrows":nrows} for f in files]
        df = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            df.extend(tqdm(executor.map(UtilsIO.read_df, configs), total=len(configs)))
        if returns=="df":
            return pd.concat(df, ignore_index=True)
        elif returns=="list":
            return df
        else:
            raise ValueError("returns can be 'df' or 'list'")
         
        
class UtilsFinancial:
    def fetch_yahoo_daily(config):
        try:
            # fetch daily market data from yahoo finance
            df = web.get_data_yahoo(config["ticker"], start=config["date"], end=config["date"])[[config["on"]]]
            df["ticker"] = config["ticker"]
            df.drop_duplicates(inplace=True)
            return df
        except Exception as err:
            print(err)
            
            
    def parallel_fetch_yahoo_daily(open_configs, max_workers=2):
        open_prices = [] # list of dfs
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            open_prices.extend(executor.map(UtilsFinancial.fetch_yahoo_daily, open_configs))
        open_prices = pd.concat(open_prices)
        open_prices.reset_index(inplace=True)
        return open_prices
