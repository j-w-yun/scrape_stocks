# scrape_stocks

Scrapes daily stock OHLC, short volume, and total volume of all Nasdaq and NYSE stocks from 3/1/2011 to the present.

Dependencies
```
pip install xone==0.1.6
pip install pandas==1.2.2
pip install pytz==2021.1
pip install requests==2.25.1
pip install yfinance==0.1.55
```

Run to write/update `stock_data/{SYMBOL}.csv`
```
python scrape_stocks.py
```
