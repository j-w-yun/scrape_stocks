import csv
import io
import os
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime, timedelta
from pytz import timezone
from xone import calendar


pd.options.mode.chained_assignment = None

REGSHO_FILENAME = 'regsho_data/regsho_data.csv'
STOCKS_FILENAME = 'stock_data/{}.csv'

def trading_dates(start_date, end_date):
	"""Get all trading dates.
	"""
	ust_calendar = calendar.USTradingCalendar()
	dates = pd.bdate_range(start=start_date, end=end_date)
	holidays = ust_calendar.holidays(start=start_date, end=end_date)
	return dates.drop(holidays)

class REGSHO:
	def __init__(self, filename, delimiter='|'):
		self.filename = filename
		self.delimiter = delimiter

		# Create regsho data path
		path = self.filename.split(os.path.sep)
		cur_path = ''
		for p in path[:-1]:
			cur_path = os.path.join(cur_path, p)
			if not os.path.exists(cur_path):
				os.makedirs(cur_path)

		self.base_urls = [
			'http://regsho.finra.org/FNYXshvol{}.txt',
			'http://regsho.finra.org/FNQCshvol{}.txt',
			'http://regsho.finra.org/FNSQshvol{}.txt',
		]
		self.fieldnames = [
			'Date',
			'Symbol',
			'ShortVolume',
			'ShortExemptVolume',
			'TotalVolume',
			'Market',
		]
		self.dtypes = {
			'Date': str,
			'Symbol': str,
			'Market': str,
		}

	def file_exists(self):
		return os.path.isfile(self.filename)

	def get_data(self):
		print('Fetching saved regsho data')
		return pd.read_csv(self.filename, sep=self.delimiter, dtype=self.dtypes)

	def get_last_date(self):
		"""Get latest time from CSV.
		"""
		last_line = ''
		with open(self.filename, 'r') as f:
			f.seek(0, 2)
			fsize = f.tell()
			f.seek(max (fsize-4096, 0), 0)
			lines = f.read().splitlines()
			last_line = lines[-1]
		last_date = last_line.split(self.delimiter)[0]
		last_date = datetime.strptime(last_date, '%Y%m%d')
		return last_date

	def save_data(self, data, write_header=False):
		"""Append data to csv.
		"""
		with open(self.filename, 'a') as f:
			data.to_csv(f, header=write_header, index=False, float_format='%.0f', sep=self.delimiter)

	def download_data(self, date):
		# Request
		data = []
		for base_url in self.base_urls:
			url = base_url.format(date)
			result = requests.get(url)
			content = result.content.decode('utf-8')
			datum = pd.read_csv(io.StringIO(content), sep='|')
			datum = datum.dropna()
			data.append(datum)
		data = pd.concat(data)

		# Check stock market is closed
		for col in self.fieldnames:
			if col not in data:
				return None

		data = data[self.fieldnames]

		# Append data to csv
		self.save_data(data, write_header=(date == '20110301'))

		return data

	def update(self):
		# Earliest data
		start_date = '3/1/2011'
		# Start date from last row in CSV
		start_date = datetime.strptime(start_date, '%m/%d/%Y')
		if self.file_exists():
			start_date = self.get_last_date() + timedelta(days=1)
		start_date = start_date.strftime('%m/%d/%Y')

		# End date from EST date
		end_date = datetime.now(timezone('US/Eastern'))
		# Published after 8PM
		if end_date.hour < 20:
			end_date = end_date - timedelta(days=1)
		end_date = end_date.strftime('%m/%d/%Y')

		# Get all trading dates
		dates = trading_dates(start_date, end_date).strftime('%Y%m%d')

		for date in dates:
			# Get data
			data = self.download_data(date)

			if data is None:
				print('Closed {}'.format(date))
			else:
				print('Fetched {}'.format(date))
		print('REGSHO up-to-date')

class STOCKS:
	def __init__(self, filename, regsho, delimiter='|'):
		self.filename = filename
		self.regsho = regsho
		self.delimiter = delimiter

		# Create stock data path
		path = self.filename.split(os.path.sep)
		cur_path = ''
		for p in path[:-1]:
			cur_path = os.path.join(cur_path, p)
			if not os.path.exists(cur_path):
				os.makedirs(cur_path)

		# Create symbol data path
		self.symbol_data_dir = 'symbol_data'
		if not os.path.exists(self.symbol_data_dir):
			os.makedirs(self.symbol_data_dir)
		self.symbol_list_filename = os.path.join(self.symbol_data_dir, 'symbol_list.txt')
		self.symbol_filename = os.path.join(self.symbol_data_dir, 'symbol_data.csv')

		self.history_options = {
			# Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
			'period': '1mo',
			# Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
			'interval': '1d',
			# Adjust all OHLC automatically
			'auto_adjust': True,
			# Download pre/post regular market hours data
			'prepost': False,
			# Download stock dividends and stock splits events
			'actions': True,
			'threads': False,
			'progress': False,
		}
		self.ticker_fieldnames = [
			'symbol',
			'longName',
			'shortName',
			'industry',
			'sector',
			'phone',
			'website',
			'logo_url',
			'tradeable',
			'companyOfficers',
			'isEsgPopulated',
			'quoteType',
			'currency',
			'market',
			'exchange',
			'exchangeTimezoneName',
			'exchangeTimezoneShortName',
			'address1',
			'city',
			'state',
			'zip',
			'country',
			'longBusinessSummary',
		]

	def sanitize(self, s):
		"""Sanitize whitespace and delimiter.
		"""
		res = s
		res = res.replace(self.delimiter, ',')
		res = ' '.join(res.split())
		return res

	def get_last_date(self, symbol):
		"""Get latest time from stock data.
		"""
		last_line = ''
		with open(self.filename.format(symbol), 'r') as f:
			f.seek(0, 2)
			fsize = f.tell()
			f.seek(max (fsize-4096, 0), 0)
			lines = f.read().splitlines()
			last_line = lines[-1]
		last_date = last_line.split(self.delimiter)[0]
		last_date = datetime.strptime(last_date, '%Y-%m-%d')
		return last_date

	def get_symbols(self):
		"""Get all symbols.
		"""
		unique_symbols = self.regsho.Symbol.unique()
		symbols = []
		for s in unique_symbols:
			symbols.append(str(s))
		return symbols

	def download_symbols(self):
		symbols = self.get_symbols()

		# Check if file exists
		file_exists = False
		if os.path.isfile(self.symbol_filename):
			file_exists = True

		# Identify missing symbols
		if file_exists:
			with open(self.symbol_filename, 'r', encoding='utf-8') as f:
				dw = csv.DictReader(f, delimiter=self.delimiter)
				for row in dw:
					symbol = row['symbol'].strip()
					if symbol in symbols:
						symbols.remove(symbol)

		# Update missing symbols
		with open(self.symbol_filename, 'a', encoding='utf-8') as f:
			dw = csv.DictWriter(f, delimiter=self.delimiter, extrasaction='ignore', fieldnames=self.ticker_fieldnames)
			if not file_exists:
				dw.writeheader()

			for symbol in symbols:
				# Get ticker info
				ticker = yf.Ticker(symbol)
				try:
					info = ticker.info
					info['longBusinessSummary'] = self.sanitize(info['longBusinessSummary'])
					dw.writerow(info)
					print('Fetched {}'.format(symbol))
				except:
					print('Skipped {}'.format(symbol))

	def update(self):
		symbols = self.get_symbols()

		for symbol in symbols:
			# Check if file exists
			file_exists = False
			if os.path.isfile(self.filename.format(symbol)):
				file_exists = True

			# Find greatest common date
			short_data = self.regsho[self.regsho['Symbol'] == symbol]
			start = datetime.strptime(short_data['Date'].iloc[0], '%Y%m%d')
			end = datetime.strptime(short_data['Date'].iloc[-1], '%Y%m%d') + timedelta(days=1)

			try:
				if file_exists:
					last_date = self.get_last_date(symbol) + timedelta(days=1)
					last_date = last_date.strftime('%m/%d/%Y')

					# Get all trading dates
					dates = trading_dates(last_date, end.strftime('%m/%d/%Y')).strftime('%Y-%m-%d')

					if len(dates) == 0:
						print('Up-to-date {}'.format(symbol))
						continue

					# Download
					data = yf.download(
						tickers=symbol,
						start=dates[0],
						**self.history_options)
				else:
					# Download
					data = yf.download(
						tickers=symbol,
						start=start.strftime('%Y-%m-%d'),
						end=end.strftime('%Y-%m-%d'),
						**self.history_options)

				print('Fetched {} {} - {}'.format(symbol, data.index[0], data.index[-1]))
			except:
				print('Skipped {}'.format(symbol))
				continue

			# Concatenate with short data
			data.index = pd.to_datetime(data.index, format='%Y-%m-%d')
			short_data['Date'] = pd.to_datetime(short_data['Date'], format='%Y%m%d')
			short_data = short_data.groupby('Date')[['ShortVolume', 'ShortExemptVolume', 'TotalVolume']].sum()
			short_data = short_data[short_data.index >= data.index[0]]
			short_data = short_data[short_data.index <= data.index[-1]]
			data = pd.concat([data, short_data], axis=1)

			# Remove duplicate indices
			data = data[~data.index.duplicated(keep='first')]

			# Append to csv
			with open(self.filename.format(symbol), 'a') as f:
				data.to_csv(f, header=(not file_exists), index=True, sep=self.delimiter)
		print('STOCKS up-to-date')

if __name__ == '__main__':
	regsho = REGSHO(filename=REGSHO_FILENAME)
	regsho.update()

	stocks = STOCKS(filename=STOCKS_FILENAME, regsho=regsho.get_data())
	stocks.update()
	stocks.download_symbols()
