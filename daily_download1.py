import datetime as dt
import os

with open('/home/zrz/rqdata_daily//trading_dates.txt', 'r') as file:
    trading_dates = file.readlines()
trading_dates = [x[0:8] for x in trading_dates]

today = dt.datetime.today().strftime("%Y%m%d")

if today in trading_dates:
    os.system(f'/home/zrz/.conda/envs/rqsdk/bin/python /home/zrz/rqdata_daily/download_from_rqdata1.py {today}')
