import pandas as pd
import rqdatac as rq
import os
import sys
import shutil


def download_data1(date: str):
    rq.init()

    # trading_dates
    with open('/home/zrz/rqdata_daily/trading_dates.txt', 'r') as file:
        trading_dates = file.readlines()
    tmr = rq.get_next_trading_date(date, 1, market='cn').strftime("%Y%m%d")
    tmr = tmr + '\n'
    trading_dates.append(tmr)
    with open('/home/zrz/rqdata_daily/trading_dates.txt', 'w') as file:
        file.writelines(trading_dates)

    output_dir = f'/data/zrz/rqdata_daily/daily_data1/{date}/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    shutil.copyfile('/home/zrz/rqdata_daily/trading_dates.txt', f'{output_dir}/trading_dates.txt')

    code_df = rq.all_instruments(type='CS', market='cn', date=date).query("status=='Active'")
    code_list = code_df['order_book_id']
    listed_date = code_df[['order_book_id', 'listed_date']].set_index('order_book_id')
    sws_ind_df = rq.get_instrument_industry(code_list, source='sws', level=1, date=date, market='cn')
    sws_ind_df.columns = ['sws1_industry_code', 'sws1_industry_name']
    citics_ind_df = rq.get_instrument_industry(code_list, source='citics_2019', level=1, date=date, market='cn')
    citics_ind_df.columns = ['citics1_industry_code', 'citics1_industry_name']

    price_df = rq.get_price(code_list, start_date=date, end_date=date, frequency='1d', fields=None, adjust_type='none',
                            skip_suspended =False, market='cn', expect_df=True, time_slice=None)
    price_df = price_df.reset_index(level=1, drop=True)

    adjf = rq.get_ex_factor(code_list, end_date=date, market='cn').sort_index()
    adjf2 = adjf.drop_duplicates('order_book_id', keep='last').reset_index(drop=True)
    adjf2 = adjf2.set_index('order_book_id').reindex(code_list)
    adjf2 = adjf2['ex_cum_factor'].fillna(1)

    share_df = rq.get_shares(code_list, date, date, fields=['circulation_a', 'total', 'total_a'])
    share_df = share_df.reset_index(level=1, drop=True)

    is_st = rq.is_st_stock(code_list, date, date).T
    is_st.columns = ['is_st']

    ans_df = pd.concat([listed_date, sws_ind_df, citics_ind_df, price_df, adjf2, share_df, is_st], axis=1)
    ans_df.index = [ticker_converter(x) for x in ans_df.index]
    ans_df.to_csv(f'{output_dir}/data_df1_{date}.csv')

    hs300 = rq.index_weights('000300.XSHG', date=date)
    zz500 = rq.index_weights('000905.XSHG', date=date)
    zz800 = rq.index_weights('000906.XSHG', date=date)
    zz1000 = rq.index_weights('000852.XSHG', date=date)

    hs300.index = [ticker_converter(x) for x in hs300.index]
    zz500.index = [ticker_converter(x) for x in zz500.index]
    zz800.index = [ticker_converter(x) for x in zz800.index]
    zz1000.index = [ticker_converter(x) for x in zz1000.index]

    hs300.to_csv(f'{output_dir}/hs300_{date}.csv')
    zz500.to_csv(f'{output_dir}/zz500_{date}.csv')
    zz800.to_csv(f'{output_dir}/zz800_{date}.csv')
    zz1000.to_csv(f'{output_dir}/zz1000_{date}.csv')

    indices_list = ['000001.XSHG', '000016.XSHG', '000300.XSHG', '000852.XSHG', '000905.XSHG', '000906.XSHG',
                    '000985.XSHG', '399001.XSHE', '399005.XSHE', '399006.XSHE', '399303.XSHE']
    index_price_df = rq.get_price(indices_list, start_date=date, end_date=date, frequency='1d',
                                  fields=['open', 'close'], adjust_type='none').reset_index()
    index_price_df['order_book_id'] = index_price_df['order_book_id'].apply(lambda x: x[0:6]+'.SH' if x.endswith('G')
                                                                            else x[0:6]+'.SZ')
    index_price_df.to_csv(f'{output_dir}/indices_price_{date}.csv')

    shutil.make_archive(f'{output_dir}', 'zip', f'{output_dir}')
    os.rename(f'/data/zrz/rqdata_daily/daily_data1/{date}.zip', f'/data/zrz/rqdata_daily/daily_zip/daily1_{date}.zip')
    os.system(f'scp /data/zrz/rqdata_daily/daily_zip/daily1_{date}.zip '
              f'zhangym@172.16.10.104:/app/zrz/rqdata/zips/daily1_{date}.zip')


def ticker_converter(code: str) -> str:
    if code[0] == '6':
        ans = code[0:6] + ".SH"
    else:
        ans = code[0:6] + ".SZ"
    return ans


if __name__ == '__main__':
    td = sys.argv[1]
    download_data1(td)
