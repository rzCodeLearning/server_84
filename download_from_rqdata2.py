import pandas as pd
import rqdatac as rq
import os
import sys
import shutil


def download_data2(date: str):
    rq.init()
    output_dir = f'/data/zrz/rqdata_daily/daily_data2/{date}/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    code_df = rq.all_instruments(type='CS', market='cn', date=date).query("status=='Active'")
    code_list = code_df['order_book_id']

    fdd_df = rq.get_factor(code_list, ['pe_ratio_ttm', 'pb_ratio_lf', 'ps_ratio_ttm', 'ebit_ttm', 'ebitda_ttm',
                                       'return_on_equity_ttm', 'net_profit_parent_company'], date, date, universe=None,
                           expect_df=True)
    fdd_df = fdd_df.reset_index(level=1, drop=True)

    fdd_df.index = [ticker_converter(x) for x in fdd_df.index]
    fdd_df.to_csv(f'{output_dir}/data_df2_{date}.csv')

    exposure_df = rq.get_factor_exposure(code_list, date, date).reset_index()
    exposure_df['order_book_id'] = exposure_df['order_book_id'].apply(lambda x: ticker_converter(x))
    exposure_df.to_csv(f'{output_dir}/exposure_{date}.csv')

    factor_covariance_df = rq.get_factor_covariance(date, 'daily', model='v1')
    factor_covariance_df.to_csv(f'{output_dir}/factor_covariance_{date}.csv')

    factor_return_df = rq.get_factor_return(date, date, factors=None, universe='whole_market', method='implicit',
                                            industry_mapping='sws_2021', model='v1')
    factor_return_df.to_csv(f'{output_dir}/factor_return_{date}.csv')

    specific_ret_df = rq.get_specific_return(code_list, date, date, model='v1').T
    specific_ret_df.columns = ['specific_return']
    specific_risk_df = rq.get_specific_risk(code_list, date, date, horizon='daily', model='v1').T
    specific_risk_df.columns = ['specific_risk']
    specific_df = pd.concat([specific_ret_df, specific_risk_df], axis=1)
    specific_df.index = [ticker_converter(x) for x in specific_df.index]
    specific_df.to_csv(f'{output_dir}/specific_df_{date}.csv')

    shutil.make_archive(f'{output_dir}', 'zip', f'{output_dir}')
    os.rename(f'/data/zrz/rqdata_daily/daily_data2/{date}.zip', f'/data/zrz/rqdata_daily/daily_zip/risk_{date}.zip')
    os.system(f'scp /data/zrz/rqdata_daily/daily_zip/risk_{date}.zip '
              f'zhangym@172.16.10.104:/app/zrz/rqdata/zips/risk_{date}.zip')


def ticker_converter(code: str) -> str:
    if code[0] == '6':
        ans = code[0:6] + ".SH"
    else:
        ans = code[0:6] + ".SZ"
    return ans


if __name__ == '__main__':
    td = sys.argv[1]
    download_data2(td)
