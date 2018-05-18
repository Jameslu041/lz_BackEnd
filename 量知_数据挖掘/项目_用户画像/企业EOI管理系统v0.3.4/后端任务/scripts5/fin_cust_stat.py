# coding:utf-8
#!/usr/bin/env python

"""
input_table: fact_cust_compact_detail, agg_cust_balance, dim_customer
output_table: fin_cust_stat
"""


# Li Mongmong
# 2017.09.13
# Gu Quan
# 2017.12.14
import sys
from pyspark import SparkContext,SparkConf,HiveContext
import datetime
from datetime import timedelta
import time
from pyspark.sql.functions import *
from pandas import Series
import pandas as pd


#传入日期，与当前日期的时间差，返回新日期
def compute_date(raw_date,diff):
    format_date = datetime.datetime.strptime(raw_date,'%Y%m%d').date()
    delta = timedelta(days=diff)  #计算时间差
    new_date = (format_date+delta).strftime("%Y%m%d")
    return new_date


if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    hqlContext = HiveContext(sc)
    
    db_name = ['ctprod','tag_stat']

    args = sys.argv[1:]
    if len(args) == 0:
        run_date = compute_date(time.strftime("%Y%m%d"), -1)
    else:
        run_date = args[0]
    
    # 日期转换
    sys_calendar = hqlContext.sql('''
        select * from '''+db_name[0]+'''.sys_calendar
    ''').toPandas()
    run_date_jyr = sys_calendar.jyr[sys_calendar.zrr==run_date].values[0]
    firstdayofyear = run_date[:4] + '0101'
    firstdayofyear_jyr = sys_calendar.jyr[sys_calendar.zrr==firstdayofyear].values[0]
    
    
    # 融资买入统计
    fin_times_year = hqlContext.sql('''
        select customer_no,
               count(distinct serial_no) as fin_times_year
        from(select customer_no,
                    serial_no
             from '''+db_name[0]+'''.fact_cust_compact_detail
             where part_date>='''+firstdayofyear+''' and
                   part_date<='''+run_date+''' and
                   exchange_type='融资买入') a
        group by customer_no
    ''')
    fin_times_year.registerTempTable('fin_times_year_temp')
    
    occur_balance_year = hqlContext.sql('''
        select customer_no,
               sum(occur_balance) as occur_balance_year
        from(select customer_no,
                    nvl(occur_balance,0) as occur_balance
             from '''+db_name[0]+'''.fact_cust_compact_detail
             where part_date>='''+firstdayofyear+''' and
                   part_date<='''+run_date+''' and
                   exchange_type='融资买入') a
        group by customer_no
    ''')
    occur_balance_year.registerTempTable('occur_balance_year_temp')
    
    avg_occur_balance = hqlContext.sql('''
        select customer_no,
               avg(occur_balance) as avg_occur_balance
        from(select customer_no,
                    occur_balance
             from '''+db_name[0]+'''.fact_cust_compact_detail
             where part_date>='''+firstdayofyear+''' and
                   part_date<='''+run_date+''' and
                   exchange_type='融资买入' and
                   occur_balance is not NULL) a
        group by customer_no
    ''')
    avg_occur_balance.registerTempTable('avg_occur_balance_temp')
    
    
    # 资产统计
    avg_asset = hqlContext.sql('''
        select customer_no,
               avg(total_close_asset) as avg_asset_year
        from(select customer_no,
                    total_close_asset,
                    init_date
             from '''+db_name[0]+'''.agg_cust_balance
             where part_date>='''+firstdayofyear+''' and
                   part_date<='''+run_date+''' and
                   total_close_asset is not NULL) a
        group by customer_no
    ''')
    avg_asset.registerTempTable('avg_asset_temp')
    
    asset_today = hqlContext.sql('''
        select customer_no,
               total_close_asset
        from(select customer_no,
                    total_close_asset,
                    row_number() over(partition by customer_no order by init_date desc) as rn
             from '''+db_name[0]+'''.agg_cust_balance
             where part_date>='''+run_date_jyr+''' and
                   part_date<='''+run_date+''' and
                   customer_no is not NULL and
                   total_close_asset is not NULL) a
        where rn = 1
    ''')
    asset_today.registerTempTable('asset_today_temp')
    
    asset_0101 = hqlContext.sql('''
        select customer_no,
               total_close_asset
        from(select customer_no,
                    total_close_asset,
                    row_number() over(partition by customer_no order by init_date desc) as rn
             from '''+db_name[0]+'''.agg_cust_balance
             where part_date<='''+firstdayofyear+''' and
                   part_date>='''+firstdayofyear_jyr+''' and
                   customer_no is not NULL and
                   total_close_asset is not NULL) a
        where rn = 1
    ''')
    asset_0101.registerTempTable('asset_0101_temp')
    
    asset_change = hqlContext.sql('''
        select a.customer_no,
               (a.total_close_asset-b.total_close_asset) as asset_change
        from asset_today_temp a
        join asset_0101_temp b
        on a.customer_no = b.customer_no
    ''')
    asset_change.registerTempTable('asset_change_temp')
    
    
    # 本年收益率统计
    profit_rate_year = hqlContext.sql('''
        select *,
               (case when profit_rate_year is null then '本年收益率未知'
                     when profit_rate_year>0.15 then '本年收益率高'
                     when profit_rate_year>0.05 then '本年收益率中等'
                     when profit_rate_year>0.00 then '本年收益率低'
                     when profit_rate_year>-0.08 then '本年收益率低亏损'
                     when profit_rate_year>-0.30 then '本年收益率中等亏损'
                     else '本年收益率高亏损' 
               end) as profit_rate_year_level
        from(select b.customer_no,
                    annual_profit,
                    (case when avg_asset_year>0 then annual_profit/avg_asset_year else NULL end) as profit_rate_year
             from(select customer_no,
                         sum(total_profit) as annual_profit
                  from(select customer_no,
                              total_profit
                       from '''+db_name[0]+'''.agg_cust_balance
                       where part_date>='''+firstdayofyear+''' and
                             part_date<='''+run_date+''' and
                             customer_no is not NULL) a
                  group by customer_no) b,
            avg_asset_temp c
            where b.customer_no = c.customer_no) d
    ''')
    profit_rate_year.registerTempTable('profit_rate_year_temp')
    
    # 整合
    res_all = hqlContext.sql('''
        select cust.customer_no,
               nvl(a.fin_times_year,0) as fin_times_year,
               nvl(b.occur_balance_year,0) as occur_balance_year,
               nvl(c.avg_occur_balance,0) as avg_occur_balance,
               d.avg_asset_year,
               e.asset_change,
               f.profit_rate_year,
               f.profit_rate_year_level,
               f.annual_profit
        from (select distinct customer_no 
              from '''+db_name[0]+'''.dim_customer) cust
        left outer join fin_times_year_temp a
        on cust.customer_no = a.customer_no
        left outer join occur_balance_year_temp b
        on cust.customer_no = b.customer_no
        left outer join avg_occur_balance_temp c
        on cust.customer_no = c.customer_no
        left outer join avg_asset_temp d
        on cust.customer_no = d.customer_no
        left outer join asset_change_temp e
        on cust.customer_no = e.customer_no
        left outer join profit_rate_year_temp f
        on cust.customer_no = f.customer_no
    ''')
    
    # 标签缺失补全
    res_all = res_all.na.fill({'profit_rate_year_level': u'本年收益率未知'})
    res_all.registerTempTable("res_all")
    
    hqlContext.sql('drop table if exists tag_stat.fin_cust_stat')
    hqlContext.sql('''
        create table '''+db_name[1]+'''.fin_cust_stat as 
        select * from res_all
    ''')
    
    sc.stop()