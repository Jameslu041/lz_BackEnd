# coding:utf-8

"""
input:Fact_Stock_Market_Summary, agg_cust_balance, dim_customer
output: tag_stat.market_info

"""

import sys
import time
import datetime
from datetime import timedelta
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
import pyspark.sql.functions as func
import pandas as pd

# 传入日期，与当前日期的时间差，返回新日期
def compute_date(raw_date, diff):
    format_date = datetime.datetime.strptime(raw_date, '%Y%m%d').date()
    delta = timedelta(days=diff)
    new_date = (format_date + delta).strftime("%Y%m%d")
    return new_date

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    hqlContext = HiveContext(sc)
    ### 传参/指定参数部分
    args = sys.argv[1:]  # 接受参数，run_date、数据库名db_name, 两个参数
    # 未传入任何参数，使用默认值，ctprod
    if len(args) == 0:
        run_date = compute_date(time.strftime("%Y%m%d"), -1)  # 默认run_date
        db_name = 'ctprod'  # 默认数据库名
    elif len(args) == 1:
        run_date = args[0]
        db_name = 'ctprod'  # 默认数据库名
    else:
        run_date = args[0]
        db_name = args[1]
    
    
    # 日期转换
    sys_calendar = hqlContext.sql('''
        select * from '''+db_name+'''.sys_calendar
    ''').toPandas()
    date_st = compute_date(run_date, -365)  # 12个月前
    date_end = run_date
    date_st_year = run_date[:4] + '0101'
    date_st = sys_calendar.jyr[sys_calendar.zrr==date_st].values[0]
    date_end = sys_calendar.jyr[sys_calendar.zrr==date_end].values[0]
    date_st_year = sys_calendar.jyr[sys_calendar.zrr==date_st_year].values[0]
    
    
    
    # 1. 客户相对收益率
    
    # 同期大盘增长
    dapan_rise = hqlContext.sql('''
        select b.market_no, 
               b.close / a.close as dapan 
        from (select market_no, 
                     market_name, 
                     close 
              from '''+db_name+'''.Fact_Stock_Market_Summary
              where init_date==''' + date_st + ''' ) a 
        join (select market_no, 
                     market_name, 
                     close 
              from '''+db_name+'''.Fact_Stock_Market_Summary
              where init_date==''' + date_end + ''' ) b 
        on a.market_no=b.market_no
        where b.market_no='000001'
    ''').collect()
    dapan_rise = str(dapan_rise[0]['dapan'] - 1)
    
    #增加客户绝对收益
    #收益率公式中分子改为累计收益total_profit
    khxdsy = hqlContext.sql('''
        select a.customer_no, 
               (a.sum_profit / a.avg_asset) as rate_absolute,
               (a.sum_profit / a.avg_asset - ''' + dapan_rise + ''') as rate_relative
        from (select customer_no, 
                     sum(total_profit) as sum_profit,
                     avg(total_close_asset) as avg_asset 
              from '''+db_name+'''.agg_cust_balance
              where part_date>=''' + date_st + ''' and
                    part_date<=''' + date_end + '''
              group by customer_no) a
    ''')
    khxdsy.registerTempTable('khxdsy_temp')
    
    
    
    # 2. 相对强弱值
    
    # 客户相对强弱
    kh_temp = hqlContext.sql('''
        select customer_no, 
               sum(abs(diff)) as diff_total, 
               diff_sign
        from (select customer_no, 
                     rjzc-rjzc0 as diff,
                     (case when rjzc-rjzc0>0 then 'up'
                           when rjzc-rjzc0<0 then 'down'
                           else 'keep'
                      end) as diff_sign
              from (select customer_no, 
                           init_date, 
                           days, 
                           nzzc/days as rjzc, 
                           lag(nzzc/days,1,0) over (partition by customer_no order by init_date) as rjzc0
                    from (select customer_no, 
                                 init_date, 
                                 total_close_asset,
                                 sum(total_close_asset) over (partition by customer_no order by init_date) as nzzc, 
                                 row_number() over (partition by customer_no order by init_date) as days
                          from (select cast(customer_no as string), 
                                       cast(init_date as string), 
                                       total_close_asset
                                from '''+db_name+'''.agg_cust_balance) a
                          where init_date>=''' + date_st_year + ''' and 
                                init_date<=''' + date_end + ''') b) c
              where days>1) d 
        group by customer_no, diff_sign
    ''')
    kh_temp.registerTempTable("kh_temp")
    
    khxdqr = hqlContext.sql('''
        select a.customer_no, 
               nvl(b.up,0) / (nvl(b.up,0.000001) + nvl(c.down,0)) as kehu
        from (select distinct customer_no
              from kh_temp) a
        left outer join 
                       (select customer_no, 
                               diff_total as up 
                        from kh_temp 
                        where diff_sign='up') b
        on a.customer_no=b.customer_no
        left outer join 
                       (select customer_no, 
                               diff_total as down
                        from kh_temp 
                        where diff_sign='down') c
        on a.customer_no=c.customer_no
    ''')
    khxdqr.registerTempTable("khxdqr")
    
    
    # 市场相对强弱
    sc_temp = hqlContext.sql('''
        select market_no, 
               sum(abs(diff)) as diff_total, 
               diff_sign
        from (select market_no, 
                     close-close0 as diff,
                     (case when close-close0>0 then 'up'
                           when close-close0<0 then 'down'
                           else 'keep'
                      end) as diff_sign
              from (select init_date, 
                           market_no, 
                           market_name, 
                           close, 
                           lag(close,1,0) over (partition by market_no order by init_date) as close0,
                           row_number() over (partition by market_no order by init_date) as num
                    from '''+db_name+'''.Fact_Stock_Market_Summary
                    where init_date>=''' + date_st_year + ''' and 
                          init_date<=''' + date_end + ''') a
              where num>1) b 
        group by market_no, diff_sign
    ''')
    sc_temp.registerTempTable("sc_temp")
    
    scxdqr = hqlContext.sql('''
        select a.market_no, 
               a.up/(a.up + b.down) as shichang
        from (select market_no, 
                     diff_total as up 
              from sc_temp 
              where diff_sign='up') a 
        join (select market_no, 
                     diff_total as down 
              from sc_temp 
              where diff_sign='down') b 
        on a.market_no=b.market_no
        where a.market_no='000001'
    ''').collect()
    shichang = str(scxdqr[0]['shichang'])
    
    
    # 客户市场强弱对比
    xdqr = hqlContext.sql('''
        select *,'''+ shichang +'''as shichang,
               (kehu - '''+ shichang +''') as relative_index
        from khxdqr
    ''')
    xdqr.registerTempTable('xdqr_temp')
    
    
    # 整合指标+标签
    res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               a.rate_absolute,
               a.rate_relative,
               (case when a.rate_relative is null then '相对收益未知'
                     when a.rate_relative<-0.65 then '完败给大盘'
                     when a.rate_relative>=-0.65 and a.rate_relative<-0.15 then '输给大盘'
                     when a.rate_relative>=-0.15 and a.rate_relative<0.05 then '持平大盘收益'
                     when a.rate_relative>=0.05 and a.rate_relative<1 then '跑赢大盘'
                     when a.rate_relative>=1 then '远超大盘'
                     else '相对收益未知'
                end) as rate_level,
               b.kehu,
               b.shichang,
               b.relative_index,
               (case when b.kehu is null then '资产走势未知'
                     when b.kehu<0.025 then '资产走势滑坡'
                     when b.kehu>=0.025 and b.kehu<0.20 then '资产增势低迷'
                     when b.kehu>=0.20 and b.kehu<0.60 then '资产增势低迷'
                     when b.kehu>=0.60 and b.kehu<0.80 then '资产增势强劲'
                     when b.kehu>=0.80 then '资产增势迅猛'
                     else '资产走势未知'
                end) as index_level  
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join khxdsy_temp a
        on cust.customer_no = a.customer_no
        left outer join xdqr_temp b
        on cust.customer_no = b.customer_no
    ''')
    
    
    # 标签缺失补全
    res_all = res_all.na.fill({'rate_level':u'相对收益未知', 'index_level':u'资产走势未知'})
    res_all.registerTempTable("res_all")
    
    
    # 写入hive
    hqlContext.sql('drop table if exists tag_stat.market_info')
    hqlContext.sql('''
        create table tag_stat.market_info as select * from res_all
    ''')     

    sc.stop()

