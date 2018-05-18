# coding: utf-8

"""
input_table: 
output_table:
"""

#!/usr/bin/env python
from __future__ import division
import datetime
import sys
import time
from datetime import timedelta
import numpy as np
import pandas as pd
from pyspark import SparkContext, SparkConf, HiveContext

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext()
    hqlContext = HiveContext(sc)
    
    
    ### 传参/指定参数部分
    # 传入日期，与当前日期的时间差，返回新日期
    def compute_date(raw_date, diff):
        format_date = datetime.datetime.strptime(raw_date, '%Y%m%d').date()
        delta = timedelta(days=diff)
        new_date = (format_date + delta).strftime("%Y%m%d")
        return new_date
    
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
    
    today = run_date
    
    
    ### 分区建表检查
    cust_diagnosis_cols = hqlContext.sql('desc tag_stat.cust_diagnosis').toPandas()
    if 'part_date' not in cust_diagnosis_cols.col_name.values:
        raise ValueError(u'错误！cust_diagnosis表不是分区表！')
            
    # hqlContext.sql('''
        # create external table if not exists tag_stat.cust_diagnosis
        # (
            # customer_no string,
            # customer_diagnosis string,
            # calendar_year string
        # )
        # partitioned by (part_date varchar(8))
        # stored as parquet
    # ''')
    
    
    ### 生成当日客户诊断并插入分区
    parts = hqlContext.sql("show partitions tag_stat.alldata").toPandas()
    if ('part_date='+today) not in parts.result.values:
        print u'alldata缺少当日数据，无法生成客户诊断！'
        print "missing today's alldata, can not generate data !"
    else:
        hqlContext.sql('''
            insert overwrite table tag_stat.cust_diagnosis
            partition(part_date=''' + today + ''')
            select customer_no,
                   regexp_replace(
                       concat('[
                                {\"overall\": \"',overall_rank,'\"},
                                {\"customer_financing\": \"',fin_times_year,'\", 
                                 \"total_financing\":\"',occur_balance_year,'\", 
                                 \"annual_average\": \"',avg_occur_balance,'\",
                                 \"credit_account\": \"',asset_change,'\", 
                                 \"annual_average_daily\": \"',avg_asset_year,'\", 
                                 \"annual_return\": \"',annual_profit,'\", 
                                 \"yield\": \"',profit_rate_year,'\"},
                                {\"statistics\": 
                                 [
                                  {\"name\":\"年度收益率\",\"ranking\":\"',profit_rate_year_rank,'\",\"show\":\"',profit_rate_year,'\"},
                                  {\"name\":\"选股成功率\",\"ranking\":\"',stock_selection_ability_rank,'\",\"show\":\"',stock_selection_ability_value,'\"},
                                  {\"name\":\"买卖正确率\",\"ranking\":\"',clever_rank,'\",\"show\":\"',clever_ratio,'\"},
                                  {\"name\":\"平均仓位率\",\"ranking\":\"',stock_hold_ratio_rank,'\",\"show\":\"',stock_hold_ratio,'\"},
                                  {\"name\":\"平均亏损率\",\"ranking\":\"',loss_rate_rank,'\",\"show\":\"',loss_rate_value,'\"}
                                 ]
                                }
                               ]'
                             ),
                   '\n| ', '') as customer_diagnosis,
                   calendar_year as calendar_year
            from tag_stat.alldata
            where part_date = ''' + today + '''
        ''')
    
    sc.stop()
