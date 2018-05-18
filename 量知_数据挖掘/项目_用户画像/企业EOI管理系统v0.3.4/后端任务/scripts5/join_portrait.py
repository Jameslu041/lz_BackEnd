# coding:utf-8

import datetime
import sys
import time
from datetime import timedelta
import calendar
import numpy as np
import pandas as pd
from pyspark import SparkContext, SparkConf, HiveContext

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    hqlContext = HiveContext(sc)
    
    ### 传参/指定参数部分
    # 传入日期，与当前日期的时间差，返回新日期
    def compute_date(raw_date, diff):
        format_date = datetime.datetime.strptime(raw_date, '%Y%m%d').date()
        delta = timedelta(days=diff)
        new_date = (format_date + delta).strftime("%Y%m%d")
        return new_date
    
    # 返回与某个日期相距 N 个月份对应的日期
    # 如compute_month('20170702',7)返回'20180202'，compute_month('20170831',-6)返回'20170228'
    def compute_month(raw_date, month_diff):
        format_date = datetime.datetime.strptime(raw_date, '%Y%m%d').date()
        day = format_date.day
        if format_date.month + month_diff <= 0:
            month =  format_date.month + month_diff + 12
            year = format_date.year - 1
        elif (format_date.month + month_diff > 0) and (format_date.month + month_diff <= 12):
            month = format_date.month + month_diff
            year = format_date.year
        else:
            month =  format_date.month + month_diff - 12
            year = format_date.year + 1
        daysInMonth = calendar.monthrange(year, month)[1] 
        day = min(day, daysInMonth)
        new_date = datetime.datetime(year, month, day)
        return new_date.strftime("%Y%m%d")
    
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
    today_wc = run_date
    six_month_ago_wc = compute_month(today_wc, -6) # 注意：在财通跑该脚本时，需要先保证wordcloud里面跑了6个月1号的数据
    month_begin = today_wc[0:6] + '01'
    month_end = today_wc[0:6] + '31'
    
    
    ### 生成客户画像
    exist_tbls = hqlContext.sql("show tables in tag_stat").toPandas()
    tbls_need = pd.Series(['alldata','cust_diagnosis','rcmd','wordcloud'])
    if sum(tbls_need.isin(exist_tbls.tableName))>=4:
        sign = 0

        parts1 = hqlContext.sql("show partitions tag_stat.alldata").toPandas()
        parts2 = hqlContext.sql("show partitions tag_stat.cust_diagnosis").toPandas()
        parts3 = hqlContext.sql("show partitions tag_stat.rcmd").toPandas()
        parts4 = hqlContext.sql("show partitions tag_stat.wordcloud").toPandas()
        if ('part_date='+today) not in parts1.result.values:
            sign = 1
            print "alldata 缺少当日数据，无法生成画像！"
            print "missing today's alldata !"
        if ('part_date='+today) not in parts2.result.values:
            sign = 1
            print "cust_diagnosis 缺少当日数据，无法生成画像！"
            print "missing today's cust_diagnosis !"
        if ('part_date='+today) not in parts3.result.values:
            sign = 1
            print "rcmd 缺少当日数据，无法生成画像！"
            print "missing today's rcmd !"
        if ('part_date='+month_begin) not in parts4.result.values:
            print "wordcloud dont have data on first day of the month!"
            if all(parts4.result.values < str('part_date='+month_begin)):
                sign = 1
                print u"wordcloud 当月完全没有数据，无法生成画像！"
                print "missing this month's wordcloud !"
            else:
                wc_date = max(parts4.result[parts4.result<=str('part_date='+month_end)])
                print "find other date exists, wordcloud ues date: "+wc_date
        
        if sign == 0: # 表明四张表都不缺少当日分区数据
            customer_portrait = hqlContext.sql('''
                select ''' + run_date + ''' as init_date,
                       tb1.customer_no,
                       tb1.customer_name,
                       tb1.branch_name,
                       tb1.fin_balance,         
                       tb1.old_total_asset,     
                       tb1.credit_assure_scale, 
                       tb1.stock_hold,          
                       tb1.concentrate,
                       tb2.customer_diagnosis,
                       tb1.stock_hold_array,
                       tb3.cust_rcmd,
                       tb4.wordcloud,
                       tb1.income_array,
                       tb1.credit_assure_scale_array,
                       tb1.in_balance,
                       tb1.out_balance,
                       tb1.rest_balance,
                       tb2.calendar_year,
                       tb1.stock_hold_analysis
                from (select * from tag_stat.alldata
                      where part_date = ''' + today + '''
                      ) tb1 
                left outer join 
                     (select * from tag_stat.cust_diagnosis
                      where part_date = ''' + today + '''
                      ) tb2 
                on tb1.customer_no=tb2.customer_no
                left outer join 
                     (select * from tag_stat.rcmd
                      where part_date = ''' + today + '''
                      ) tb3 
                on tb1.customer_no=tb3.customer_no
                left outer join 
                     (
                      select customer_no,
                             concat_ws(', ', collect_list(wordcloud)) as wordcloud
                      from tag_stat.wordcloud
                      where part_date <= ''' + today_wc + ''' and
                            part_date > ''' + six_month_ago_wc + '''
                      group by customer_no
                     ) tb4
                on tb1.customer_no=tb4.customer_no
            ''')
            customer_portrait.registerTempTable('customer_portrait')
            hqlContext.sql('''
                drop table if exists tag_stat.customer_portrait
            ''')
            hqlContext.sql('''
                create table tag_stat.customer_portrait as select * from customer_portrait
            ''')
        
        else:
            print "缺少当日数据分区！"
            print "missing today's data !"
    
    else:
        print u"缺少相关的表，无法生成结果！！"
        print "missing tables, can not generate portrait !!"
    
    sc.stop()

