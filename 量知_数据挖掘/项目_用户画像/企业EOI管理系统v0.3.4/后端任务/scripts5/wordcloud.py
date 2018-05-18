# coding:utf-8
import datetime
import sys
import time
from datetime import timedelta
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
    
    today = run_date
    
    nrow =  hqlContext.sql('''
        select count(1) from tag_stat.alldata 
        where part_date = ''' + today + '''
     ''')
    nrow = nrow.collect()[0][0]
    
    ### 分区建表检查
    wordcloud_cols = hqlContext.sql('desc tag_stat.wordcloud').toPandas()
    if 'part_date' not in wordcloud_cols.col_name.values:
        raise ValueError(u'错误！wordcloud表不是分区表！')
        
    # hqlContext.sql('''
        # create external table if not exists tag_stat.wordcloud
        # (
            # customer_no string,
            # wordcloud string
        # )
        # partitioned by (part_date varchar(8))
        # stored as parquet
    # ''')
    
    
    ### 生成当日标签云并插入分区
    parts = hqlContext.sql("show partitions tag_stat.alldata").toPandas()
    if ('part_date='+today) not in parts.result.values:
        print "alldata缺少当日数据，无法生成标签云！"
        print "missing today's alldata, can not generate data !"
    else:
        alltags = hqlContext.sql('''
            select customer_no,
                   index_level, index_rank, 
                   rate_level, rate_rank, 
                   clever_level,clever_rank, 
                   beta_level, beta_rank,
                   stock_hold_ratio_level, stock_hold_ratio_rank,
                   open_years_level, open_years_rank,
                   stock_concentrate_level, stock_concentrate_rank,
                   income_level, income_rank,
                   debit_level, debit_rank,
                   asset_level, asset_rank,
                   total_profit_level, total_profit_rank,
                   activity_level, activity_rank,
                   cust_devo_level, devo_ratio_rank,
                   close_rate_level, close_rate_rank,
                   profit_rate_year_level, profit_rate_year_rank,
                   turnover_rate_label, turnover_rate_rank,
                   sharp_ratio_label, sharp_ratio_rank,
                   profit_volatility_year_label, profit_volatility_year_rank,
                   unit_risk_premium_rate_label, unit_risk_premium_rate_rank,
                   stock_selection_ability_label, stock_selection_ability_rank,
                   percep_in_level, percep_in_rank, 
                   percep_out_level, percep_out_rank,
                   stock_corr_level, stock_corr_rank,
                   margin_corr_level, margin_corr_rank,
                   hold_risk_level, hold_risk_rank,
                   contri_level, contri_value_rank,
                   contri_stab_level, contri_stab_rank,
                   up_level, up_rate_rank,
                   cycle_stable_level, cycle_stable_rank,
                   control_loss_level, control_loss_rank,
                   age_tag, profession, degree_code
            from tag_stat.alldata
            where part_date = ''' + today + '''
        ''')
        
        alltags = alltags.na.fill("未知")
        alltags.registerTempTable("alltags")
        
        wordcloud_today = hqlContext.sql('''
            select customer_no,
                   concat("\\"",''' + today + ''', "\\"", ": {",
                          "\\"", index_level, "\\"", ":", ''' + str(nrow+1) + ''' - index_rank, " ,",
                          "\\"", rate_level, "\\"", ":", ''' + str(nrow+1) + ''' - rate_rank, " ,",
                          "\\"", clever_level, "\\"", ":", ''' + str(nrow+1) + ''' - clever_rank, " ,",
                          "\\"", beta_level, "\\"", ":", ''' + str(nrow+1) + ''' - beta_rank, " ,"
                          "\\"", stock_hold_ratio_level, "\\"", ":", ''' + str(nrow+1) + ''' - stock_hold_ratio_rank, " ,"
                          "\\"", open_years_level, "\\"", ":", ''' + str(nrow+1) + ''' - open_years_rank, " ,"
                          "\\"", stock_concentrate_level, "\\"", ":", ''' + str(nrow+1) + ''' - stock_concentrate_rank, " ,"
                          "\\"", income_level, "\\"", ":", ''' + str(nrow+1) + ''' - income_rank, " ,"
                          "\\"", debit_level, "\\"", ":", ''' + str(nrow+1) + ''' - debit_rank, " ,"
                          "\\"", asset_level, "\\"", ":", ''' + str(nrow+1) + ''' - asset_rank, " ,"
                          "\\"", total_profit_level, "\\"", ":", ''' + str(nrow+1) + ''' - total_profit_rank, " ,"
                          "\\"", activity_level , "\\"", ":", ''' + str(nrow+1) + ''' - activity_rank, " ,"
                          "\\"", cust_devo_level, "\\"", ":", ''' + str(nrow+1) + ''' - devo_ratio_rank, " ,"
                          "\\"", close_rate_level, "\\"", ":", ''' + str(nrow+1) + ''' - close_rate_rank, " ,"
                          "\\"", profit_rate_year_level, "\\"", ":", ''' + str(nrow+1) + ''' - profit_rate_year_rank, " ,"
                          "\\"", turnover_rate_label, "\\"", ":", ''' + str(nrow+1) + ''' - turnover_rate_rank, " ,"
                          "\\"", sharp_ratio_label, "\\"", ":", ''' + str(nrow+1) + ''' - sharp_ratio_rank, " ,"
                          "\\"", profit_volatility_year_label, "\\"", ":", ''' + str(nrow+1) + ''' - profit_volatility_year_rank, " ,"
                          "\\"", unit_risk_premium_rate_label, "\\"", ":", ''' + str(nrow+1) + ''' - unit_risk_premium_rate_rank, " ,"
                          "\\"", stock_selection_ability_label, "\\"", ":", ''' + str(nrow+1) + ''' - stock_selection_ability_rank, " ,"
                          "\\"", percep_in_level, "\\"", ":", ''' + str(nrow+1) + ''' - percep_in_rank, " ,"
                          "\\"", percep_out_level, "\\"", ":", ''' + str(nrow+1) + ''' - percep_out_rank, " ,"
                          "\\"", stock_corr_level, "\\"", ":", ''' + str(nrow+1) + ''' - stock_corr_rank, " ,"
                          "\\"", margin_corr_level, "\\"", ":", ''' + str(nrow+1) + ''' - margin_corr_rank, " ,"
                          "\\"", hold_risk_level, "\\"", ":", ''' + str(nrow+1) + ''' - hold_risk_rank, " ,"
                          "\\"", contri_level, "\\"", ":", ''' + str(nrow+1) + ''' - contri_value_rank, " ,"
                          "\\"", contri_stab_level, "\\"", ":", ''' + str(nrow+1) + ''' - contri_stab_rank, " ,"
                          "\\"", up_level, "\\"", ":", ''' + str(nrow+1) + ''' - up_rate_rank, " ,"
                          "\\"", cycle_stable_level, "\\"", ":", ''' + str(nrow+1) + ''' - cycle_stable_rank, " ,"
                          "\\"", control_loss_level, "\\"", ":", ''' + str(nrow+1) + ''' - control_loss_rank, " ,"
                          "\\"", age_tag, "\\"", ":", 50, " ,"
                          "\\"", profession, "\\"", ":", 60, " ,"
                          "\\"", degree_code, "\\"", ":", 70,
                          "}"
                          ) as wordcloud
            from alltags
        ''')
        wordcloud_today.registerTempTable("wordcloud_today")
        
        # 插入分区
        hqlContext.sql('''
            insert overwrite table tag_stat.wordcloud
            partition(part_date=''' + today + ''')
            select customer_no, wordcloud
            from wordcloud_today
        ''')
    
    
    sc.stop()
