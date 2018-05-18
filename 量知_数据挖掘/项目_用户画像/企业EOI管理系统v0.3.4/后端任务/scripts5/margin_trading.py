
# coding: utf-8


#!/usr/bin/env python
"""
input: agg_cust_balance, agg_cust_statistics_info, Fact_Cust_Fund_Detail, Fact_Cust_Stock_Detail, dim_customer, Fact_Stock_Market_Summary, fact_fin_slo_market_summary
output: margin_trading
"""

# Li Mongmong
# 2017.08.08
# Gu Quan
# 2017.12.13
# Gu Quan
# 2018.02.02
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
    sc = SparkContext()
    hqlContext = HiveContext(sc)
    
    db_name = ['ctprod','tag_stat']
    
    args = sys.argv[1:]
    if len(args) == 0:
        run_date = compute_date(time.strftime("%Y%m%d"), -1)
    else:
        run_date = args[0]
    
    six_month_ago = compute_date(run_date,-180) #6个月前的日期
    six_month_days = 180 #6个月的时间间隔
    year = time.strftime("%Y")
    firstdayofyear = year+"0101"
    twenty_days_ago = compute_date(run_date,-20)  # for turnover_rate

    
        
    #盈利额
    total_profit = hqlContext.sql('''
        select a.customer_no,
               sum(a.total_profit) as total_profit
        from(select customer_no, 
                    total_profit 
             from '''+db_name[0]+'''.agg_cust_balance 
             where part_date>='''+firstdayofyear+''' and
                   part_date<='''+run_date+''') a
        group by customer_no
    ''')
    total_profit.registerTempTable('total_profit_temp')

    is_profit = hqlContext.sql('''
        select customer_no,
               total_profit,
               (case when total_profit is null then '盈利未知'
                     when total_profit>150000 then '高盈利'
                     when total_profit>30000 then '中等盈利'
                     when total_profit>0 then '低盈利'
                     when total_profit>-30000 then '低亏损'
                     when total_profit>-150000 then '中等亏损'
                     else '高亏损' end) as total_profit_level,
               (case when total_profit>0 then 1 
                     when total_profit<0 then -1 
                     else 0 end) as isprofit
        from total_profit_temp
    ''')
    is_profit.registerTempTable('profit_df_temp')
        
                
            
    #资产周转率
    turnover_rate_df = hqlContext.sql('''
        select customer_no,
               nvl(turnover_rate_year,0) as turnover_rate_year
        from '''+db_name[0]+'''.agg_cust_statistics_info
        where part_date='''+run_date+''' and
              customer_no is not NULL
    ''')
    #临时表
    turnover_rate_df.registerTempTable('turnover_rate_temp')


    #融资频率
    fin_freq_df = hqlContext.sql('''
        select m.customer_no,
               count(m.init_date)/180 as margin_trading_freq 
        from (select customer_no,
                     init_date 
              from '''+db_name[0]+'''.Fact_Cust_Fund_Detail 
              where exchange_type='融资借款' and 
                    part_date>'''+six_month_ago+''' and 
                    part_date<='''+run_date+''') m 
        group by m.customer_no
    ''')
    fin_freq_df.registerTempTable('fin_freq_temp')
        
        
        
    #客户贡献额
    cust_contribution_df = hqlContext.sql('''
        select B.customer_no as customer_no,
               (nvl(A.fare_year,0) + nvl(B.interest_year,0)) as cust_contribution 
        from (
              select customer_no,
                     sum(fare0) as fare_year 
              from '''+db_name[0]+'''.Fact_Cust_Stock_Detail 
              where (exchange_type='融资买入' or exchange_type='融券卖出') and 
                    part_date >= '''+firstdayofyear+''' and
                    part_date <='''+run_date+'''
              group by customer_no
             ) A 
        full outer join 
             (select customer_no,
                     interest_year
              from '''+db_name[0]+'''.agg_cust_statistics_info 
              where part_date='''+run_date+''' and
                    customer_no is not NULL
             ) B 
        on A.customer_no = B.customer_no
       ''')
    #临时表
    cust_contribution_df.registerTempTable('cust_contribution_temp')


    #客户贡献率
    S = hqlContext.sql('''
        select sum(cust_contribution) as sum_devo
        from cust_contribution_temp
    ''')
    S.registerTempTable('S_temp')

    cust_contri_rate = hqlContext.sql('''
        select a.customer_no,
               a.cust_contribution/b.sum_devo as devo_ratio
        from cust_contribution_temp a,
             S_temp b
    ''')
    #临时表
    cust_contri_rate.registerTempTable('cust_contri_rate_temp')
    cust_contri_rate = hqlContext.sql('''
        select *,
               (case when devo_ratio is null then '客户贡献未知'
                     when devo_ratio>6.872E-4 then '客户贡献高'
                     when devo_ratio>1.541E-4 then '客户贡献中高'
                     when devo_ratio>3.755E-5 then '客户贡献中'
                     else '客户贡献低' 
               end) as cust_devo_level
        from cust_contri_rate_temp
    ''')
    cust_contri_rate.registerTempTable('cust_contri_rate_temp')
        
        
        
    #当日维持担保比例
    guarantee_rate_df = hqlContext.sql('''
        select customer_no,
               close_rate,
               (case when close_rate is null then '维保比例未知'
                     when close_rate<=130 then '强制平仓'
                     when close_rate<=150 then '潜在平仓风险'
                     else '维保比例正常' 
                end) as close_rate_level
        from(select customer_no,
                    nvl(close_rate,0) as close_rate
             from '''+db_name[0]+'''.agg_cust_balance 
             where part_date='''+run_date+''' and
                   customer_no is not NULL) a
    ''')
    #临时表
    guarantee_rate_df.registerTempTable('guarantee_rate_temp')


    #保证金可用余额
    crdt_balance_df = hqlContext.sql('''
        select *,
               (case when crdt_close_balance is null then '保证金可用余额未知'
                     when crdt_close_balance>300000 then '保证金可用余额高'
                     when crdt_close_balance>30000 then '保证金可用余额中'
                     else '保证金可用余额低' 
               end) as crdt_close_balance_level
        from(select customer_no,
                    nvl(crdt_close_balance,0) as crdt_close_balance
             from '''+db_name[0]+'''.agg_cust_balance 
             where part_date='''+run_date+''' and
                   customer_no is not NULL) a
    ''')
    #临时表
    crdt_balance_df.registerTempTable('crdt_balance_temp')
        
        
        
    #持仓比例
    stock_time_df = hqlContext.sql('''
        select customer_no,
               corr(c.stock_close_value,c.close) as stock_correlation
        from (select a.init_date,
                     a.customer_no,
                     nvl(a.stock_close_value,0) as stock_close_value,
                     nvl(b.close,0) as close
              from '''+db_name[0]+'''.agg_cust_balance a
              join '''+db_name[0]+'''.Fact_Stock_Market_Summary b
              on a.init_date = b.init_date
              where a.part_date>'''+six_month_ago+''' and
                    a.part_date<='''+run_date+''' and
                    b.market_name='上证指数') c
        group by c.customer_no
    ''')
    #临时表
    stock_time_df.registerTempTable('stock_time_temp')

    stock_time_l = hqlContext.sql('''
        select customer_no,
               (case when stock_correlation is null then '指数相似程度未知'
                     when stock_correlation>=0.63 then '指数相似程度较高'
                     when stock_correlation>=0.35 then '指数相似程度一般'
                     when stock_correlation>=-0.02 then '指数相似程度较低'
                     else '指数不相似' 
               end) as stock_corr
        from stock_time_temp
    ''')
    stock_time_l.registerTempTable('stock_time_l_temp')
        
        
        
    #融资余额相似性
    fin_balance_df = hqlContext.sql('''    
        select customer_no,
               corr(c.fin_close_balance,c.fin_balance) as correlation
        from 
            (select a.init_date,
                    a.customer_no,
                    nvl(a.fin_close_balance,0) as fin_close_balance,
                    nvl(b.fin_balance,0) as fin_balance
             from '''+db_name[0]+'''.agg_cust_balance a
             join '''+db_name[0]+'''.fact_fin_slo_market_summary b
             on a.init_date = b.init_date
             where a.part_date>'''+six_month_ago+''') c
        group by c.customer_no
    ''')
    #临时表
    fin_balance_df.registerTempTable('fin_balance_corr_temp')

    balance_l = hqlContext.sql('''
        select customer_no,
               (case when correlation is null then '市场敏感度未知'
                     when correlation>=0.15 then '市场敏感度较高'
                     when correlation >=0.06  then '市场敏感度一般'
                     when correlation >=-0.047 then '市场敏感度较低'
                     else '市场不敏感' 
                end) as margin_corr
        from fin_balance_corr_temp
    ''')
    balance_l.registerTempTable('balance_l_temp')
        
        
        
    #交易活跃度
    # detail_rank = hqlContext.sql('''
        # select distinct customer_no,
               # den_rank 
        # from (select customer_no,
                     # den_rank,
                     # dense_rank() over(partition by customer_no order by den_rank desc) as rn
              # from (select customer_no,
                           # init_date,
                           # dense_rank() over(partition by customer_no order by init_date asc) as den_rank
                    # from '''+db_name[0]+'''.Fact_Cust_Fund_Detail 
                    # where part_date>'''+six_month_ago+''' and 
                          # part_date<='''+run_date+''') m
             # ) n
        # where n.rn<=1
    # ''')
    # 每个客户最近半年的总交易天数
    cust_hy_days = hqlContext.sql('''
        select customer_no,
               count(distinct init_date) as cust_hy
        from '''+db_name[0]+'''.Fact_Cust_Fund_Detail 
        where part_date>'''+six_month_ago+''' 
          and part_date<='''+run_date+'''
        group by customer_no
    ''')
    cust_hy_days.registerTempTable('cust_hy_days_table')
    hqlContext.cacheTable('cust_hy_days_table')
    
    activity_df = hqlContext.sql('''
        select zczzl.customer_no,
               0.5*nvl(zczzl.turnover_rate_year,0) + 0.2*nvl(hyts.hy,0) + 0.3*nvl(rzpl.fin_frequency,0) as cust_activity
        from (select t1.customer_no,
                     (t1.cust_hy-t2.min_hy)/(t2.max_hy-t2.max_hy) as hy
              from cust_hy_days_table t1,
                    (select min(cust_hy) as min_hy,
                            max(cust_hy) as max_hy
                     from cust_hy_days_table) t2
             ) hyts
        full outer join 
             (select c1.customer_no,
                     (c1.turnover_rate_year-c2.min_try)/(c2.max_try-c2.min_try) as turnover_rate_year
              from (select customer_no,
                           nvl(turnover_rate_year,0.0) as turnover_rate_year
                    from '''+db_name[0]+'''.agg_cust_statistics_info
                    where part_date='''+run_date+''' and
                          customer_no is not NULL
                   ) c1,
                   (select max(latest.turnover_rate_year) as max_try,
                           min(latest.turnover_rate_year) as min_try
                    from(select customer_no,
                                turnover_rate_year
                         from '''+db_name[0]+'''.agg_cust_statistics_info
                         where part_date='''+run_date+''' and
                               customer_no is not NULL
                        ) latest
                   ) c2
             ) zczzl
        on hyts.customer_no=zczzl.customer_no
        full outer join 
             (select b1.customer_no,
                     (b1.trade_fre-min_tra)/(max_tra-min_tra) as fin_frequency
              from (select customer_no,
                           trade_times/180 as trade_fre
                    from (select customer_no,
                                 trade_times,
                          row_number() over(partition by customer_no order by trade_times desc) as rn
                          from (select customer_no,
                                       row_number() over(partition by customer_no order by init_date asc) as trade_times
                                from '''+db_name[0]+'''.Fact_Cust_Fund_Detail 
                                where exchange_type='融资借款' and 
                                      init_date>'''+six_month_ago+''' and 
                                      init_date<='''+run_date+''') m) n 
                    where rn<=1) b1,
                  (select max(trade_fre) as max_tra,
                          min(trade_fre) as min_tra
                   from (select customer_no,
                                trade_times/180 as trade_fre
                         from (select customer_no,
                                      trade_times,
                                      row_number() over(partition by customer_no order by trade_times desc) as rn
                               from (select customer_no,
                                            row_number() over(partition by customer_no order by init_date asc) as trade_times
                                     from '''+db_name[0]+'''.Fact_Cust_Fund_Detail 
                                     where exchange_type='融资借款' and 
                                           init_date>'''+six_month_ago+''' and 
                                           init_date<='''+run_date+''') m) n 
                         where rn<=1) b
                  ) b2
             ) rzpl
        on hyts.customer_no = rzpl.customer_no
    ''')
    #临时表
    activity_df.registerTempTable('activity_temp')

    activity_labeled = hqlContext.sql('''
        select customer_no,
               (case when cust_activity is null then '活跃度未知'
                     when cust_activity<=0.03 then '趋近休眠'
                     when cust_activity<=0.1 then '低活跃度'
                     when cust_activity<=0.3 then '中活跃度'
                     else '高活跃度' 
               end) as activity
        from activity_temp
    ''')
    activity_labeled.registerTempTable('activity_l_temp')
        
    
    #指标
    fact = hqlContext.sql('''
        select t1.customer_no,
               t1.total_profit,
               t1.isprofit,
               t2.turnover_rate_year,
               t3.margin_trading_freq as margin_freq,
               t4.cust_contribution,
               t5.devo_ratio,
               t6.close_rate,
               t7.crdt_close_balance,
               t8.cust_activity,
               t9.correlation,
               t10.stock_correlation
        from profit_df_temp t1
        full outer join turnover_rate_temp t2
        on t1.customer_no = t2.customer_no
        full outer join fin_freq_temp t3
        on t1.customer_no = t3.customer_no    
        full outer join cust_contribution_temp t4
        on t1.customer_no = t4.customer_no
        full outer join cust_contri_rate_temp t5
        on t1.customer_no = t5.customer_no
        full outer join guarantee_rate_temp t6
        on t1.customer_no = t6.customer_no
        full outer join crdt_balance_temp t7
        on t1.customer_no = t7.customer_no
        full outer join activity_temp t8
        on t1.customer_no = t8.customer_no
        full outer join fin_balance_corr_temp t9
        on t1.customer_no = t9.customer_no
        full outer join stock_time_temp t10
        on t1.customer_no = t10.customer_no
    ''')
    fact.registerTempTable('fact_temp')
    

    #标签
    model = hqlContext.sql('''
        select t1.customer_no,
               t1.stock_corr,
               t2.margin_corr,
               t3.activity,
               t4.cust_devo_level,
               t5.total_profit_level,
               t6.close_rate_level,
               t7.crdt_close_balance_level
        from stock_time_l_temp t1
        full outer join balance_l_temp t2
        on t1.customer_no = t2.customer_no
        full outer join activity_l_temp t3
        on t1.customer_no = t3.customer_no
        full outer join cust_contri_rate_temp t4
        on t1.customer_no = t4.customer_no
        full outer join profit_df_temp t5
        on t1.customer_no = t5.customer_no
        full outer join guarantee_rate_temp t6
        on t1.customer_no = t6.customer_no
        full outer join crdt_balance_temp t7
        on t1.customer_no = t7.customer_no
    ''')
    model.registerTempTable('model_temp')
    
    
    # 整合指标+标签
    res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               a.total_profit as total_profit,
               a.isprofit as isprofit,
               a.turnover_rate_year as turnover_rate_year,
               a.margin_freq as margin_freq,
               a.cust_contribution as profit_devo,
               a.devo_ratio as devo_ratio,
               a.close_rate as close_rate,
               a.crdt_close_balance as crdt_close_balance,
               a.cust_activity as cust_activity,
               a.correlation as fin_corr,
               a.stock_correlation as stock_correlation,
               b.stock_corr as stock_corr,
               b.margin_corr as margin_corr,
               b.activity as activity,
               b.cust_devo_level,
               b.total_profit_level,
               b.close_rate_level,
               b.crdt_close_balance_level
        from (select distinct customer_no 
              from '''+db_name[0]+'''.dim_customer) cust
        left outer join fact_temp a
        on cust.customer_no = a.customer_no
        left outer join model_temp b
        on cust.customer_no = b.customer_no
    ''')
    
    # 标签缺失补全
    res_all = res_all.na.fill({'total_profit_level': u'盈利未知', 
                               'cust_devo_level': u'客户贡献未知',
                               'close_rate_level': u'维保比例未知',
                               'crdt_close_balance_level': u'保证金可用余额未知',
                               'stock_corr': u'指数相似程度未知',
                               'margin_corr': u'市场敏感度未知',
                               'activity': u'活跃度未知'})
    res_all.registerTempTable("res_all")
    
    
    
    hqlContext.sql('''drop table if exists '''+db_name[1]+'''.margin_trading''')
    hqlContext.sql('''
        create table '''+db_name[1]+'''.margin_trading as 
        select * from res_all
    ''')
    
    hqlContext.uncacheTable('cust_hy_days_table')
    
    sc.stop()

