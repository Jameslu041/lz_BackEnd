# coding:utf-8

"""
input_table: agg_cust_balance; dim_customer;
output_table: account_analysis
"""
from __future__ import division  # 使得整数相除时结果为小数
import datetime
import sys
import time
from datetime import timedelta
import numpy as np
import pandas as pd
from pyspark import SparkContext, SparkConf, HiveContext

# 传入日期，与当前日期的时间差，返回新日期
def compute_date(raw_date, diff):
    format_date = datetime.datetime.strptime(raw_date, '%Y%m%d').date()
    delta = timedelta(days=diff)
    new_date = (format_date + delta).strftime("%Y%m%d")
    return new_date

# 账户分析
if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext()
    hqlContext = HiveContext(sc)

    args = sys.argv[1:]  # 接受参数，目前只接数据库名, run_date两个参数，数据库名参数必须输入

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
    
    # 本地测试时间
    # run_date = '20170130'
    half_year_ago = compute_date(run_date, -180)  # 6个月前的日期
    
    
    # 年化收益波动率
    profit_volatility_year = hqlContext.sql('''
        select n.customer_no as customer_no,
               round(n.nsybdl, 4) as nsybdl,
               (case when n.nsybdl is null then '近半年无收益波动'
                     when n.nsybdl >= percentile_approx(n.nsybdl, 0.75) over() then '年化收益波动率高'
                     when n.nsybdl <= percentile_approx(n.nsybdl, 0.25) over() then '年化收益波动率低'
                     else '年化收益波动率中等'
                end) as tag1
        from (
              select b.customer_no, 
                     stddev(nvl((b.qcqy-b.lqcqy)/b.lqcqy,0.0))*sqrt(252) as nsybdl 
              from (
                    select a.customer_no as customer_no, 
                           a.init_date as init_date, 
                           a.qcqy as qcqy,
                           lag(a.qcqy, 1, a.qcqy) over(partition by a.customer_no order by a.init_date) as lqcqy 
                    from (
                          select customer_no, 
                                 init_date, 
                                 total_close_asset-total_close_debit as qcqy 
                          from ''' + db_name + '''.agg_cust_balance 
                          where part_date >= ''' + half_year_ago + ''' and 
                                part_date <= ''' + run_date + '''
                          order by customer_no, init_date
                         ) a
                   ) b 
              where b.qcqy != 0 and
                    b.lqcqy != 0
              group by b.customer_no
             ) n
    ''')
    profit_volatility_year.registerTempTable('profit_volatility_year_table')
    
    # 夏普指数
    sharp_ratio = hqlContext.sql('''
        select n.customer_no as customer_no,
               round(n.xiapu, 4) as xiapu,
               (case when n.xiapu is null then '夏普指数未知'
                     when abs(n.xiapu) >= percentile_approx(abs(n.xiapu), 0.75) over() then '夏普指数高'
                     when abs(n.xiapu) <= percentile_approx(abs(n.xiapu), 0.25) over() then '夏普指数低'
                     else '夏普指数一般'
                end) as tag2
        from (
              select c.customer_no,
                     nvl((avg(c.zzl)-0.04)/stddev(c.zzl), 0.0) as xiapu
              from (
                    select b.customer_no,
                           b.init_date,
                           (b.total_close_money-b.lqcqy)/b.lqcqy as zzl
                    from (
                          select a.customer_no, 
                                 a.init_date, 
                                 a.total_close_money, 
                                 lag(a.total_close_money, 1) over(partition by a.customer_no order by a.init_date) as lqcqy  
                          from (
                                select customer_no, 
                                       init_date, 
                                       total_close_asset-total_close_debit as total_close_money
                                from ''' + db_name + '''.agg_cust_balance 
                                where part_date >= ''' + half_year_ago + ''' and 
                                      part_date <= ''' + run_date + '''
                                order by customer_no, init_date
                               ) a
                          ) b
                    where b.lqcqy is not null and
                          b.total_close_money != 0 and
                          b.lqcqy != 0
                   ) c
              group by c.customer_no
             ) n
    ''')
    sharp_ratio.registerTempTable('sharp_ratio_table')

    # 平均换手率
    turnover_rate = hqlContext.sql('''
        select n.customer_no as customer_no,
               round(n.exchangep, 4) as exchangep,
               (case when n.exchangep is null then '近半年交易行为不足计算'
                     when n.exchangep >= percentile_approx(n.exchangep, 0.75) over() then '平均换手率高'
                     when n.exchangep <= percentile_approx(n.exchangep, 0.25) over() then '平均换手率低'
                     else '平均换手率一般'
                end) as tag3
        from (
              select customer_no,
                     avg(exchange_amount/(total_begin_asset-total_begin_debit+stock_in)) as exchangep 
              from ''' + db_name + '''.agg_cust_balance 
              where part_date >= ''' + half_year_ago + ''' and 
                    part_date <= ''' + run_date + ''' and
                    total_begin_asset-total_begin_debit+stock_in != 0
              group by customer_no
             ) n
    ''')
    turnover_rate.registerTempTable('turnover_rate_table')

    # 单位风险报酬率
    unit_risk_premium_rate = hqlContext.sql('''
        select n.customer_no as customer_no,
               round(n.dwfxbcl, 4) as dwfxbcl,
               (case when n.dwfxbcl is null then '单位风险报酬率未知'
                     when n.dwfxbcl >= percentile_approx(n.dwfxbcl, 0.75) over() then '单位风险报酬率高'
                     when n.dwfxbcl <= percentile_approx(n.dwfxbcl, 0.25) over() then '单位风险报酬率低'
                     else '单位风险报酬率中等'
                end) as tag4
        from (
              select d.customer_no,
                     nvl(avg((d.f-d.l)/d.l)/stddev(d.zcjz),0.0) as dwfxbcl
              from (
                    select c.customer_no,
                           c.init_date,
                           c.zcjz,
                           first_value(c.zcjz) over(partition by c.customer_no order by c.init_date) as l,
                           first_value(c.zcjz) over(partition by c.customer_no order by c.init_date desc) as f
                    from (
                          select customer_no, 
                                 init_date, 
                                 total_close_asset-total_close_debit as zcjz
                          from ''' + db_name + '''.agg_cust_balance 
                          where part_date >= ''' + half_year_ago + ''' and 
                                part_date <= ''' + run_date + '''
                          order by customer_no, init_date
                         ) c
                   ) d
              group by d.customer_no
             ) n
    ''')
    unit_risk_premium_rate.registerTempTable('unit_risk_premium_rate_table')
    
    
    
    # 整合指标+标签
    res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               (case when a.nsybdl is not null then a.nsybdl
                     else 0
                end) as profit_volatility_year_value,
               (case when a.tag1 is not null then a.tag1
                     else '近半年无收益波动'
                end) as profit_volatility_year_label,
               rank() over (order by a.nsybdl desc) as profit_volatility_year_level,
               b.xiapu as sharp_ratio_value,
               (case when b.tag2 is not null then b.tag2
                     else '夏普指数未知'
                end) as sharp_ratio_label,
               rank() over (order by abs(b.xiapu) desc) as sharp_ratio_level,
               (case when c.exchangep is not null then c.exchangep
                     else 0
                end) as turnover_rate_value,
               (case when c.tag3 is not null then c.tag3
                     else '近半年交易行为不足计算'
                end) as turnover_rate_label,
               rank() over (order by c.exchangep desc) as turnover_rate_level,
               d.dwfxbcl as unit_risk_premium_rate_value,
               (case when d.tag4 is not null then d.tag4
                     else '单位风险报酬率未知'
                end) as unit_risk_premium_rate_label,
               rank() over (order by d.dwfxbcl desc) as unit_risk_premium_rate_level
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join profit_volatility_year_table a
        on cust.customer_no = a.customer_no
        left outer join sharp_ratio_table b
        on cust.customer_no=b.customer_no
        left outer join turnover_rate_table c
        on cust.customer_no=c.customer_no
        left outer join unit_risk_premium_rate_table d
        on cust.customer_no=d.customer_no
        order by customer_no
    ''')
    
    # 标签缺失补全
    res_all = res_all.na.fill({'profit_volatility_year_label': u'近半年无收益波动', 
                               'sharp_ratio_label': u'夏普指数未知',
                               'turnover_rate_label': u'近半年交易行为不足计算',
                               'unit_risk_premium_rate_label': u'单位风险报酬率未知',
                               'profit_volatility_year_value': 0,
                               'turnover_rate_value': 0 })
    res_all.registerTempTable("res_all")
    
    
    # 删除可能存在的表
    hqlContext.sql('drop table if exists tag_stat.account_analysis')
    hqlContext.sql('''
    create table tag_stat.account_analysis as 
    select * from res_all
    ''')

    sc.stop()
