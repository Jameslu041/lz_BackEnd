# coding:utf-8

"""
input: fact_cust_stock_detail, agg_cust_stock, dim_stock_industry, dim_customer, fact_stock_market_detail
output: tag_stat.market_perception
"""

from __future__ import division  # 使得整数相除时结果为小数
from pyspark import SparkContext, SparkConf, HiveContext
import pandas as pd
import datetime
import time
from datetime import timedelta
import sys
    
    
#传入日期，与当前日期的时间差，返回新日期
def compute_date(raw_date,diff):
    format_date = datetime.datetime.strptime(raw_date,'%Y%m%d').date()
    delta = timedelta(days=diff)  #计算时间差
    new_date = (format_date+delta).strftime("%Y%m%d")
    return new_date
    
# 账户分析
if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext()
    hqlContext = HiveContext(sc)
    
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

    half_year_ago = compute_date(run_date, -180) #半年前的日期

    # 本地测试
    # run_date = '20170214'
    # half_year_ago = '20170114'
    
    # 盘面感知
    pmgz_detail = hqlContext.sql('''
        select a.customer_no as customer_no,
               a.stock_no as stock_no,
               a.business_price as business_price,
               a.entrust_bs as entrust_bs,
               b.close_price as close_price
        from (select customer_no,
                      stock_no,
                      business_price,
                      init_date,
                      entrust_bs
              from ''' + db_name + '''.fact_cust_stock_detail
              where part_date >= ''' + half_year_ago + ''' and 
                    part_date <= ''' + run_date + ''' and 
                    business_type='证券买卖'
              ) a
        left outer join 
            (select stock_no,
                    close_price,
                    init_date
            from ''' + db_name + '''.fact_stock_market_detail
            where init_date >= ''' + half_year_ago + ''' and
                  init_date <= ''' + run_date + '''
            ) b
        on a.stock_no = b.stock_no and
           a.init_date = b.init_date
    ''')
    pmgz_detail.registerTempTable('pmgz_detail_table')
    hqlContext.cacheTable('pmgz_detail_table')
    
    # 买入盘面感知（只计算了有买入交易的客户的盘面感知，没有买入交易的客户的盘面感知应为0，下同）
    market_perception_bought = hqlContext.sql('''
        select customer_no,
               count(case when business_price < close_price then 1 else null end)/count(customer_no) as percep_in
        from pmgz_detail_table
        where entrust_bs = '买入'
        group by customer_no
    ''')
    market_perception_bought.registerTempTable('market_perception_bought_table')
    
    # 卖出盘面感知
    market_perception_sold = hqlContext.sql('''
        select customer_no,
               count(case when business_price > close_price then 1 else null end)/count(customer_no) as percep_out
        from pmgz_detail_table
        where entrust_bs = '卖出'
        group by customer_no
    ''')
    market_perception_sold.registerTempTable('market_perception_sold_table')
    
    #日期边界2
    date_bound2 = hqlContext.sql('''
        select max(part_date) as max_date,
               min(part_date) as min_date
        from ''' + db_name + '''.agg_cust_stock
        where part_date >= ''' + half_year_ago + ''' and
              part_date <= ''' + run_date + '''
    ''').collect()
    last_day2 = date_bound2[0][0]
    first_day2 = date_bound2[0][1]

    # 期末股票资产
    close_stock = hqlContext.sql('''
        select customer_no,
               stock_no,
               current_amount as stock_value
        from ''' + db_name + '''.agg_cust_stock
        where part_date = ''' + last_day2 + '''
    ''')
    close_stock.registerTempTable('close_stock_table')
    
    # 期初股票资产
    begin_stock = hqlContext.sql('''
        select customer_no,
               stock_no,
               -current_amount as stock_value
        from ''' + db_name + '''.agg_cust_stock
        where part_date = ''' + first_day2+ '''
    ''')
    begin_stock.registerTempTable('begin_stock_table')
    
    # 期间买入股票值
    buy_stock = hqlContext.sql('''
        select customer_no,
               stock_no,
               -sum(business_balance) as stock_value
        from ''' + db_name + '''.fact_cust_stock_detail
        where entrust_bs = '买入' and 
              part_date > ''' + half_year_ago + ''' and 
              part_date <= ''' + run_date + '''
        group by customer_no,
                 stock_no
    ''')
    buy_stock.registerTempTable('buy_stock_table')
    
    # 期间卖出股票值
    sell_stock = hqlContext.sql('''
        select customer_no,
               stock_no,
               sum(business_balance) as stock_value
        from ''' + db_name + '''.fact_cust_stock_detail
        where entrust_bs = '卖出' and 
              part_date > ''' + half_year_ago + ''' and 
              part_date <= ''' + run_date + '''
        group by customer_no,
                 stock_no
    ''')
    sell_stock.registerTempTable('sell_stock_table')
    
    # 客户行业的盈亏明细
    industry_profits_loss_detail = hqlContext.sql('''
        select e.customer_no as customer_no,
               f.industry as industry,
               sum(e.profits) as profits
        from 
        (
        select a.customer_no as customer_no,
               a.stock_no as stock_no,
               sum(a.stock_value) as profits
        from (select *
              from close_stock_table
              union all
              select *
              from begin_stock_table
              union all
              select *
              from buy_stock_table
              union all
              select *
              from sell_stock_table) a
        group by a.customer_no,
                 a.stock_no
        ) e
        left outer join 
        (select stock_no,
                industry
        from ''' + db_name + '''.dim_stock_industry 
        ) f
        on e.stock_no = f.stock_no
        group by e.customer_no,
                 f.industry
    ''')
    industry_profits_loss_detail.registerTempTable('industry_profits_loss_detail_table')
    
    # 行业盈利分布
    industry_profit = hqlContext.sql('''
        select a.customer_no as customer_no,
               concat_ws(',', 
                         collect_list(concat(nvl(industry, ""), 
                                             ":", 
                                             profits_perct
                                      )
                         )
               ) as industry_profit
        from 
        (
            select customer_no,
                   industry,
                   profits/(sum(profits) over(partition by customer_no)) as profits_perct
            from industry_profits_loss_detail_table
            where profits >= 0.0
        ) a
        group by a.customer_no
    ''')
    industry_profit.registerTempTable('industry_profit_table')
    
    # 行业亏损分布
    industry_loss = hqlContext.sql('''
        select a.customer_no as customer_no,
               concat_ws(',', 
                         collect_list(concat(nvl(industry, ""), 
                                             ":", 
                                             loss_perct
                                      )
                         )
               ) as industry_loss
        from 
        (
            select customer_no,
                   industry,
                   profits/(sum(profits) over(partition by customer_no)) as loss_perct
            from industry_profits_loss_detail_table
            where profits < 0.0
        ) a
        group by a.customer_no
    ''')
    industry_loss.registerTempTable('industry_loss_table')
    
    
    
    
    # 整合指标+标签
    res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               b.percep_in as percep_in,
               (case when b.percep_in is null then '买入感知未知'
                     when b.percep_in < 0.3 then '弱买入感知'
                     when b.percep_in > 0.7 then '强买入感知'
                     else '中等买入感知' 
                end) as percep_in_level,
               c.percep_out as percep_out,
               (case when c.percep_out is null then '卖出感知未知'
                     when c.percep_out < 0.3 then '弱卖出感知'
                     when c.percep_out > 0.7 then '强卖出感知'
                     else '中等卖出感知' 
                end) as percep_out_level,
               nvl(d.industry_profit, '') as industry_profit,
               nvl(e.industry_loss, '') as industry_loss
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join market_perception_bought_table b
        on cust.customer_no=b.customer_no
        left outer join market_perception_sold_table c
        on cust.customer_no=c.customer_no
        left outer join industry_profit_table d
        on cust.customer_no=d.customer_no
        left outer join industry_loss_table e
        on cust.customer_no=e.customer_no
    ''')
    
    # 标签缺失补全
    res_all = res_all.na.fill({'percep_in_level': u'买入感知未知', 
                               'percep_out_level': u'卖出感知未知',
                               'industry_profit': '',
                               'industry_loss': ''})
    res_all.registerTempTable("res_all")
    
    
    # 删除可能存在的表
    hqlContext.sql('drop table if exists tag_stat.market_perception')
    hqlContext.sql('''
        create table tag_stat.market_perception as 
        select * from res_all
    ''')

    sc.stop()
