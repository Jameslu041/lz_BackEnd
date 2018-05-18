# coding:utf-8
"""
input: fact_cust_stock_detail, dim_stock
output:

"""
from __future__ import division  # 使得整数相除时结果为小数
from pyspark import SparkContext, SparkConf, HiveContext
import datetime
import sys
import time
from datetime import timedelta


# 传入日期，与当前日期的时间差，返回新日期
def compute_date(raw_date, diff):
    format_date = datetime.datetime.strptime(raw_date, '%Y%m%d').date()
    delta = timedelta(days=diff)
    new_date = (format_date + delta).strftime("%Y%m%d")
    return new_date


# 两个参数：日期、数据库名，若没有传入参数则全部使用默认
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

    six_months_ago = compute_date(run_date, -180)  # 6个月前的日期
    
    
    # 偏好行业明细
    industry_prefer_detail = hqlContext.sql('''
        select c.customer_no as customer_no,
               c.stock_type as stock_type,
               row_number() over (partition by c.customer_no order by c.total_exchange_amount) as rank
        from
        (
            select a.customer_no as customer_no,
                   b.industry as stock_type,
                   sum(a.business_balance) as total_exchange_amount
            from ''' + db_name + '''.fact_cust_stock_detail a
            left outer join ''' + db_name + '''.dim_stock b
            on a.stock_no=b.stock_no
            where a.part_date>=''' + six_months_ago + '''
              and a.part_date<=''' + run_date + '''
              and a.entrust_bs='买入'
            group by a.customer_no,
                     b.industry
        ) c
    ''')
    industry_prefer_detail.registerTempTable('industry_prefer_detail_table')  # 注册成临时表
    hqlContext.cacheTable('industry_prefer_detail_table')

    # 获取前N个偏好行业
    industry_prefer_topN = hqlContext.sql('''
        select customer_no,
               concat_ws(',', collect_list(stock_type)) as stock_industry_n
        from industry_prefer_detail_table
        where rank<=3
        group by customer_no
    ''')
    industry_prefer_topN.registerTempTable('industry_prefer_topN_table')  # 注册成临时表

    # 持仓比例，近6个月平均持仓比例
    stock_hold_ratio = hqlContext.sql('''   
        select a.customer_no as customer_no,
               round(sum(nvl(a.total_stock/a.total_asset, 0.0)) / count(distinct a.init_date), 4) as stock_hold_ratio
        from 
        (
        select init_date,
               customer_no,
               sum(nvl(total_close_asset, 0.0)) as total_asset,
               sum(nvl(stock_close_value, 0.0)) as total_stock
        from ''' + db_name + '''.agg_cust_balance
        where part_date>=''' + six_months_ago + '''
          and part_date<=''' + run_date + '''
        group by init_date,
                 customer_no
        ) a
        group by a.customer_no
    ''')
    stock_hold_ratio.registerTempTable('stock_hold_ratio_table')  # 注册成临时表

    # 开户年限
    open_years = hqlContext.sql('''
        select customer_no,
               round(nvl(open_days, 0.0)/365,2) as open_years
        from ''' + db_name + '''.agg_cust_statistics_info
        where part_date=''' + run_date)
    open_years.registerTempTable('open_years_table')  # 注册成临时表

    # 持股集中度
    stock_concentrate = hqlContext.sql('''
        select customer_no,
               max(nvl(stock_concentrate, 0.0)) as stock_concentrate
        from ''' + db_name + '''.agg_cust_stock
        where part_date=''' + run_date + '''
        group by customer_no
    ''')
    stock_concentrate.registerTempTable('stock_concentrate_table')  # 注册成临时表
    
    
    
    # 整合指标+标签
    res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               a.stock_hold_ratio as stock_hold_ratio,
               (case when a.stock_hold_ratio is null then '持仓水平未知'
                     when a.stock_hold_ratio>=0.75 then '持仓水平高'
                     when a.stock_hold_ratio>=0.6 then '持仓水平中高'
                     when a.stock_hold_ratio>=0.49 then '持仓水平中'
                     else '持仓水平低'
                end) as stock_hold_ratio_level,
               --nvl(b.stock_industry_n, '行业偏好未知') as stock_industry_n,
               (case when b.stock_industry_n is null or length(b.stock_industry_n)=0 then '行业偏好未知'
                     else b.stock_industry_n
                end) as stock_industry_n,
               c.open_years as open_years,
               (case when c.open_years is null then '开户年限未知'
                     when c.open_years>=4 then '开户4年以上'
                     when c.open_years>=2 then '开户2至4年'
                     when c.open_years>=1 then '开户1至2年'
                     else '开户1年内'
                end) as open_years_level,
               d.stock_concentrate as stock_concentrate,
               (case when d.stock_concentrate is null then '持股集中度未知'
                     when d.stock_concentrate=0.69 then '持股集中度高'
                     when d.stock_concentrate=0.46 then '持股集中度中高'
                     when d.stock_concentrate=0.27 then '持股集中度中'
                     else '持股集中度低'
                end) as stock_concentrate_level
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join stock_hold_ratio_table a
        on cust.customer_no = a.customer_no
        left outer join industry_prefer_topN_table b
        on cust.customer_no = b.customer_no
        left outer join open_years_table c
        on cust.customer_no = c.customer_no
        left outer join stock_concentrate_table d
        on cust.customer_no = d.customer_no
    ''')
    
    
    # 标签缺失补全
    res_all = res_all.na.fill({'stock_hold_ratio_level': u'持仓水平未知', 
                               'stock_industry_n': u'行业偏好未知',
                               'open_years_level': u'开户年限未知',
                               'stock_concentrate_level': u'持股集中度未知'})
    # res_all.registerTempTable("res_all")
    
    
    # 写入hive物理表
    res_all.write.mode('overwrite').saveAsTable('tag_stat.invest_basic')

    hqlContext.uncacheTable('industry_prefer_detail_table')

    sc.stop()
