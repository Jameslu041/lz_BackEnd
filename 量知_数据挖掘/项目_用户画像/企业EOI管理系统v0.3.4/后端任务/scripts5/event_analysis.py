# coding: utf-8
#!/usr/bin/env python

"""
input_table: agg_cust_balance, Fact_Cust_Fund_Detail
output_table: tag_stat.event_analysis
"""

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
    
    
    ### 分区建表检查
    event_analysis_cols = hqlContext.sql('desc tag_stat.event_analysis').toPandas()
    if 'part_date' not in event_analysis_cols.col_name.values:
        raise ValueError(u'错误！event_analysis表不是分区表！')
    
    # 创建可能不存在的目标表
    # hqlContext.sql('''
        # create external table if not exists tag_stat.event_analysis
        # (
        # init_date string comment'riqi',
        # customer_no string comment'客户id',
        # event string comment'客户事件'
        # )
        # partitioned by (part_date varchar (8))
        # stored as parquet
    # ''')
    
    # 本地测试：获取所有客户id
    cust = hqlContext.sql('''
        select distinct customer_no 
        from ''' + db_name + '''.agg_cust_balance
        where part_date= ''' + run_date)
    cust.registerTempTable('cust_temp')
    
    
    
    # 资金转入
    zjzr = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',zjzr,'","date":"',init_date,'","balance":"',occur_balance,'"}') as zjzr
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(occur_balance) as occur_balance,
                    ('资金转入') as zjzr
             from(select customer_no,
                         occur_balance
                  from(select customer_no,
                              abs(occur_balance) as occur_balance,
                              exchange_type
                       from '''+ db_name +'''.Fact_Cust_Fund_Detail
                       where part_date='''+run_date+''') a
                  where exchange_type = '现金存' or
                        exchange_type = '现金支票存' or
                        exchange_type = '资金转存' or
                        exchange_type = '内部转账存' or
                        exchange_type = '资金蓝补' or
                        exchange_type = '利息归本' or
                        exchange_type = '转账存' or
                        exchange_type = '银行转存' or
                        exchange_type = '转存管转入' or
                        exchange_type = '存折存' or
                        exchange_type = '冲现金取' or
                        exchange_type = '冲现金红冲' or
                        exchange_type = '冲银行取' or
                        exchange_type = '冲存折取' or
                        exchange_type = '转账支票存' or
                        exchange_type = '支票内转存' or
                        exchange_type = '冲转账支票取' or
                        exchange_type = '冲支票内转取' or
                        exchange_type = '交收资金划入') b
             group by customer_no) c
    ''')
    zjzr.registerTempTable('zjzr_temp')
    
    #资金转出
    zjzc = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',zjzc,'","date":"',init_date,'","value":"',occur_balance,'"}') as zjzc
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(occur_balance) as occur_balance,
                    ('资金转出') as zjzc
             from(select customer_no,
                         nvl(occur_balance,0) as occur_balance
                  from(select customer_no,
                              abs(occur_balance) as occur_balance,
                              exchange_type
                       from '''+ db_name +'''.Fact_Cust_Fund_Detail
                       where part_date = '''+run_date+''') a
                  where exchange_type = '现金取' or
                        exchange_type = '现金支票取' or
                        exchange_type = '资金转取' or
                        exchange_type = '内部转账取' or
                        exchange_type = '资金红冲' or
                        exchange_type = '罚息归本' or
                        exchange_type = '利税代扣' or
                        exchange_type = '强制现金取' or
                        exchange_type = '强制支票取' or
                        exchange_type = '银行转取' or
                        exchange_type = '转存管转出' or
                        exchange_type = '存折取' or
                        exchange_type = '冲现金存' or
                        exchange_type = '冲现金蓝补' or
                        exchange_type = '冲转账存' or
                        exchange_type = '冲银行存' or
                        exchange_type = '冲存折存' or
                        exchange_type = '转账支票取' or
                        exchange_type = '支票内转取' or
                        exchange_type = '冲转账支票存' or
                        exchange_type = '冲支票内转存') b
             group by customer_no) c
    ''')
    zjzc.registerTempTable('zjzc_temp')
    
    #客户每日每支股票买入情况
    gpmr0 = hqlContext.sql('''
        select customer_no,
               business_balance,
               concat('{"stock_name":"',stock_name,'","stock_no":"',stock_no,'","qty":"',business_qty,'"}') as stock
        from(select customer_no,
                    stock_no,
                    stock_name,
                    sum(business_balance) as business_balance,
                    sum(business_qty) as business_qty
             from(select customer_no,
                         nvl(business_balance,0) as business_balance,
                         nvl(stock_no,'') as stock_no,
                         nvl(stock_name,'') as stock_name,
                         nvl(business_qty,0) as business_qty
                  from '''+ db_name +'''.Fact_Cust_Stock_Detail
                  where part_date = '''+run_date+''' and
                        exchange_type = '证券买入') a
             group by customer_no,stock_no,stock_name) b
    ''')
    gpmr0.registerTempTable('gpmr0_temp')
    
    #股票买入
    gpmr = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',gpmr,'","date":"',init_date,'","balance":"',business_balance,'","stock":[',stock,']}') as gpmr
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(business_balance) as business_balance,
                    concat_ws(',', collect_list(stock)) as stock,
                    ('股票买入') as gpmr
             from gpmr0_temp
             group by customer_no) a
    ''')
    gpmr.registerTempTable('gpmr_temp')
    
    #客户每日每支股票卖出情况
    gpmc0 = hqlContext.sql('''
        select customer_no,
               business_balance,
               concat('{"stock_name":"',stock_name,'","stock_no":"',stock_no,'","qty":"',business_qty,'"}') as stock
        from(select customer_no,
                    stock_no,
                    stock_name,
                    sum(business_balance) as business_balance,
                    sum(business_qty) as business_qty
             from(select customer_no,
                         nvl(business_balance,0) as business_balance,
                         nvl(stock_no,'') as stock_no,
                         nvl(stock_name,'') as stock_name,
                         nvl(business_qty,0) as business_qty
                  from '''+ db_name +'''.Fact_Cust_Stock_Detail
                  where part_date = '''+run_date+''' and
                        exchange_type = '证券卖出') a
             group by customer_no,stock_no,stock_name) b
    ''')
    gpmc0.registerTempTable('gpmc0_temp')
    
    # 股票卖出
    gpmc = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',gpmc,'","date":"',init_date,'","balance":"',business_balance,'","stock":[',stock,']}') as gpmc
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(business_balance) as business_balance,
                    concat_ws(',', collect_list(stock)) as stock,
                    ('股票卖出') as gpmc
             from gpmc0_temp
             group by customer_no) a
    ''')
    gpmc.registerTempTable('gpmc_temp')
    
    # 客户每日融资买入股票情况
    rzmr0 = hqlContext.sql('''
        select customer_no,
               business_balance,
               concat('{"stock_name":"',stock_name,'","stock_no":"',stock_no,'","qty":"',business_qty,'"}') as stock
        from(select customer_no,
                    stock_no,
                    stock_name,
                    sum(business_balance) as business_balance,
                    sum(business_qty) as business_qty
             from(select customer_no,
                         nvl(occur_balance,0) as business_balance,
                         nvl(stock_code,'') as stock_no,
                         nvl(stock_name,'') as stock_name,
                         nvl(occur_qty,0) as business_qty
                  from '''+ db_name +'''.fact_cust_compact_detail
                  where part_date = '''+run_date+''' and
                        exchange_type = '融资买入') a
             group by customer_no,stock_no,stock_name) b
    ''')
    rzmr0.registerTempTable('rzmr0_temp')
    
    # 融资买入
    rzmr = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',rzmr,'","date":"',init_date,'","balance":"',business_balance,'","stock":[',stock,']}') as rzmr
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(business_balance) as business_balance,
                    concat_ws(',',collect_list(stock)) as stock,
                    ('融资买股') as rzmr
             from rzmr0_temp
             group by customer_no) a
    ''')
    rzmr.registerTempTable('rzmr_temp')
    
    # 融资还款
    rzhk = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',rzhk,'","date":"',init_date,'","balance":"',business_balance,'"}') as rzhk
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(business_balance) as business_balance,
                    ('融资还款') as rzhk
             from(select customer_no,
                         business_balance
                  from(select customer_no,
                              occur_balance as business_balance,
                              exchange_type
                       from '''+ db_name +'''.fact_cust_compact_detail
                       where part_date = '''+run_date+''') a
                  where exchange_type = '卖券还款' or 
                        exchange_type = '直接还款（平仓归还）' or
                        exchange_type = '直接还款（现金归还）') b
             group by customer_no) c
    ''')
    rzhk.registerTempTable('rzhk_temp')

    #客户每日融券卖出股票情况
    rqmc0 = hqlContext.sql('''
        select customer_no,
               business_balance,
               concat('{"stock_name":"',stock_name,'","stock_no":"',stock_no,'","qty":"',business_qty,'"}') as stock
        from(select customer_no,
                    stock_no,
                    stock_name,
                    sum(business_balance) as business_balance,
                    sum(business_qty) as business_qty
             from(select t.customer_no,
                         business_balance,
                         stock_no,
                         stock_name,
                         business_qty
                  from cust_temp t left outer join
                       (select customer_no,
                               nvl(occur_balance,0) as business_balance,
                               nvl(stock_code,'') as stock_no,
                               nvl(stock_name,'') as stock_name,
                               nvl(occur_qty,0) as business_qty
                        from '''+ db_name +'''.fact_cust_compact_detail
                        where part_date = '''+run_date+''' and
                              exchange_type = '融券卖出') a
                  on t.customer_no = a.customer_no) i
             group by customer_no,stock_no,stock_name) b
    ''')
    rqmc0.registerTempTable('rqmc0_temp')

    #融券卖出
    rqmc = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',rqmc,'","date":"',init_date,'","balance":"',business_balance,'","stock":[',stock,']}') as rqmc
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(business_balance) as business_balance,
                    concat_ws(',',collect_list(stock)) as stock,
                    ('融券卖出') as rqmc
             from rqmc0_temp
             group by customer_no) a
    ''')
    rqmc.registerTempTable('rqmc_temp')

    #客户每日融券还券情况
    rqhq0 = hqlContext.sql('''
        select customer_no,
               business_balance,
               concat('{"stock_name":"',stock_name,'","stock_no":"',stock_no,'","qty":"',business_qty,'"}') as stock
        from(select customer_no,
                    stock_no,
                    stock_name,
                    sum(business_balance) as business_balance,
                    sum(business_qty) as business_qty
             from(select cu.customer_no,
                         business_balance,
                         stock_no,
                         stock_name,
                         business_qty
                  from cust_temp cu left outer join 
                       (select customer_no,
                               business_balance,
                               stock_no,
                               stock_name,
                               business_qty
                        from(select customer_no,
                                    nvl(occur_balance,0) as business_balance,
                                    nvl(stock_code,'') as stock_no,
                                    nvl(stock_name,'') as stock_name,
                                    nvl(occur_qty,0) as business_qty,
                                    exchange_type
                             from '''+ db_name +'''.fact_cust_compact_detail
                             where part_date = '''+run_date+''') t
                        where exchange_type = '现券还券' or
                              exchange_type = '买券还券') a
                  on cu.customer_no = a.customer_no) i 
             group by customer_no,stock_no,stock_name) b
    ''')
    rqhq0.registerTempTable('rqhq0_temp')

    # 融券还券
    rqhq = hqlContext.sql('''
        select customer_no,
               concat('{"name":"',rqhq,'","date":"',init_date,'","balance":"',business_balance,'","stock":[',stock,']}') as rqhq
        from(select customer_no,
                    '''+run_date+''' as init_date,
                    sum(business_balance) as business_balance,
                    concat_ws(',',collect_list(stock)) as stock,
                    ('融券还券') as rqhq
             from rqhq0_temp
             group by customer_no) a
    ''')
    rqhq.registerTempTable('rqhq_temp')

    # 将客户号与所有事件左关联
    event = hqlContext.sql('''
        select cust.customer_no as customer_no,
               '''+run_date+''' as init_date,
               zjzr,
               zjzc,
               gpmr,
               gpmc,
               rzmr,
               rzhk,
               rqmc,
               rqhq
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join zjzr_temp b 
        on cust.customer_no = b.customer_no 
        left outer join zjzc_temp c 
        on cust.customer_no = c.customer_no 
        left outer join gpmr_temp d 
        on cust.customer_no = d.customer_no 
        left outer join gpmc_temp e 
        on cust.customer_no = e.customer_no 
        left outer join rzmr_temp f 
        on cust.customer_no = f.customer_no 
        left outer join rzhk_temp g 
        on cust.customer_no = g.customer_no 
        left outer join rqmc_temp h 
        on cust.customer_no = h.customer_no 
        left outer join rqhq_temp i 
        on cust.customer_no = i.customer_no
    ''')
    event.registerTempTable('event_temp')
    
    # 将事件拼接成字符串
    event1 = hqlContext.sql('''
        select customer_no,
               init_date,
               concat(zjzr,',',zjzc,',',gpmr,',',gpmc,',',rzmr,',',rzhk,',',rqmc,',',rqhq) as event
        from(select customer_no,
                    init_date,
                    nvl(zjzr,'na') as zjzr,
                    nvl(zjzc,'na') as zjzc,
                    nvl(gpmr,'na') as gpmr,
                    nvl(gpmc,'na') as gpmc,
                    nvl(rzmr,'na') as rzmr,
                    nvl(rzhk,'na') as rzhk,
                    nvl(rqmc,'na') as rqmc,
                    nvl(rqhq,'na') as rqhq
             from event_temp) a
    ''')
    event1.registerTempTable('event1_temp')
    
    # 去除空值
    event2 = hqlContext.sql('''
        select distinct customer_no,
               init_date,
               regexp_replace(regexp_replace(regexp_replace(event,'na,',''),
                                             ',na',''),
                              'na','') as event
        from event1_temp
    ''')
    event2.registerTempTable('event2_temp')
    
    # 插入分区表
    hqlContext.sql('''
        insert overwrite table tag_stat.event_analysis
        partition(part_date = '''+run_date+''')
        select init_date,
               customer_no,
               concat('{"date":"',init_date,'","data":[',event,']}') as event
        from event2_temp
    ''')
    
    sc.stop()