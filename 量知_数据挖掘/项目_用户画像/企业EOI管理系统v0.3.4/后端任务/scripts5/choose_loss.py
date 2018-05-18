# coding:utf-8

"""
input_table: agg_cust_stock, fact_cust_stock_detail
output_table: choose_loss
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
    
    three_months_ago = compute_date(run_date, -90)  # 3个月前的日期
    
    
    
    xuangunengli = hqlContext.sql('''
        select n.customer_no,
               n.xuangunengli,
               (case when n.xuangunengli is null then '选股能力未知'
                     when n.xuangunengli <= 0.10 then '选股能力弱'
                     when n.xuangunengli > 0.10 and n.xuangunengli < 0.40 then '选股能力一般'
                     when n.xuangunengli >= 0.40 then '选股能力强'
                end) as tag1
        from
            (select f.customer_no,
                    sum(f.label)/count(1) as xuangunengli
             from
                (select c.customer_no,
                    c.stock_no,
                    (case when c.yuemoshizhi+nvl(e.total_sell_balance,0)-c.yuechushizhi-nvl(d.total_buy_balance,0) > 0 then 1
                          else 0
                     end) as label
                 from
                    (select b.customer_no,
                            b.stock_no,
                            avg(b.yuechushizhi_base) as yuechushizhi,
                            avg(b.yuemoshizhi_base) as yuemoshizhi
                    from
                        (select a.customer_no, 
                                a.stock_no, 
                                first_value(a.shizhi) over(partition by a.customer_no,a.stock_no order by a.init_date) as yuechushizhi_base, 
                                last_value(a.shizhi) over(partition by a.customer_no,a.stock_no order by a.init_date) as yuemoshizhi_base
                         from
                              (
                               select customer_no,stock_no,
                                   init_date,
                                   sum(nvl(current_amount,0)+nvl(frozen_amount,0)) as shizhi
                               from ''' + db_name + '''.agg_cust_stock
                               where part_date >=''' + three_months_ago + ''' and 
                                     part_date <=''' + run_date + '''
                               group by customer_no,stock_no,init_date 
                               order by customer_no,stock_no,init_date 
                              ) a) b
                    group by b.customer_no,b.stock_no) c
                    left outer join
                        (select customer_no,
                            stock_no,
                            sum(nvl(business_balance,0)) as total_buy_balance
                         from ''' + db_name + '''.fact_cust_stock_detail
                         where part_date>=''' + three_months_ago + ''' and
                               part_date<=''' + run_date + ''' and
                               entrust_bs = '买入'
                         group by customer_no,stock_no) d
                    on c.customer_no = d.customer_no and
                       c.stock_no = d.stock_no
                    left outer join
                        (select customer_no,
                                stock_no,
                                sum(nvl(business_balance,0)) as total_sell_balance
                         from ''' + db_name + '''.fact_cust_stock_detail
                         where part_date>=''' + three_months_ago + ''' and
                               part_date<=''' + run_date + ''' and
                               entrust_bs = '卖出'
                         group by customer_no,stock_no) e
                    on c.customer_no = e.customer_no and
                       c.stock_no = e.stock_no
                ) f
             group by f.customer_no) n
    ''')
    xuangunengli.registerTempTable('xuangunengli_table')

    kuisunlv = hqlContext.sql('''
        select n.customer_no,
               n.kuisunlv,
               (case when n.kuisunlv is null then '无亏损行为或未知'
                     when n.kuisunlv <= 0.10 then '亏损率低'
                     when n.kuisunlv > 0.10 and n.kuisunlv < 0.40 then '亏损率中等'
                     when n.kuisunlv >= 0.40 then '亏损率高'
                end) as tag2
        from 
            (select d.customer_no,
                    avg(1-nvl(d.maichujine/d.mairujine,1)) as kuisunlv
             from
                (select c.customer_no,
                        c.stock_no,
                        sum(c.business_qty*c.business_price) as maichujine,
                        sum(c.business_qty*c.cost_price) as mairujine
                 from
                    (select a.customer_no,
                            a.stock_no,
                            a.init_date,
                            a.business_price,
                            a.business_qty,
                            b.cost_price
                     from
                        (select customer_no,
                                stock_no,
                                init_date,
                                entrust_bs,
                                business_price,
                                business_qty
                        from ''' + db_name + '''.fact_cust_stock_detail
                        where part_date >=''' + three_months_ago + ''' and 
                              part_date <=''' + run_date + ''' and 
                              entrust_bs = '卖出') a
                    left outer join
                        (select customer_no,
                                stock_no,
                                init_date,
                                cost_price
                         from ''' + db_name + '''.agg_cust_stock
                         where part_date >=''' + three_months_ago + ''' and 
                               part_date <=''' + run_date + ''') b
                    on a.customer_no = b.customer_no and
                       a.stock_no = b.stock_no and
                       a.init_date = b.init_date) c
                 where c.business_price < c.cost_price
             group by c.customer_no,c.stock_no) d
        group by d.customer_no) n
    ''')
    kuisunlv.registerTempTable('kuisunlv_table')
    
    
    # 整合指标+标签
    res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               a.xuangunengli as stock_selection_ability_value,
               a.tag1 as stock_selection_ability_label,
               nvl(b.kuisunlv,0) as loss_rate_value,
               b.tag2 as loss_rate_label
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join xuangunengli_table a
        on cust.customer_no = a.customer_no
        left outer join kuisunlv_table b
        on cust.customer_no = b.customer_no
    ''')
    
    # 标签缺失补全
    res_all = res_all.na.fill({'stock_selection_ability_label': u'选股能力未知', 
                               'loss_rate_value': 0,
                               'loss_rate_label': u'无亏损行为或未知'})
    res_all.registerTempTable("res_all")
    
    
    hqlContext.sql('drop table if exists tag_stat.choose_loss')
    hqlContext.sql('''
        create table tag_stat.choose_loss as 
        select * from res_all
    ''')
    
    
    sc.stop()