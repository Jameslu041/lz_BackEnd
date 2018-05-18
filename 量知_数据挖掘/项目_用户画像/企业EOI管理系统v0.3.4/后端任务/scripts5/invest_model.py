# coding:utf-8
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


# 解析出持仓开始结束日期
# 传入参数的格式tuple: (客户ID,股票ID,股票行业类型, 日期1:持股量1 日期2:持股量2)
# 日期格式为YYYY-MM-DD方便后续计算
def parse_s_e_date(cus_stock_info):
    tmp_date = ''  # 记录当前持仓开始时间
    interval_list = []  # 保存每次持仓开始结束时间，格式:date1,date2

    detail_list = cus_stock_info[1].split()  # 散列日期、持股量
    detail_list.sort()  # 按日期排序
    for item in detail_list:
        dt = item.split(':')[0]  # 日期
        num = round(float(item.split(':')[1]))  # 股票持有量
        if tmp_date == '':
            if num == 0:  # 遇到上一个结束的日期，忽略
                continue
            else:
                tmp_date = dt  # 周期开始日期赋值
        else:
            if num == 0:  # 遇到该段持仓结束时间
                tmp_list = cus_stock_info[0].split(',')
                tmp_date = tmp_date[:4] + '-' + tmp_date[4:6] + '-' + tmp_date[6:8]  # 日期YYYYMMDD重新拼接为YYYY-MM-DD
                tmp_list.append(tmp_date)
                dt = dt[:4] + '-' + dt[4:6] + '-' + dt[6:8]  # 日期YYYYMMDD重新拼接为YYYY-MM-DD
                tmp_list.append(dt)
                interval_list.append(tmp_list)
                tmp_date = ''  # tmp_date重新赋值
            else:
                continue
    return interval_list


# 计算每支股票的盈亏率
# 传入数据格式：customer_no, stock_no, 交易流水明细(日期1，买入卖出类型1，交易量1，交易单价1     日期2，买入卖出类型2，交易量2，交易单价2)
def compute_rate(raw_row):
    cust_id = raw_row['customer_no']  # 客户ID
    stock_id = raw_row['stock_no']  # 股票ID
    stock_cycle_start_date = raw_row['start_date'].replace('-', '')  # 起始日期，原格式YYYY-MM-DD，替换掉-
    stock_cycle_end_date = raw_row['end_date'].replace('-', '')  # 结束日期，原格式YYYY-MM-DD，替换掉-
    detail_info = raw_row['stock_deal_detail'].split()  # 交易流水明细list

    tmp_list = list()  # 格式[[],[]]
    for item in detail_info:
        tmp_list.append(item.split(','))
    stock_detail_df = pd.DataFrame(tmp_list, columns=['init_date', 'buy_type', 'deal_num', 'price'])  # 转成dataframe

    stock_buy_num_list = list()  # stock数组记录股买入票股数
    stock_buy_price_list = list()  # price记录对应的买入股票市价
    profit_rate_list = list()  # 存储每笔卖出交易的盈亏率

    for index, row in stock_detail_df.iterrows():
        stock_cost = 0.0  # 买股票的成本

        if stock_cycle_start_date <= row['init_date'] < stock_cycle_end_date:  # 时间在有效范围内
            if row['buy_type'] == 'buy':  # 买入，那么在数组中添加股数和单价记录
                stock_buy_num_list.append(row['deal_num'])
                stock_buy_price_list.append(row['price'])
            else:  # 否则，比较卖出的股票股数与剩余最早买入的股票股数变化
                # 若卖出比剩余最早一次买入多，那么在数组中删除该次买入的股票股数和单价，并调整卖出股票数，继续进行下一次比较
                sell_num = abs(float(row['deal_num']))  # 卖出股数

                if len(stock_buy_num_list) == 0:  # 还未有买入交易，则过滤
                    continue
                else:
                    for i in range(len(stock_buy_num_list)):
                        if i < (len(stock_buy_num_list) - 1):  # 未遍历到最后一个元素
                            if stock_buy_num_list[i] == 'del':  # 已被删除的数据
                                continue
                            else:
                                if sell_num >= float(stock_buy_num_list[i]):  # 卖出股数大于最前面的买入股数
                                    stock_cost += float(stock_buy_num_list[i]) * float(stock_buy_price_list[i])  # 股票成本
                                    sell_num = sell_num - float(stock_buy_num_list[i])  # 更新卖出数量
                                    stock_buy_num_list[i] = 'del'
                                    stock_buy_price_list[i] = 'del'
                                else:
                                    stock_cost += sell_num * float(stock_buy_price_list[i])  # 股票成本
                                    stock_buy_num_list[i] = float(stock_buy_num_list[i]) - sell_num
                        else:  # 遍历到了最后一个元素
                            if stock_buy_num_list[i] == 'del':  # 没有买入数据了
                                pass
                            else:
                                if sell_num >= float(stock_buy_num_list[i]):  # 卖出股数大于最前面的买入股数
                                    stock_cost += float(stock_buy_num_list[i]) * float(stock_buy_price_list[i])  # 股票成本
                                    stock_buy_num_list[i] = 'del'
                                    stock_buy_price_list[i] = 'del'
                                else:
                                    stock_cost += sell_num * float(stock_buy_price_list[i])  # 股票成本
                                    stock_buy_num_list[i] = float(stock_buy_num_list[i]) - sell_num
                        stock_profit = abs(float(row['deal_num'])) * float(row['price'])  # 回本
                        if stock_cost != 0.0:  # 排除除数为0的情况
                            profit_rate = round((stock_profit - stock_cost) / stock_cost, 6)  # 该轮收益率，可正可负，保留6位小数
                            profit_rate_list.append(str(profit_rate))
        else:
            continue
    profit_rate_list_str = ' '.join(profit_rate_list)  # profit_rate_list转为字符串，空格连接
    return cust_id + ',' + stock_id + ',' + profit_rate_list_str


# 计算每支股票持股周期的方差
def compute_cycle_variance(cycle_row):
    cust_id = cycle_row['customer_no']
    s_id = cycle_row['stock_no']
    tmp_list_to_nparr = np.array(cycle_row['cycle_list'].split()).astype(np.float)
    tmp_list_to_nparr = tmp_list_to_nparr[tmp_list_to_nparr >= 0.0]  # 过滤掉特殊处理的负数
    var_v = np.sqrt(tmp_list_to_nparr.var())  # 数组方差开根号

    return cust_id + ' ' + s_id + ' ' + str(1 / (var_v + 1))  # 返回方差+1的倒数


# 计算客户缴费方差,sum,和上升占比
# 数据格式：customer_no, fee1,fee2....feen，fee按日期排序(customera_no, fee_list)
# 返回费用客户总和、费用方差加1倒数、费用上升占比
def compute_fee_variance(fees):
    cust_id = fees['customer_no']
    fee_list = np.array(fees['fee_list'].split()).astype(np.float)
    fee_var = np.sqrt(fee_list.var())  # 方差开根号
    fee_var_rec = 1 / (fee_var + 1)  # 方差加1后的倒数

    rise_count = 0  # 计数，缴费金额上升的次数
    fee_sum = 0.0  # 客户费用总计
    for index in range(len(fee_list) - 1):
        if fee_list[index] < fee_list[index + 1]:
            rise_count += 1  # 计数加1
        fee_sum += fee_list[index]

    fee_sum += fee_list[len(fee_list) - 1]  # 加上最后一项
    rise_rate = rise_count / len(fee_list)  # 上升占比

    return cust_id + ' ' + str(fee_sum) + ' ' + str(fee_var_rec) + ' ' + str(rise_rate)


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

    six_months_ago = compute_date(run_date, -180)  # 6个月前的日期
    one_year_ago = compute_date(run_date, -365)  # 12个月前
    
    
    
    # 持股流水明细
    stock_hold_cur_detail = hqlContext.sql('''
        select init_date,
               customer_no,
               stock_no,
               nvl(stock_name, '') as stock_name,
               nvl(stock_type, '') as stock_type,
               nvl(current_qty, 0) as current_qty
        from ''' + db_name + '''.agg_cust_stock
        where part_date>=''' + one_year_ago + '''
          and part_date<=''' + run_date + '''
    ''').rdd

    # collect 后的结果集type为list
    s_e_date_rdd = stock_hold_cur_detail. \
        map(lambda row_item: (row_item['customer_no'] + ',' + row_item['stock_no'] + ',' + row_item['stock_type'],
                              row_item['init_date'] + ':' + str(row_item['current_qty']))). \
        reduceByKey(lambda x, y: x + ' ' + y). \
        flatMap(lambda i: parse_s_e_date(i))

    # DataFrame可由list、tuple、rdd转化
    s_e_date_df = hqlContext.createDataFrame(s_e_date_rdd, [
        'customer_no', 'stock_no', 'stock_type', 'stock_start_date', 'stock_end_date'])
    s_e_date_df.registerTempTable("stock_hold_date_table")  # 所有持仓信息注册为表
    hqlContext.cacheTable('stock_hold_date_table')

    # 偏好股票明细
    stock_prefer_detail = hqlContext.sql('''
        select a.customer_no as customer_no,
               a.stock_no as stock_no,
               a.stock_name as stock_name,
               row_number() over (partition by customer_no order by total_exchange_amount) as rank
        from
        (
        select customer_no,
               stock_no,
               stock_name,
               sum(business_balance) as total_exchange_amount
        from ''' + db_name + '''.fact_cust_stock_detail
        where part_date>=''' + one_year_ago + '''
          and part_date<=''' + run_date + '''
          and entrust_bs='买入'
          and business_type='证券买卖'
        group by customer_no,
                 stock_no,
                 stock_name
        ) a
    ''')
    stock_prefer_detail.registerTempTable('stock_prefer_detail_table')
    hqlContext.cacheTable('stock_prefer_detail_table')

    # 炒股模式
    stock_deal_model = hqlContext.sql('''
        select c.customer_no as customer_no,
               c.avg_hold_days as avg_hold_days,
               (case when c.avg_hold_days is null then '炒股模式未知'
                     when c.avg_hold_days <= 3 then '超短线'
                     when c.avg_hold_days <= 7 then '短线'
                     when c.avg_hold_days < 90 then '中线'
                     else '长线'
                end) as deal_model
        from 
        (
            select a.customer_no as customer_no,
                   sum(a.hold_days)/count(1) as avg_hold_days
            from
            (
                select customer_no,
                       stock_no,
                       datediff(stock_end_date, stock_start_date) as hold_days
                from stock_hold_date_table
            ) a
            left outer join 
            (
                select customer_no,
                       stock_no
                from stock_prefer_detail_table
                where rank<=20
            ) b
            on a.customer_no=b.customer_no
            and a.stock_no=b.stock_no
            where b.customer_no is not null
              and b.stock_no is not null
            group by a.customer_no
        ) c
    ''')
    stock_deal_model.registerTempTable('stock_deal_model_table')

    # 客户持股规律（个股）
    # 取前20个股票偏好
    # 获取每个客户所持股票持股周期方差+1的倒数均值
    stock_cycle_detail = hqlContext.sql('''
        select a.customer_no as customer_no,
               a.stock_no as stock_no,
               concat_ws(' ', collect_list(cast(nvl(b.hold_days, -1.0) as string))) as cycle_list
        from
        (
            select customer_no,
                   stock_no
            from stock_prefer_detail_table
            where rank<=20
        ) a
        left outer join
        (
            select customer_no,
                   stock_no,
                   datediff(stock_end_date, stock_start_date) as hold_days
            from stock_hold_date_table
        ) b
        on a.customer_no=b.customer_no
        and a.stock_no=b.stock_no
        where b.customer_no is not null
          and b.stock_no is not null
        group by a.customer_no,
                 a.stock_no
    ''').rdd

    # 计算客户所持股票持股周期方差+1后的倒数
    # 前10的股票
    stock_cycle_rdd = stock_cycle_detail.map(lambda cycle_list: compute_cycle_variance(cycle_list)).map(
        lambda row: (row.split()[0], float(row.split()[2]))).reduceByKey(lambda v_1, v_2: v_1 + v_2).map(
        lambda tp: (tp[0], round(tp[1] / 20, 6)))

    # cycle_stable_level<=1.0，越小越不稳定
    cycle_var_df = hqlContext.createDataFrame(stock_cycle_rdd, ['customer_no', 'cycle_stable_level'])
    cycle_var_df.registerTempTable('cycle_stable_table')

    # 客户价值计算
    # 由客户贡献率和客户贡献率方差+1的倒数及上升占比相乘决定，越接近1价值越高
    cust_worthy = hqlContext.sql('''
        select t3.customer_no as customer_no,
               concat_ws(' ', collect_list( cast (t3.interest_fee+t3.commission_fee as string))) as fee_list
        from 
        (
            select t1.init_date as init_date,
                   t1.customer_no as customer_no,
                   nvl(t1.interest_fee, 0.0) as interest_fee,
                   nvl(t2.commission_fee, 0.0) as commission_fee,
                   row_number() over (partition by t1.customer_no order by t1.init_date) as rn
            from 
            (
            select init_date,
                   customer_no,
                   sum(abs(occur_balance)) as interest_fee
            from ''' + db_name + '''.fact_cust_fund_detail
            where part_date>=''' + one_year_ago + '''
              and part_date<=''' + run_date + '''
              and trim(exchange_type) in 
                 ('直接平仓偿还其他负债利息',
                  '其他负债罚息扣收',
                  '直接平仓偿还融资利息',
                  '融资合约利息结息扣收',
                  '卖券偿还其他负债利息',
                  '自动偿还其他负债利息',
                  '平仓偿还其他负债利息',
                  '直接偿还其他负债利息',
                  '买券偿还融券利息',
                  '卖券偿还融资利息',
                  '自动偿还融资利息',
                  '直接偿还融资利息',
                  '专项融券费用扣收',
                  '专项融券违约金扣收',
                  '专项融券补偿金扣收',
                  '融券罚息扣收',
                  '融资罚息扣收',
                  '专项融资费用扣收',
                  '专项融资违约金扣收',
                  '平仓偿还融资利息'
                  )
            group by init_date,
                     customer_no
            ) t1
            left outer join
            (
            select init_date,
                   customer_no, 
                   sum(fare0+fare1+fare2+fare3+farex-exchange_fare0-exchange_fare1-exchange_fare2-exchange_fare3-
                        exchange_fare4-exchange_fare5-exchange_fare6-exchange_farex) as commission_fee
            from ''' + db_name + '''.fact_cust_stock_detail
            where part_date>=''' + one_year_ago + '''
              and part_date<=''' + run_date + '''
            group by init_date,
                     customer_no
            ) t2
            on t1.init_date=t2.init_date
            and t1.customer_no=t2.customer_no
        ) t3
        group by t3.customer_no
    ''').rdd

    # 计算每个客户的费用总和、费用方差+1的倒数、费用上升占比
    fee_detail = cust_worthy.map(lambda raw_row: compute_fee_variance(raw_row)).map(
        lambda fee_row: (fee_row.split()[0], fee_row.split()[1], fee_row.split()[2], fee_row.split()[3]))

    fee_df = hqlContext.createDataFrame(fee_detail, ['customer_no', 'cust_fee_sum', 'var_rec', 'r_rate'])
    fee_df.registerTempTable('fee_table')  # 注册成表

    # 计算客户价值，未分等级
    # 分别是客户贡献水平、客户贡献稳定度、客户贡献上升占比
    cust_worth_value = hqlContext.sql('''
        select a.customer_no as customer_no,
               (a.cust_fee_sum-a.min_fee)/(a.max_fee-a.min_fee) as contri_level_value,
               a.var_rec as contri_stab_level_value,
               a.r_rate as up_rate
        from 
        (
            select customer_no,
                   double(cust_fee_sum) as cust_fee_sum,
                   double(var_rec) as var_rec,
                   double(r_rate) as r_rate,
                   min(double(cust_fee_sum)) over () as min_fee,
                   max(double(cust_fee_sum)) over () as max_fee
            from fee_table
        ) a
    ''')
    cust_worth_value.registerTempTable('cust_worth_value_table')

    # 客户持仓风险等级
    hold_risk_level = hqlContext.sql('''
        select a.customer_no as customer_no,
               (case when b.avg_rate is null then a.close_rate
                     when a.close_rate==-1 then b.avg_rate
                     else 0.7*a.close_rate + 0.3*b.avg_rate
                end) as hold_risk_value
        from 
        (
            select customer_no,
                   close_rate
            from ''' + db_name + '''.agg_cust_balance
            where part_date=''' + run_date + '''
        ) a
        left outer join
        (
            select customer_no,
                   sum(close_rate)/count(distinct init_date) as avg_rate
            from ''' + db_name + '''.agg_cust_balance
            where part_date>=''' + six_months_ago + '''
              and part_date<''' + run_date + '''
              and close_rate!=-1
            group by customer_no
        ) b
        on a.customer_no=b.customer_no
    ''')
    hold_risk_level.registerTempTable('hold_risk_level_table')

    # 止损能力，只计算亏损股票的亏损率，不计算盈利率
    # 原始数据处理，每支股票在最近半年内的起止时间，及期间股票的买卖数量和价格
    stock_deal_detail = hqlContext.sql('''
        select c.customer_no as customer_no,
               c.stock_no as stock_no,
               c.start_date as start_date,
               c.end_date as end_date,
               concat_ws('    ', 
                         collect_list(
                                      concat(c.init_date, ',', 
                                             c.entrust_bs, ',', 
                                             c.business_qty, ',', 
                                             c.business_price
                                      )
                         )
               ) as stock_deal_detail
        from
        (
            select a.customer_no as customer_no,
                   a.stock_no as stock_no,
                   a.init_date as init_date,
                   (case when a.entrust_bs = '买入' then 'buy'
                         when a.entrust_bs = '卖出' then 'sell'
                         else ''
                    end) as entrust_bs,
                   a.business_qty as business_qty,
                   a.business_price as business_price,
                   (case when b.customer_no is null or b.stock_no is null
                         then ''' + six_months_ago + '''
                         else b.cycle_start_date
                    end) as start_date,
                   (case when b.customer_no is null or b.stock_no is null 
                         then ''' + run_date + '''
                         else b.cycle_end_date
                    end) as end_date,
                   row_number() over (partition by a.customer_no,a.stock_no order by a.init_date) as rank
            from
            (
                select customer_no,
                       stock_no,
                       init_date,
                       entrust_bs,
                       business_qty,
                       business_price
                from ''' + db_name + '''.fact_cust_stock_detail
                where part_date>=''' + six_months_ago + '''
                  and part_date<=''' + run_date + '''
                  and business_type='证券买卖'
            ) a
            left outer join 
            (
                select customer_no,
                       stock_no,
                       first_value(stock_start_date) over (partition by customer_no, stock_no order by stock_start_date) 
                                                                                                    as cycle_start_date,
                       first_value(stock_end_date) over (partition by customer_no, stock_no order by stock_end_date desc) 
                                                                                                    as cycle_end_date
                from stock_hold_date_table
                where stock_start_date>=''' + six_months_ago + '''
                  and stock_start_date<=''' + run_date + '''
                  and stock_end_date>=''' + six_months_ago + '''
                  and stock_end_date<=''' + run_date + '''
                  and stock_start_date<=stock_end_date
            ) b
            on a.customer_no=b.customer_no
            and a.stock_no=b.stock_no
        ) c
        group by c.customer_no,
                 c.stock_no,
                 c.start_date,
                 c.end_date
    ''').rdd

    deal_detail_rdd = stock_deal_detail.map(lambda raw_row: compute_rate(raw_row)).map(
        lambda rate_row: (rate_row.split(',')[0] + ',' + rate_row.split(',')[1], rate_row.split(',')[2])).flatMapValues(
        lambda v: v.split()).filter(lambda tp: float(tp[1]) <= 0.0).map(
        lambda c_s_r_row: (c_s_r_row[0].split(',')[0], c_s_r_row[1] + ',' + '1')).reduceByKey(
        lambda r1, r2: str(float(r1.split(',')[0]) + float(r2.split(',')[0])) + ',' + str(
            int(r1.split(',')[1]) + int(r2.split(',')[1]))).map(
        lambda tp: (tp[0], round(float(tp[1].split(',')[0]) / float(tp[1].split(',')[1]), 6)))

    # 列名：客户id、平均亏损率
    deal_detail_df = hqlContext.createDataFrame(deal_detail_rdd, ['customer_no', 'avg_loss_rate'])
    deal_detail_df.registerTempTable('deal_detail_table')

    # 止损能力
    s_l_a = hqlContext.sql('''
        select customer_no,
               avg_loss_rate,
               (case when avg_loss_rate is null then '止损能力未知'
                     when avg_loss_rate>=-0.15 then '止损能力高'
                     when avg_loss_rate>=-0.25 then '止损能力中高'
                     when avg_loss_rate>=-0.4 then '止损能力中'
                     else '止损能力低'
                end) as control_loss_level          
        from deal_detail_table
    ''')
    s_l_a.registerTempTable('s_l_a_table')

    # 持仓分析图表数据
    industry_prefer = hqlContext.sql('''
        select t1.customer_no as customer_no,
               concat('[', 
                      concat_ws(',', 
                                collect_list(
                                            concat('{"name":"', t1.stock_type, 
                                                   '","val":"', t1.total_exchange_amount, 
                                                   '","data":{"date_list":[', t2.date_list, 
                                                   '],"stock_value":[', t2.val_list, 
                                                   '],"close_val":[]}}'
                                            )
                                )
                      ), 
                      ']'
               ) as stock_hold_analysis
        from 
        (
            select d.customer_no as customer_no,
                   d.stock_type as stock_type,
                   d.total_exchange_amount as total_exchange_amount,
                   d.rank as rank
            from 
            (
                select c.customer_no as customer_no,
                       c.stock_type as stock_type,
                       round(c.total_exchange_amount/10000, 4) as total_exchange_amount,
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
                where c.total_exchange_amount>0.0
            ) d
            where d.rank<=5
        ) t1
        left outer join 
        (
            select b.customer_no as customer_no,
                   b.stock_type as stock_type,
                   concat_ws(',', collect_list(b.init_date)) as date_list,
                   concat_ws(',', collect_list(string(b.stock_value))) as val_list
            from 
            (
                select a.customer_no as customer_no,
                       a.init_date as init_date,
                       a.stock_type as stock_type,
                       round(a.stock_value/10000, 4) as stock_value,
                       row_number() over (partition by a.customer_no,a.stock_type order by a.stock_value) as rn
                from 
                (
                select customer_no,
                       init_date,
                       nvl(stock_type, '其他') as stock_type,
                       sum(nvl(current_qty*market_close_price, 0.0)) as stock_value
                from ''' + db_name + '''.agg_cust_stock
                where part_date>=''' + six_months_ago + '''
                  and part_date<=''' + run_date + '''
                group by customer_no,
                         init_date,
                         stock_type
                ) a
                where a.stock_value>0.0
                order by customer_no,stock_type,init_date
            ) b
            group by b.customer_no,
                     b.stock_type
        ) t2
        on t1.customer_no=t2.customer_no
        and t1.stock_type=t2.stock_type
        group by t1.customer_no
    ''')
    industry_prefer.registerTempTable('industry_prefer_table')
    
    
    # 整合指标+标签
    res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               a.avg_hold_days as avg_hold_days,
               a.deal_model as deal_model,
               b.cycle_stable_level as cycle_stable_value,
               (case when b.cycle_stable_level is null then '炒股模式平稳度未知'
                     when b.cycle_stable_level>0.8 then '炒股模式平稳度高'
                     when b.cycle_stable_level>0.52 then '炒股模式平稳度中高'
                     when b.cycle_stable_level>0.2 then '炒股模式平稳度中'
                     else '炒股模式平稳度低'
               end) as cycle_stable_level,
               c.contri_level_value as contri_value,
               (case when c.contri_level_value is null then '缴费贡献度未知'
                     when c.contri_level_value>0.00502 then '缴费贡献度高'
                     when c.contri_level_value>0.00191 then '缴费贡献度中高'
                     when c.contri_level_value>0.00051 then '缴费贡献度中'
                     else '缴费贡献度低'
                end) as contri_level,
               c.contri_stab_level_value as contri_stab_value,
               (case when c.contri_stab_level_value is null then '缴费稳定度未知'
                     when c.contri_stab_level_value>0.00374 then '缴费稳定度高'
                     when c.contri_stab_level_value>0.00104 then '缴费稳定度中高'
                     when c.contri_stab_level_value>0.000271 then '缴费稳定度中'
                     else '缴费稳定度低'
                end) as contri_stab_level,
               c.up_rate as up_rate,
               (case when c.up_rate is null then '缴费上升占比未知'
                     when c.up_rate>0.5 then '缴费上升占比高'
                     when c.up_rate>0.455 then '缴费上升占比中高'
                     when c.up_rate>0.357 then '缴费上升占比中'
                     else '缴费上升占比低'
                end) as up_level,
               d.hold_risk_value as hold_risk_value,   
               (case when d.hold_risk_value is null then '平仓风险未知'
                     when d.hold_risk_value=-1 then '无负债'
                     when d.hold_risk_value>4 then '无平仓风险'
                     when d.hold_risk_value>3 then '低平仓风险'
                     when d.hold_risk_value>2 then '中等平仓风险'
                     when d.hold_risk_value>1.4 then '中高平仓风险'
                     else '高平仓风险'
                end) as hold_risk_level,
               e.avg_loss_rate as avg_loss_rate,
               e.control_loss_level as control_loss_level,
               f.stock_hold_analysis as stock_hold_analysis
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join stock_deal_model_table a
        on cust.customer_no=a.customer_no
        left outer join cycle_stable_table b
        on cust.customer_no=b.customer_no
        left outer join cust_worth_value_table c
        on cust.customer_no=c.customer_no
        left outer join hold_risk_level_table d
        on cust.customer_no=d.customer_no
        left outer join s_l_a_table e
        on cust.customer_no=e.customer_no
        left outer join industry_prefer_table f
        on cust.customer_no=f.customer_no
    ''')
    
    
    # 标签缺失补全
    res_all = res_all.na.fill({'cycle_stable_level': u'炒股模式平稳度未知', 
                               'contri_level': u'缴费贡献度未知',
                               'contri_stab_level': u'缴费稳定度未知',
                               'up_level': u'缴费上升占比未知',
                               'hold_risk_level': u'平仓风险未知',
                               'deal_model': u'炒股模式未知',
                               'control_loss_level': u'止损能力未知'})
    res_all.registerTempTable("res_all")
    
    
    # 写入hive物理表
    res_all.write.mode('overwrite').saveAsTable('tag_stat.invest_model')

    hqlContext.uncacheTable('stock_hold_date_table')
    hqlContext.uncacheTable('stock_prefer_detail_table')

    sc.stop()
