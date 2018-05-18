
# coding: utf-8


#!/usr/bin/env python

"""
input_table: dim_customer, schema_cust_analysis, schema_cust_view
output_table: basic_info
"""
# Li Mongmong
# 2017.08.08
# Gu Quan
# 2017.12.13
import sys
from pyspark import SparkContext, SparkConf, HiveContext
import datetime
from datetime import timedelta
import time
from pandas import Series
import pandas as pd


# 传入日期，与当前日期的时间差，返回新日期
def compute_date(raw_date, diff):
  format_date = datetime.datetime.strptime(raw_date, '%Y%m%d').date()
  delta = timedelta(days=diff)  # 计算时间差
  new_date = (format_date + delta).strftime("%Y%m%d")
  return new_date


if __name__ == "__main__":
  conf = SparkConf()
  sc = SparkContext()
  hqlContext = HiveContext(sc)

  db_name = ['ctprod', 'tag_stat']

  args = sys.argv[1:]
  if len(args) == 0:
    run_date = compute_date(time.strftime("%Y%m%d"), -1)
  else:
    run_date = args[0]

  six_month_ago = compute_date(run_date, -180)  # 6个月前的日期
  six_month_days = 180  # 6个月的时间间隔

  # 个人属性
  cust_df = hqlContext.sql('''
        select customer_no,
               customer_gender,
               nationality,
               birthday,
               FLOOR(datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), 
               concat_ws('-',substr(birthday,1,4), substr(birthday, 5,2), substr(birthday,7,2)))/365) as age,
               cast(substr(birthday,1,4) as bigint) as birthyear,
               nation_id,
               id_address,
               address,
               zipcode,
               profession_code,
               company,
               industry_type,
               work_date,
               degree_code
        from ''' + db_name[0] + '''.dim_customer''')
  cust_df.registerTempTable('cust_df_temp')

  # 资产现状
  fund_df = hqlContext.sql('''
        select a.customer_no as fund_customer_no,
               a.customer_income, 
               concat('现收入等级', a.customer_income_level) as customer_income_level,
               b.total_asset,
               b.total_debit,
               b.stock_value,
               a.asset_y,
               concat('现资产等级', a.asset_y_level) as asset_y_level,
               a.debit_y,
               concat('现负债等级', a.debit_y_level) as debit_y_level
               
        from(select customer_no,
                    customer_income,
                    customer_income_level,
                    asset_y,
                    asset_y_level,
                    debit_y,
                    debit_y_level
             from (select customer_no,
                          customer_income,
                          customer_income_level,
                          asset_y,
                          asset_y_level,
                          debit_y,
                          debit_y_level,
                          rank() over(partition by customer_no order by init_date desc) as rn
                   from ''' + db_name[0] + '''.schema_cust_analysis
                   where customer_no is not NULL) rnk
             where rn = 1)a
        full outer join 
            (select customer_no,
                    total_asset,
                    total_debit,
                    stock_value
             from(select customer_no,
                         total_asset,
                         total_debit,
                         stock_value,
                         rank() over(partition by customer_no order by init_date desc) as rn
                  from ''' + db_name[0] + '''.schema_cust_view
                  where customer_no is not NULL) rnk2 
             where rn = 1) b
        on a.customer_no = b.customer_no
    ''')

  cust = cust_df.join(fund_df, cust_df['customer_no'] == fund_df['fund_customer_no'], 'inner')
  cust = cust.drop('fund_customer_no').na.drop()
  cust.registerTempTable('cust_temp')

  # 指标到标签
  cust_tag = hqlContext.sql('''
        select customer_no,
               (case when customer_gender is null then '性别未知'
                     when customer_gender='男' then '男' 
                     when customer_gender='女' then '女' 
                     when customer_gender='非自然人' then '机构' 
                     else '性别未知' 
                end) as sex,
               (case when zipcode is null then '地区未知'
                     when substring(zipcode,1,2)='31' then '本省' 
                     when substring(zipcode,1,2)='32' then '本省' 
                     when length(zipcode)=5 then '外省' 
                     else '地区未知' 
                end) as place,
               (case when profession_code is null then '职业未知'
                     else profession_code 
                end) as profession_code,
               (case when degree_code is null then '学历未知'
                     else degree_code 
                end) as degree_code,
               (case when customer_income is null then '收入未知'
                     when customer_income>=10000000 then '收入1000万以上' 
                     when customer_income>=1000000 then '收入100万-1000万' 
                     else '收入100万以下' 
                end) as income_level,
               (case when birthyear is null then '年龄未知'
                     when birthyear>=1990 then '90后' 
                     when birthyear>=1980 then '80后' 
                     when birthyear>=1970 then '70后' 
                     when birthyear>=1960 then '60后' 
                     when birthyear>=1950 then '50后' 
                     else '50前' 
                end) as age,
               (case when total_asset is null or asset_y is null then '资产水平未知'
                     when (0.5*total_asset+0.5*asset_y)<=1000000 then '资产100万以下'
                     when (0.5*total_asset+0.5*asset_y)<=2000000 then '资产100万-200万'
                     when (0.5*total_asset+0.5*asset_y)<=5000000 then '资产200万-500万'
                     when (0.5*total_asset+0.5*asset_y)>=10000000 then '资产500万-1000万'
                     else '资产1000万以上' 
                end) as asset_level,
               (case when total_debit is null or debit_y is null then '负债水平未知'
                     when (0.5*total_debit+0.5*debit_y)>=10000000 then '负债1000万以上' 
                     when (0.5*total_debit+0.5*debit_y)>=5000000 then '负债500万-1000万' 
                     when (0.5*total_debit+0.5*debit_y)>=2000000 then '负债200万-500万' 
                     when (0.5*total_debit+0.5*debit_y)>1000000 then '负债100万-200万' 
                     else '负债100万以下' 
                end) as debt_level
        from cust_temp
    ''')
  cust_tag.registerTempTable('cust_tag_temp')

  # 整合指标+标签
  res_all = hqlContext.sql('''
        select cust.customer_no as customer_no,
               a.customer_gender as customer_gender,
               a.nationality as nationality,
               a.birthday as birthday,
               a.age as age,
               a.nation_id as nation_id,
               a.id_address as id_address,
               a.address as address,
               a.zipcode as zipcode,
               a.profession_code as profession,
               a.company as company,
               a.industry_type as industry_type,
               a.work_date as work_date,
               a.degree_code as degree,
               a.customer_income as customer_income, 
               a.customer_income_level as customer_income_level,
               a.total_asset as total_asset,
               a.total_debit as total_debit,
               a.stock_value as stock_value,
               a.asset_y as asset_y,
               a.asset_y_level as asset_y_level,
               a.debit_y as debit_y,
               a.debit_y_level as debit_y_level,
               b.sex as sex,
               b.place as place,
               b.age as age_tag,
               b.profession_code as profession_code,
               b.degree_code as degree_code,
               b.income_level as income_level,
               b.asset_level as asset_level,
               b.debt_level as debit_level
        from (select distinct customer_no 
              from ''' + db_name[0] + '''.dim_customer) cust
        left outer join cust_temp a
        on cust.customer_no = a.customer_no
        left outer join cust_tag_temp b
        on cust.customer_no = b.customer_no
    ''')

  # 标签缺失补全
  res_all = res_all.na.fill({'sex': u'性别未知',
                             'place': u'地区未知',
                             'age_tag': u'年龄未知',
                             'profession_code': u'职业未知',
                             'degree_code': u'学历未知',
                             'income_level': u'收入未知',
                             'asset_level': u'资产水平未知',
                             'debit_level': u'负债水平未知'})
  res_all.registerTempTable("res_all")

  hqlContext.sql('drop table if exists tag_stat.basic_info')
  hqlContext.sql('''
        create table ''' + db_name[1] + '''.basic_info as 
        select * from res_all
    ''')

  sc.stop()
