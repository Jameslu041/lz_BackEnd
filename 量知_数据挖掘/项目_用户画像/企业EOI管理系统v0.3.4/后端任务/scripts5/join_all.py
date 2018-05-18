# coding:utf-8
#!/usr/bin/env python

# from __future__ import division
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
    
    part2_date = run_date

    # 本地测试：先手动指定日期
    # run_date = '20170301'
    # part2_date = '20170214'
    
    three_month_ago = compute_date(run_date, -90) # 三个月前的日期
    six_month_ago = compute_date(run_date, -180) # 六个月前的日期
    first_day_of_year = run_date[0:4] + '0101' # 年初日期
    
    ### 分区建表检查
    alldata_cols = hqlContext.sql('desc tag_stat.alldata').toPandas()
    if 'part_date' not in alldata_cols.col_name.values:
        raise ValueError(u'错误！alldata表不是分区表！')
    if False:
        hqlContext.sql('''
            create external table if not exists tag_stat.alldata
            (
                customer_no string,
                loss_rate_value double,
                loss_rate_label string,
                stock_selection_ability_value double,
                stock_selection_ability_label string,
                percep_in double,
                percep_out double,
                industry_profit string,
                industry_loss string,
                percep_in_level string,
                percep_out_level string,
                total_profit double,
                isprofit bigint,
                turnover_rate_year double,
                margin_freq double,
                profit_devo double,
                devo_ratio double,
                close_rate double,
                crdt_close_balance double,
                stock_corr_level string,
                margin_corr_level string,
                activity_level string,
                cust_activity double,
                cust_devo_level string,
                total_profit_level string,
                close_rate_level string,
                crdt_close_balance_level string,
                stock_correlation double,
                fin_corr double,
                customer_gender string,
                nationality string,
                birthday string,
                age bigint,
                nation_id string,
                id_address string,
                address string,
                zipcode string,
                profession string,
                company string,
                industry_type string,
                work_date double,
                degree_code string,
                customer_income double,
                customer_income_level string,
                total_asset double,
                total_debit double,
                stock_value double,
                asset_y double,
                asset_y_level string,
                debit_y double,
                debit_y_level string,
                sex string,
                place string,
                age_tag string,
                profession_code string,
                income_level string,
                asset_level string,
                debit_level string,
                total_num double,
                clever_num double,
                clever_ratio double,
                cust_beta double,
                risk_rate double,
                clever_level string,
                beta_level string,
                kehu double,
                shichang double,
                relative_index double,
                rate_relative double,
                rate_absolute double,
                rate_level string,
                index_level string,
                profit_volatility_year_value double,
                profit_volatility_year_label string,
                sharp_ratio_value double,
                sharp_ratio_label string,
                turnover_rate_value double,
                turnover_rate_label string,
                unit_risk_premium_rate_value double,
                unit_risk_premium_rate_label string,
                stock_hold_ratio double,
                stock_industry_n string,
                open_years double,
                stock_concentrate double,
                stock_hold_ratio_level string,
                open_years_level string,
                stock_concentrate_level string,
                avg_hold_days double,
                deal_model string,
                cycle_stable_level string,
                cycle_stable_value double,
                contri_level string,
                contri_value double,
                contri_stab_level string,
                contri_stab_value double,
                up_rate double,
                up_level string,
                hold_risk_value double,
                hold_risk_level string,
                control_loss_level string,
                avg_loss_rate double,
                stock_hold_analysis string,
                fin_times_year bigint,
                occur_balance_year double,
                avg_occur_balance double,
                avg_asset_year double,
                asset_change double,
                profit_rate_year double,
                profit_rate_year_level string,
                annual_profit double,
                loss_rate_rank int,
                stock_selection_ability_rank int,
                profit_volatility_year_rank int,
                sharp_ratio_rank int,
                turnover_rate_rank int,
                unit_risk_premium_rate_rank int,
                work_date_rank int,
                income_rank int,
                total_asset_rank int,
                asset_y_rank int,
                asset_rank int,
                total_debit_rank int,
                debit_y_rank int,
                debit_rank int,
                total_profit_rank int,
                devo_ratio_rank int,
                close_rate_rank int,
                crdt_close_balance_rank int,
                activity_rank int,
                margin_corr_rank int,
                stock_corr_rank int,
                percep_in_rank int,
                percep_out_rank int,
                index_rank int,
                relative_index_rank int,
                rate_rank int,
                rate_absolute_rank int,
                clever_rank int,
                beta_rank int,
                stock_hold_ratio_rank int,
                open_years_rank int,
                stock_concentrate_rank int,
                control_loss_rank int,
                hold_risk_rank int,
                up_rate_rank int,
                contri_stab_rank int,
                contri_value_rank int,
                cycle_stable_rank int,
                profit_rate_year_rank int,
                overall_value double,
                overall_rank int,
                customer_name string,
                branch_name string,
                fin_balance double,
                old_total_asset double,
                credit_assure_scale double,
                stock_hold double,
                concentrate double,
                stock_hold_array string,
                income_array string,
                credit_assure_scale_array string,
                in_balance double,
                out_balance double,
                rest_balance double,
                calendar_year string
            )
            partitioned by (part_date varchar(8))
            stored as parquet
        ''')
            

            
    ### alldata_part1部分
    # 只需将当天跑的各子表join即可  
    join_part1 = hqlContext.sql('''
        select cust.customer_no as customer_no,
               a.loss_rate_value as loss_rate_value,
               a.loss_rate_label as loss_rate_label,
               a.stock_selection_ability_value as stock_selection_ability_value,
               a.stock_selection_ability_label as stock_selection_ability_label,
               b.percep_in as percep_in,
               b.percep_out as percep_out,
               b.industry_profit as industry_profit,
               b.industry_loss as industry_loss,
               b.percep_in_level as percep_in_level,
               b.percep_out_level as percep_out_level,
               c.total_profit as total_profit,
               c.isprofit as isprofit,
               c.turnover_rate_year as turnover_rate_year,
               c.margin_freq as margin_freq,
               c.profit_devo as profit_devo,
               c.devo_ratio as devo_ratio,
               c.close_rate as close_rate,
               c.crdt_close_balance as crdt_close_balance,
               c.stock_corr as stock_corr_level,
               c.margin_corr as margin_corr_level,
               c.activity as activity_level,
               c.cust_activity as cust_activity,
               c.cust_devo_level as cust_devo_level,
               c.total_profit_level as total_profit_level,
               c.close_rate_level as close_rate_level,
               c.crdt_close_balance_level as crdt_close_balance_level,
               c.stock_correlation as stock_correlation,
               c.fin_corr as fin_corr,
               d.customer_gender as customer_gender,
               d.nationality as nationality,
               d.birthday as birthday,
               d.age as age,
               d.nation_id as nation_id,
               d.id_address as id_address,
               d.address as address,
               d.zipcode as zipcode,
               d.profession as profession,
               d.company as company,
               d.industry_type as industry_type,
               d.work_date as work_date,
               d.degree_code as degree_code,
               d.customer_income as customer_income, 
               d.customer_income_level as customer_income_level,
               d.total_asset as total_asset,
               d.total_debit as total_debit,
               d.stock_value as stock_value,
               d.asset_y as asset_y,
               d.asset_y_level as asset_y_level,
               d.debit_y as debit_y,
               d.debit_y_level as debit_y_level,
               d.sex as sex,
               d.place as place,
               d.age_tag as age_tag,
               d.profession_code as profession_code,
               d.income_level as income_level,
               d.asset_level as asset_level,
               d.debit_level as debit_level,
               e.total_num as total_num,
               e.clever_num as clever_num,
               e.clever_ratio as clever_ratio,
               e.cust_beta as cust_beta,
               e.risk_rate as risk_rate,
               e.clever_level as clever_level,
               e.beta_level as beta_level,
               f.kehu as kehu,
               f.shichang as shichang,
               f.relative_index as relative_index,
               f.rate_relative as rate_relative,
               f.rate_absolute as rate_absolute,
               f.rate_level as rate_level,
               f.index_level as index_level,
               g.profit_volatility_year_value as profit_volatility_year_value,
               g.profit_volatility_year_label as profit_volatility_year_label,
               g.sharp_ratio_value as sharp_ratio_value,
               g.sharp_ratio_label as sharp_ratio_label,
               g.turnover_rate_value as turnover_rate_value,
               g.turnover_rate_label as turnover_rate_label,
               g.unit_risk_premium_rate_value as unit_risk_premium_rate_value,
               g.unit_risk_premium_rate_label as unit_risk_premium_rate_label,
               h.stock_hold_ratio as stock_hold_ratio,
               h.stock_industry_n as stock_industry_n,
               h.open_years as open_years,
               h.stock_concentrate as stock_concentrate,
               h.stock_hold_ratio_level as stock_hold_ratio_level,
               h.open_years_level as open_years_level,
               h.stock_concentrate_level as stock_concentrate_level,
               i.avg_hold_days as avg_hold_days,
               i.deal_model as deal_model,
               i.cycle_stable_level as cycle_stable_level,
               i.cycle_stable_value as cycle_stable_value,
               i.contri_level as contri_level,
               i.contri_value as contri_value,
               i.contri_stab_level as contri_stab_level,
               i.contri_stab_value as contri_stab_value,
               i.up_rate as up_rate,
               i.up_level as up_level,
               i.hold_risk_value as hold_risk_value,
               i.hold_risk_level as hold_risk_level,
               i.control_loss_level as control_loss_level,
               i.avg_loss_rate as avg_loss_rate,
               i.stock_hold_analysis as stock_hold_analysis,
               j.fin_times_year as fin_times_year,
               j.occur_balance_year as occur_balance_year,
               j.avg_occur_balance as avg_occur_balance,
               j.avg_asset_year as avg_asset_year,
               j.asset_change as asset_change,
               j.profit_rate_year as profit_rate_year,
               j.profit_rate_year_level as profit_rate_year_level,
               j.annual_profit as annual_profit
        from (select distinct customer_no 
              from '''+db_name+'''.dim_customer) cust
        left outer join tag_stat.choose_loss a
        on cust.customer_no = a.customer_no
        left outer join tag_stat.market_perception b
        on cust.customer_no = b.customer_no
        left outer join tag_stat.margin_trading c
        on cust.customer_no = c.customer_no
        left outer join tag_stat.basic_info d
        on cust.customer_no = d.customer_no
        left outer join tag_stat.beta_chance e
        on cust.customer_no = e.customer_no
        left outer join tag_stat.market_info f
        on cust.customer_no = f.customer_no
        left outer join tag_stat.account_analysis g
        on cust.customer_no = g.customer_no
        left outer join tag_stat.invest_basic h
        on cust.customer_no = h.customer_no
        left outer join tag_stat.invest_model i
        on cust.customer_no = i.customer_no
        left outer join tag_stat.fin_cust_stat j
        on cust.customer_no = j.customer_no
    ''')
    join_part1.registerTempTable('join_part1_temp')
    
    alldata_part1 = hqlContext.sql('''
        select * ,
               rank() over(order by loss_rate_value desc) as loss_rate_rank,
               rank() over(order by stock_selection_ability_value desc) as stock_selection_ability_rank,
               rank() over(order by profit_volatility_year_value asc) as profit_volatility_year_rank,
               rank() over(order by abs(sharp_ratio_value) desc) as sharp_ratio_rank,
               rank() over(order by turnover_rate_value desc) as turnover_rate_rank,
               rank() over(order by unit_risk_premium_rate_value desc) as unit_risk_premium_rate_rank,
               rank() over(order by work_date desc) as work_date_rank,
               rank() over(order by customer_income desc) as income_rank,
               rank() over(order by total_asset desc) as total_asset_rank,
               rank() over(order by asset_y desc) as asset_y_rank,
               rank() over(order by 0.5*total_asset+0.5*asset_y desc) as asset_rank,
               rank() over(order by abs(total_debit) desc) as total_debit_rank,
               rank() over(order by abs(debit_y) desc) as debit_y_rank,
               rank() over(order by abs(0.5*total_debit+0.5*debit_y) desc) as debit_rank,
               rank() over(order by abs(total_profit) desc) as total_profit_rank,
               rank() over(order by devo_ratio desc) as devo_ratio_rank,
               rank() over(order by close_rate asc) as close_rate_rank,
               rank() over(order by crdt_close_balance desc) as crdt_close_balance_rank,
               rank() over(order by cust_activity desc) as activity_rank,
               rank() over(order by fin_corr desc) as margin_corr_rank,
               rank() over(order by stock_correlation desc) as stock_corr_rank,
               rank() over(order by percep_in desc) as percep_in_rank,
               rank() over(order by percep_out desc) as percep_out_rank,
               rank() over(order by abs(kehu) desc) as index_rank,
               rank() over(order by relative_index desc) as relative_index_rank,
               rank() over(order by abs(rate_relative) desc) as rate_rank,
               rank() over(order by rate_absolute desc) as rate_absolute_rank,
               rank() over(order by clever_ratio desc) as clever_rank,
               rank() over(order by abs(cust_beta) desc) as beta_rank,
               rank() over(order by stock_hold_ratio desc) as stock_hold_ratio_rank,
               rank() over(order by open_years desc) as open_years_rank,
               rank() over(order by stock_concentrate desc) as stock_concentrate_rank,
               rank() over(order by avg_loss_rate desc) as control_loss_rank,
               rank() over(order by hold_risk_value desc) as hold_risk_rank,
               rank() over(order by up_rate desc) as up_rate_rank,
               rank() over(order by contri_stab_value desc) as contri_stab_rank,
               rank() over(order by contri_value desc) as contri_value_rank,
               rank() over(order by cycle_stable_value desc) as cycle_stable_rank,
               rank() over(order by abs(profit_rate_year) desc) as profit_rate_year_rank
        from join_part1_temp
    ''')
    alldata_part1.registerTempTable('alldata_part1_temp')
    
    alldata_part1_2 = hqlContext.sql('''
        select *,
               (0.3*profit_rate_year_rank + 
                0.15*stock_selection_ability_rank + 
                0.2*clever_rank + 
                0.15*stock_hold_ratio_rank + 
                0.2*loss_rate_rank) as overall_value
        from alldata_part1_temp
    ''')
    alldata_part1_2.registerTempTable('alldata_part1_temp2')
    alldata_part1_3 = hqlContext.sql('''
        select *,
               rank() over(order by overall_value asc) as overall_rank
               from alldata_part1_temp2
    ''')
    alldata_part1_3.registerTempTable('alldata_part1_temp3')
    
    
    calendar_year = hqlContext.sql('''
        select t3.customer_no as customer_no,
               concat('{"calendar_year":[', concat_ws(',', collect_list(concat('{"year":"', int(t3.year), '","average_daily_financing":"', t3.avg_fin, '","average_daily_assets":"', t3.avg_asset, '","profits":"', t3.total_profit, '","interest":"', t3.interest, '","annual_turnover":"', t3.turnover_rate_year, '"}'))), ']}') as calendar_year
        from ''' + db_name + '''.schema_cust_view_sub04 t3
        group by t3.customer_no
    ''')
    calendar_year.registerTempTable('calendar_year_table')
    
    
    
    ### alldata_part2部分
    # 自然日对接到交易日，只针对fact表等于的情况
    date_transfer = hqlContext.sql('''
        select jyr
        from ''' + db_name + '''.sys_calendar 
        where zrr=''' + run_date + '''
    ''').toPandas()
    market_day = str(date_transfer['jyr'][0])  # 获取到传入日期对应的交易日
    
    # ##########################################################stock_hold_array
    agg_cust_stock_df = hqlContext.sql('''
        select customer_no,
               stock_no,
               stock_name,
               nvl(stock_concentrate,0.0) as stock_concentrate,
               nvl(current_qty,0.0) as current_qty,
               nvl(cost_price,0.0) as cost_price,
               round(nvl(stock_profit/10000,0.0),2) as stock_profit
        from '''+db_name+'''.agg_cust_stock 
        where part_date='''+run_date
    )
    agg_cust_stock_df.registerTempTable('agg_cust_stock_table')
    
    fact_stock_market_detail_df = hqlContext.sql('''
        select trim(stock_no) as stock_no,
               open_price,
               close_price
        from '''+db_name+'''.fact_stock_market_detail
        where init_date='''+market_day
    )
    fact_stock_market_detail_df.registerTempTable('fact_stock_market_detail_table')
    
    stock_hold_df = hqlContext.sql('''
        select t.customer_no as customer_no,
               t.concentrate as concentrate,
               t.total_stock_value/10000 as total_stock_value,
               concat('[',t.stock_hold_array,']') as stock_hold_array
        from 
        (
            select a.customer_no as customer_no,
                   a.concentrate as concentrate,
                   round(a.total_stock_value_one+nvl(b.total_stock_value_two,0.0),2) as total_stock_value,
                   (case when b.customer_no is null
                         then a.stock_hold_array
                         else concat(a.stock_hold_array,',',b.stock_hold_array)
                    end) as stock_hold_array
            from 
            (
            select t4.customer_no as customer_no,
                   max(t4.ratio_of_total) as concentrate,
                   sum(nvl(t4.current_amount,0.0)) as total_stock_value_one,
                   concat_ws(',',collect_list(concat('{',t4.name,',',t4.value,',"attributes":{',t4.stock_num,',',t4.stock_balance,',',t4.stock_cost,',',t4.stock_income,'}}'))) as stock_hold_array
                   
            from
            (  
                select t1.customer_no as customer_no,
                       t1.stock_no as stock_no,
                       round((t1.current_qty*nvl(t1.cost_price,0.0)),2) as current_amount,
                       concat('"name":"',t1.stock_name,'"') as name,
                       concat('"value":"',nvl(round((t1.current_qty*nvl(t1.cost_price,0.0))/t3.total_stock,4),0.0),'"') as value,
                       concat('"持仓股数量":"',t1.current_qty,'"') as stock_num,
                       concat('"成本市值":"',round(t1.current_qty*t1.cost_price/10000,2),'万元"') as stock_cost, 
                       concat('"持仓市值":"',round(t1.current_qty*nvl(t2.close_price,0.0)/10000,2),'万元"') as stock_balance,
                       concat('"股票收益":"',round(t1.stock_profit,2),'万元"') as stock_income,
                       t1.stock_concentrate as ratio_of_total,
                       nvl((t1.current_qty*t1.cost_price)/t3.total_stock,0.0) as ratio_of_stock
                from agg_cust_stock_table t1
                left outer join
                (select stock_no,
                        open_price,
                        close_price
                from fact_stock_market_detail_table
                ) t2
                on t1.stock_no=t2.stock_no
                left outer join 
                (select customer_no,
                        sum(current_qty*cost_price) as total_stock
                from agg_cust_stock_table
                group by customer_no
                ) t3
                on t1.customer_no=t3.customer_no
            ) t4
            where t4.ratio_of_stock>0.01
            group by t4.customer_no
            ) a
            
            left outer join
            (
                select t5.customer_no as customer_no,
                       t5.total_stock_value_two*10000 as total_stock_value_two,
                       concat('{"name":"其他"',',"value":"',t5.value,'","attributes":{','"持仓股数量":"',t5.stock_num,'","持仓市值":"',t5.stock_balance,'万元","成本市值":"',t5.stock_cost,'万元","收益":"',t5.stock_income,'万元"}}') as stock_hold_array
            
                from
                (
                select t4.customer_no as customer_no,
                       sum(t4.stock_cost) as total_stock_value_two,
                       round(sum(nvl(t4.ratio_of_stock,0.0)),4) as value,
                       sum(t4.current_qty) as stock_num,
                       round(sum(t4.stock_balance),4) as stock_balance,
                       round(sum(t4.stock_cost),4) as stock_cost,
                       round(sum(t4.stock_income),4) as stock_income
                from
                (  
                    select t1.customer_no as customer_no,
                           t1.current_qty as current_qty, 
                           round((t1.current_qty*t1.cost_price)/10000,2) as stock_cost,  
                           round((t1.current_qty*nvl(t2.close_price,0.0))/10000,2) as stock_balance, 
                           t1.stock_profit as stock_income,
                           t1.stock_concentrate as ratio_of_total,
                           nvl(round((t1.current_qty*t1.cost_price)/t3.total_stock,4),0.0) as ratio_of_stock
                    from agg_cust_stock_table t1
                    left outer join
                    (select stock_no,
                            open_price,
                            close_price
                    from fact_stock_market_detail_table
                    ) t2
                    on t1.stock_no=t2.stock_no
                    left outer join 
                    (select customer_no,
                            sum(current_qty*cost_price) as total_stock
                    from agg_cust_stock_table
                    group by customer_no
                    ) t3
                    on t1.customer_no=t3.customer_no
                ) t4
                where t4.ratio_of_stock<=0.01
                group by customer_no
                ) t5
            ) b       
            on a.customer_no=b.customer_no
        ) t
    ''')
    stock_hold_df.registerTempTable('stock_hold_table')
    
    
    
    # #############################################################income_array
    profit_sum_detail=hqlContext.sql('''
        select customer_no,
               init_date,
               round(sum(total_profit) over (partition by customer_no order by init_date),2) as profit_day_sum,
               row_number() over (partition by customer_no order by init_date) as rn
        from '''+db_name+'''.agg_cust_balance
        where init_date>='''+first_day_of_year+'''
          and init_date<='''+run_date+'''
    ''')
    profit_sum_detail.registerTempTable("profit_sum_detail")
    
    
    
    # 近一年的收益曲线，每天的的收益累加值曲线
    income_array_df = hqlContext.sql('''
        select c.customer_no as customer_no,
               concat('{',c.income_array,'}') as income_array,
               nvl(d.profit_day_sum,0.0) as profit_day_sum
        from 
        (
            select b.customer_no as customer_no,
                   concat_ws(',',collect_list(concat('"',b.init_date,'":"',b.profit_day_sum,'"'))) as income_array
            from 
            (
                select a.customer_no as customer_no,
                       a.init_date as init_date,
                       a.profit_day_sum as profit_day_sum,
                       a.rn as rn
                from profit_sum_detail a
            ) b
            group by b.customer_no
        ) c
        left outer join 
        (
            select a.customer_no as customer_no,
                   a.init_date as init_date,
                   a.profit_day_sum as profit_day_sum
            from profit_sum_detail a
            where a.init_date='''+run_date+'''
        ) d
        on c.customer_no=d.customer_no
    ''')
    income_array_df.registerTempTable('income_array_table')
    
    # #################################################################credit_assure_scale_array
    credit_assure_scale_df = hqlContext.sql('''
        select t1.customer_no as customer_no,
               concat_ws(',',collect_list(concat('"',t1.init_date,'":"',t1.credit_assure_scale,'"'))) as credit_assure_scale_array
        from 
        (
            select init_date as init_date,
                   customer_no as customer_no,
                   nvl(close_rate,0.0) as credit_assure_scale
            from '''+db_name+'''.agg_cust_balance
            where init_date>='''+six_month_ago+''' 
              and init_date<='''+run_date+'''
            order by customer_no,
                     init_date
            ) t1
        group by t1.customer_no
    ''')
    credit_assure_scale_df.registerTempTable('credit_assure_scale_table')
    
    # 计算原schema_cust_view_sub02表
    schema_cust_view_sub02 = hqlContext.sql('''
        select customer_no,
               inout_type,
               sum(abs(occur_balance)) as occur_balance
        from (select customer_no,
                    (case when exchange_type in ('专户资金划入','专户资金存','银行转存') then '转入'
                          else '转出'
                     end) as inout_type,
                     occur_balance
              from ''' + db_name + '''.fact_cust_fund_detail
              where part_date >= ''' + three_month_ago + ''' and
                    part_date <= ''' + run_date + ''' and
                    exchange_type in ('专户资金划入','专户资金划出','专户资金取','专户资金存','银行转取','银行转存')) a
        group by customer_no,
                 inout_type
    ''')
    schema_cust_view_sub02.registerTempTable('schema_cust_view_sub02_table')
    
    # 计算原schema_cust_view_sub表
    schema_cust_view = hqlContext.sql('''
        select a.customer_no as customer_no,
               a.customer_name as customer_name,
               a.branch_name as branch_name,
               b.fin_balance as fin_balance,
               b.total_asset as total_asset,
               b.credit_assure_scale as credit_assure_scale,
               nvl(b.stock_hold, 0.0) as stock_hold,
               nvl(c.concentrate, 0.0) as concentrate,
               d.stock_hold_array as stock_hold_array,
               e.income_array as income_array,
               f.credit_assure_scale_array as credit_assure_scale_array
        from (select customer_no,
                     customer_name,
                     branch_name
              from ''' + db_name + '''.dim_customer) a
        left outer join
             (select customer_no,
                     nvl(fin_close_balance/10000, 0.0) as fin_balance,
                     nvl(total_close_asset/10000, 0.0) as total_asset,
                     nvl(close_rate, 0.0) as credit_assure_scale,
                     stock_close_value/total_close_asset as stock_hold
              from ''' + db_name + '''.agg_cust_balance
              where part_date = ''' + run_date + ''') b
        on a.customer_no = b.customer_no
        left outer join
             (select customer_no,
                     max(nvl(stock_concentrate, 0.0)) as concentrate
              from ''' + db_name + '''.agg_cust_stock
              where part_date = ''' + run_date + '''
              group by customer_no) c
        on a.customer_no = c.customer_no
        left outer join stock_hold_table d
        on a.customer_no = d.customer_no
        left outer join income_array_table e
        on a.customer_no = e.customer_no
        left outer join credit_assure_scale_table f
        on a.customer_no = f.customer_no
    ''')
    schema_cust_view.registerTempTable('schema_cust_view_table')
    
    part2 = hqlContext.sql('''
        select t1.customer_no,
               t1.customer_name,
               t1.branch_name,
               t1.fin_balance as fin_balance,
               t1.total_asset as old_total_asset,
               t1.credit_assure_scale as credit_assure_scale,
               t1.stock_hold as stock_hold,
               t1.concentrate as concentrate,
               t1.stock_hold_array as stock_hold_array,
               t1.income_array as income_array,
               t1.credit_assure_scale_array as credit_assure_scale_array,
               nvl(t2.in_balance, 0.0) as in_balance,
               nvl(t2.out_balance, 0.0) as out_balance,
               nvl(t2.rest_balance, 0.0) as rest_balance,
               nvl(t3.calendar_year, '') as calendar_year
        from schema_cust_view_table t1
        left outer join 
        (
            select a.customer_no as customer_no,
                   a.occur_balance as in_balance,  
                   nvl(b.occur_balance, 0.0) as out_balance,  
                   a.occur_balance-nvl(b.occur_balance, 0.0) as rest_balance  
            from
            (
                select customer_no,
                       inout_type,
                       occur_balance
                from schema_cust_view_sub02_table
                where inout_type='转入'
            ) a
            left outer join 
            (
                select customer_no,
                       inout_type,
                       occur_balance
                from schema_cust_view_sub02_table
                where inout_type='转出'
            ) b
            on a.customer_no=b.customer_no
        ) t2
        on t1.customer_no=t2.customer_no
        left outer join calendar_year_table t3
        on t1.customer_no=t3.customer_no
    ''')
    part2.registerTempTable('alldata_part2_temp')
    
    
    ### 写入分区
    alldata = hqlContext.sql('''
        select p1.*,
               p2.customer_name,
               p2.branch_name,
               p2.fin_balance,
               p2.old_total_asset,
               p2.credit_assure_scale,
               p2.stock_hold,
               p2.concentrate,
               p2.stock_hold_array,
               p2.income_array,
               p2.credit_assure_scale_array,
               p2.in_balance,
               p2.out_balance,
               p2.rest_balance,
               p2.calendar_year
        from alldata_part1_temp3 p1
        left outer join alldata_part2_temp p2
        on p1.customer_no = p2.customer_no
    ''')
    
    # 标签缺失补全
    alldata = alldata.na.fill({'customer_name': u'姓名未知',
                               'branch_name': u'所属营业部未知',
                               'fin_balance': 0,
                               'credit_assure_scale': 0,
                               'stock_hold': 0,
                               'concentrate': 0,
                               'stock_hold_array': '',
                               'income_array': '',
                               'credit_assure_scale_array': '',
                               'in_balance': 0, 
                               'out_balance': 0,
                               'rest_balance': 0,
                               'calendar_year': ''})
    alldata.registerTempTable('alldatatemp')
    
    hqlContext.sql('''
        insert overwrite table tag_stat.alldata
        partition (part_date=''' + run_date + ''')
        select * from alldatatemp
    ''')
    
    
    sc.stop()