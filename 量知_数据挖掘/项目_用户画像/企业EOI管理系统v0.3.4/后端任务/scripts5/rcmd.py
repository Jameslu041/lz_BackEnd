# -*- coding:utf-8 -*-
from __future__ import division
from pyspark import SparkConf, SparkContext, HiveContext
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np
from copy import deepcopy
import pyfpgrowth
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


# 获取具有代表性的标签集
# items_df物品dataframe， min_sup最小支持度
# 单机版混合并行版
def get_representative_label(items_df, min_sup):
    tmp = np.array(items_df)
    data = tmp.tolist()
    patterns = pyfpgrowth.find_frequent_patterns(data, min_sup)  # dict，格式为{(1, 2): 4, (2,): 7}
    freq_items = patterns.keys()  # list，格式为[(1, 2), (2,), (1, 3), (2, 3), (1,)]

    if freq_items:  # 获取到了符合要求的频繁项集
        label_list = list()
        max_len = sorted(patterns.values())[-1]  # 频繁项集最大长度
        for k, v in patterns.items():
            if v == max_len:  # 符合长度要求的label集合
                label_list += k

        label_list = list(set(label_list))  # 去重

        # 删除空字符串
        if '' in label_list:
            label_list.remove('')

        label_str = '","'.join(label_list)  # 逗号拼接为字符串
        label_str = '"' + label_str + '"'  # 格式转为"tag1","tag2".....
        return label_str  # 返回标签字符串
    else:
        return '""'  # 返回空标签串


# 在cluster的size低于min_sup时特殊处理代表标签
# 返回第二common的标签集合（若存在）
def get_common_label(items_df):
    if items_df.shape[0] > 1:
        tmp_l = np.array(items_df).tolist()  # 格式：[['a', 'b', 'c'], ['a', 'b', 'd']]
        tmp_dict = dict()  # 存储每个标签的计数
        for l in tmp_l:
            for item in l:
                if item in tmp_dict.keys():
                    tmp_dict[item] += 1  # 计数加1
                else:
                    tmp_dict[item] = 1
        max_len = sorted(tmp_dict.values())[-1]  # 频繁项最大长度
        label_list = list()  # 存储标签集合
        for k, v in tmp_dict.items():
            if v == max_len:  # 符合长度要求的label集合
                label_list.append(k)

        # 删除空字符串
        if '' in label_list:
            label_list.remove('')

        label_str = '","'.join(label_list)  # 逗号拼接为字符串
        label_str = '"' + label_str + '"'  # 格式转为"tag1","tag2".....
        return label_str  # 返回最common标签字符串

    else:  # 只有一条数据，返回全部标签
        tmp = np.array(items_df).tolist()

        # 删除空字符串
        label_list = tmp[0]
        if '' in label_list:
            label_list.remove('')

        tags = '","'.join(label_list)
        tags = '"' + tags + '"'  # 格式转为"tag1","tag2"
        return tags


# index是customer_no
# target_customer_df是目标客户，计算子群客户与其相似性，type为df
def compute_customer_similar(target_customer_df, candidate_customer_df):
    t_id_list = target_customer_df.index  # 获取目标客户customer_no
    c_id_list = candidate_customer_df.index  # 获取候选客户customer_no

    # 计算客户新维度feature的相似度，返回相似度矩阵
    rs_matrix = np.mat(np.sqrt(np.sum((np.array(candidate_customer_df) - np.array(target_customer_df)) ** 2, axis=1)))

    return pd.DataFrame(rs_matrix, index=t_id_list, columns=c_id_list)  # 更新index和column，返回df


# 获取优质客户群及其相关信息
def get_high_quality_cluster(customer_no, qualified_list, indicator_df, label_df):
    # 单客户相似度计算专用
    min_sup_rate = 0.8  # 最小支持度
    min_num = 8  # 最小支持数

    # 热门板块，公司top专用
    cluster_min_sup_rate = 0.9  # 最小支持度
    cluster_min_num = 40  # 最小支持数

    if qualified_list and customer_no != '':
        entire_id_list = deepcopy(qualified_list)  # 深拷贝一份数据
        entire_id_list.append(customer_no)  # 加入目标客户的id，完整id列表

        entire_cluster_info = indicator_df[indicator_df.customer_no.isin(entire_id_list)]  # 子群所有信息，包括目标客户

        # 利用pca对数据进行降维，降低相似度计算量
        pca = PCA(n_components=0.9, copy=True, whiten=True)  # 降维后总方差占比90%以上，白化，方差为1

        # 降维后的数据，客户id列表作为index，entire_dim_feature包括目标客户
        entire_dim_feature = pd.DataFrame(pca.fit_transform(entire_cluster_info[entire_cluster_info.columns[1:]]),
                                          index=entire_id_list)
        new_target_info = entire_dim_feature.loc[[customer_no]]  # 目标客户新维度数据
        new_qualified_info = entire_dim_feature[entire_dim_feature.index.isin(qualified_list)]  # 符合条件新维度信息

        # 计算子群相似度
        similar_df = compute_customer_similar(new_target_info, new_qualified_info)  # 获取相似度df

        # 对相似值排序
        similar_sort = similar_df.loc[customer_no].sort_values()

        # 获取相似度排名前20的客户id列表
        similar_id_list = similar_sort[:50].index.tolist()

        # 获取相似客户的标签数据
        # 目前是返回“优质客户”的所有数据，根据需求变化，输出再作调整
        similar_candidate_tags = label_df[label_df.customer_no.isin(similar_id_list)]
        similar_high_quality = similar_candidate_tags[(similar_candidate_tags['total_profit_level'] == u'高盈利')]

        # 优质客户代表性标签
        # 将偏好行业拆分成3列，以便获取频繁项集
        similar_high_quality.set_axis(0, range(similar_high_quality.shape[0]))  # 对similar_high_quality重新index
        split_industry = pd.concat(
            [similar_high_quality, pd.DataFrame(similar_high_quality.stock_industry_n.str.split(',').tolist())], axis=1)
        split_industry.fillna('', inplace=True)  # 填充切割行业偏好可能产生的none

        # 删除不需要的列，保存customer_no信息列表
        cluster_id_list = split_industry['customer_no'].tolist()  # 获取客户id列表
        split_df = split_industry.drop(['customer_no', 'stock_industry_n'], axis=1)

        # 优质客户人数
        cluster_size = similar_high_quality.shape[0]

        # 获取具有代表性的标签集合
        # 结果格式: a,b,c,d，其中a、b等表示标签
        if split_df.shape[0] > 0:  # 优质cluster存在
            id_str = ','.join(cluster_id_list)  # 将客户id拼接为字符串

            min_sup_num = max(int(min_sup_rate * cluster_size), min_num)  # 计算出的最小支持数

            # 若最小支持数大于cluster的size，则特殊处理
            if min_sup_num > cluster_size:
                tags_str = get_common_label(split_df)
            else:
                tags_str = get_representative_label(split_df, min_sup_num)

            result_list = [customer_no, cluster_size, tags_str, id_str]
            return result_list
        else:
            return []

    else:  # 不存在候选客户群
        if customer_no != '':
            return []
        else:  # 未传入目标客户，公司top、热门板块
            # 优质客户代表性标签
            # 将偏好行业拆分成3列，以便获取频繁项集
            similar_high_quality = deepcopy(label_df)  # 深拷贝一份数据
            similar_high_quality.set_axis(0, range(similar_high_quality.shape[0]))  # 对similar_high_quality重新index
            split_industry = pd.concat(
                [similar_high_quality, pd.DataFrame(similar_high_quality.stock_industry_n.str.split(',').tolist())],
                axis=1)
            split_industry.fillna('', inplace=True)  # 填充切割行业偏好可能产生的none

            # 删除不需要的列，保存customer_no信息列表
            cluster_id_list = split_industry['customer_no'].tolist()  # 获取客户id列表
            split_df = split_industry.drop(['customer_no', 'stock_industry_n'], axis=1)

            # 资产水平相当的优质客户人数
            cluster_size = similar_high_quality.shape[0]

            # 获取具有代表性的标签集合
            # 结果格式: a,b,c,d，其中a、b等表示标签
            if split_df.shape[0] > 0:  # 优质cluster存在
                id_str = ','.join(cluster_id_list)  # 将客户id拼接为字符串

                min_sup_num = max(int(cluster_min_sup_rate * cluster_size), cluster_min_num)  # 计算出的最小支持数

                # 若最小支持数大于cluster的size，则特殊处理
                if min_sup_num > cluster_size:
                    tags_str = get_common_label(split_df)
                else:
                    tags_str = get_representative_label(split_df, min_sup_num)

                # 结果存入list中返回，便于构建dataframe
                # 返回最终结果：[客户id，子群人数，代表标签字符串，子群客户id]
                result_list = [customer_no, cluster_size, tags_str, id_str]
                return result_list
            else:
                return []


# 输入参数：([id1, 行业], [id2, 行业])
# 判断id1、id2是否有重合行业，有则返回id2，否则返回空字符串
# 传入被比较list，目标list
def get_x(c_list, t_list):
    indus_t = t_list[1].split(',')
    indus_c = c_list[1].split(',')

    count = 0  # 计数，统计重合行业的数量
    # 判断是否有重合行业
    for i in indus_t:
        if i in indus_c and i != '':
            count += 1
    if count >= 2:  # 有超过2个重合行业，则认为兴趣相同
        return c_list[0]  # 返回客户号
    else:
        return ''  # 返回空字符串


# 相似板块客群推荐
def get_similar_industry(cust_no, feature_df, tags_df):
    features = deepcopy(feature_df)  # 深拷贝数据
    # 删除了string型数据、填充nan后的全量数据
    features = features.fillna(features.mean(), axis=0)  # 以平均值填充nan

    # 所有客户的id和所在行业
    industry_df = tags_df[['customer_no', 'stock_industry_n']]

    # 目标客户的行业信息
    t_industry_df = industry_df[industry_df['customer_no'] == cust_no]
    # 转为list，单层list
    t_industry_data = (np.array(t_industry_df)).tolist()

    # 待分析的客户群体
    c_industry_df = industry_df[industry_df['customer_no'] != cust_no]
    # 转为list，嵌套list[[], []]
    c_industry_data = (np.array(c_industry_df)).tolist()

    if t_industry_data[0][1]:  # 目标客户存在行业数据
        qualified_id_list = list()  # 存储候选客户ID

        t_industry_data = t_industry_data[0]  # 将嵌套list转为单层list
        # 获取有共同行业的id
        for c_data in c_industry_data:
            if c_data[1]:
                id = get_x(c_data, t_industry_data)
                if id != '':
                    qualified_id_list.append(id)
    else:  # 处理异常情况
        qualified_id_list = []  # 无候选客户群

    # 返回最终结果：[客户id，子群人数，代表标签字符串，子群客户id]
    cluster_info = get_high_quality_cluster(cust_no, qualified_id_list, features, tags_df)

    # 返回结果
    if cluster_info:  # 有返回结果
        return cluster_info
    else:
        return [cust_no, 0, '""', '']


# 同资产等级客群推荐
# 每个客户都要出一份同样的数据
def get_similar_asset(cust_no, feature_df, tags_df):
    features = deepcopy(feature_df)  # 深拷贝数据
    # 删除了string型数据、填充nan后的全量数据
    features = features.fillna(features.mean(), axis=0)  # 以平均值填充nan

    # 获取跟目标用户属于同一个资产水平的子群
    target_info = features[features['customer_no'] == cust_no]  # 目标客户信息

    candidate_info = features[features['customer_no'] != cust_no]  # 不包含目标客户

    # 获取符合条件的子群客户id
    qualified_id_list = candidate_info[
        (
            (candidate_info['asset_y'] - target_info['asset_y'].tolist()[0]) / target_info['asset_y'].tolist()[
                0] >= -0.5) &
        ((candidate_info['asset_y'] - target_info['asset_y'].tolist()[0]) / target_info['asset_y'].tolist()[0] <= 1.0)][
        'customer_no'].tolist()

    # 返回最终结果：[客户id，子群人数，代表标签字符串，子群客户id]
    return get_high_quality_cluster(cust_no, qualified_id_list, features, tags_df)


# 热门板块客群推荐
def get_hot_industry(feature_df, tags_df):
    # # 所有客户的id和所在行业
    # industry_rdd = hqlContext.createDataFrame(tags_df[['customer_no', 'stock_industry_n']]).rdd  # 行业信息rdd
    #
    # # 行业：[客户1，客户2，客户3]，倒排索引保存为dict
    # inverted_index = industry_rdd.flatMapValues(lambda hy: hy.split(',')).map(lambda t: (t[1], t[0])).reduceByKey(
    #     lambda cid1, cid2: cid1 + ',' + cid2).map(lambda t: (t[0], t[1].split(','))).map(
    #     lambda t: (','.join(t[1]), len(t[1]))).collectAsMap()
    #
    # # 按照板块热度，获取最热板块客户id集合
    # hot_ids = sorted(inverted_index.iteritems(), key=lambda d: d[1], reverse=True)[0][0]
    # raw_qualified_id_list = hot_ids.split(',')  # 转为id列表，需要进一步过滤优质客户
    # raw_label_df = tags_df[tags_df.customer_no.isin(raw_qualified_id_list)]
    #
    # # 过滤优质客户
    # high_quality = raw_label_df[(raw_label_df['total_profit_level'] == u'高盈利')]
    # # 获取符合要求的客户id列表
    # qualified_id_list = high_quality['customer_no'].tolist()
    # qualified_indicators_df = feature_df[feature_df.customer_no.isin(qualified_id_list)]  # 最热门板块所有客户指标数据
    # qualified_labels_df = tags_df[tags_df.customer_no.isin(qualified_id_list)]  # 最热门板块所有客户指标数据
    #
    # # 返回最终结果：[客户id，子群人数，代表标签字符串，子群客户id]
    # return get_high_quality_cluster('', qualified_id_list, qualified_indicators_df, qualified_labels_df)

    # 获取高盈利客户集合
    hq_ids = tags_df[(tags_df['total_profit_level'] == u'高盈利')]['customer_no'].tolist()
    industry_rdd = hqlContext.createDataFrame(
        tags_df[tags_df.customer_no.isin(hq_ids)][['customer_no', 'stock_industry_n']]).rdd

    # 对热门行业排序
    inverted_index = industry_rdd.flatMapValues(lambda hy: hy.split(',')).map(lambda t: (t[1], 1)).reduceByKey(
        lambda x, y: x + y).collectAsMap()
    hot_hy = sorted(inverted_index.iteritems(), key=lambda d: d[1], reverse=True)

    # 临时存储行业标签
    tmp_hy_list = []
    for i in range(0, 10):
        tmp_hy_list.append('"' + hot_hy[i][0] + '"')

    id_str = ','.join(hq_ids)
    hy_str = ','.join(tmp_hy_list)
    return ['', len(hq_ids), hy_str, id_str]


# top客户推荐
def get_top(feature_df, tags_df):
    features = deepcopy(feature_df)  # 深拷贝数据
    features = features.fillna(features.mean(), axis=0)  # 以平均值填充nan
    top_n = features[['customer_no', 'profit_rate_year']]  # 年均收益率
    top_df = hqlContext.createDataFrame(top_n)
    top_df.registerTempTable('top_table')

    # 获取收益率公司内排名前50的客户id
    top_rdd = hqlContext.sql('''
        select customer_no
        from top_table
        order by profit_rate_year desc 
        limit 50
    ''').rdd
    qualified_id_list = top_rdd.map(lambda row: row['customer_no']).collect()  # 公司top50的客户id
    qualified_indicators_df = feature_df[feature_df.customer_no.isin(qualified_id_list)]  # 公司top20客户指标数据
    qualified_labels_df = tags_df[tags_df.customer_no.isin(qualified_id_list)]  # 公司top50客户标签数据

    # 返回最终结果：[客户id，子群人数，代表标签字符串，子群客户id]
    return get_high_quality_cluster('', qualified_id_list, qualified_indicators_df, qualified_labels_df)


# 寻找相似客户中的优质客户
if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext()
    hqlContext = HiveContext(sc)

    args = sys.argv[1:]  # 接受参数，目前只接数据库名, run_date两个参数，数据库名参数必须输入

    # 未传入任何参数，使用默认值
    if len(args) == 0:
        run_date = compute_date(time.strftime("%Y%m%d"), -1)  # 默认run_date
    else:
        run_date = args[0]

    ### 分区建表检查
    rcmd_cols = hqlContext.sql('desc tag_stat.rcmd').toPandas()
    if 'part_date' not in rcmd_cols.col_name.values:
        raise ValueError(u'错误！rcmd表不是分区表！')

        # 创建可能不存在的目标表
        # hqlContext.sql('''
        # create external table if not exists tag_stat.rcmd
        # (
        # customer_no string comment'客户id',
        # cust_rcmd string comment'客户推荐'
        # )
        # partitioned by (part_date varchar (8))
        # stored as parquet
    # ''')

    # 判断是否存在相应的分区
    partitions = hqlContext.sql('show partitions tag_stat.alldata').toPandas()
    if ('part_date=' + run_date) not in partitions.result.values:
        print(u'alldata不存在相应分区，无法运行脚本!')
    else:
        # 获取所有指标数据
        indicators = hqlContext.sql('''
            select customer_no,
                   loss_rate_value,
                   stock_selection_ability_value,
                   percep_in,
                   percep_out,
                   total_profit,
                   turnover_rate_year,
                   margin_freq,
                   profit_devo,
                   devo_ratio,
                   close_rate,
                   crdt_close_balance,
                   cust_activity,
                   stock_correlation,
                   fin_corr,
                   total_asset,
                   total_debit,
                   stock_value,
                   asset_y,
                   debit_y,
                   clever_ratio,
                   cust_beta,
                   risk_rate,
                   kehu,
                   shichang,
                   relative_index,
                   rate_relative,
                   rate_absolute,
                   profit_volatility_year_value,
                   sharp_ratio_value,
                   turnover_rate_value,
                   unit_risk_premium_rate_value,
                   stock_hold_ratio,
                   open_years,
                   stock_concentrate,
                   avg_hold_days,
                   cycle_stable_value,
                   contri_stab_value,
                   up_rate,
                   hold_risk_value,
                   avg_loss_rate,
                   profit_rate_year,
                   annual_profit
            from tag_stat.alldata
            where part_date = ''' + run_date + '''
              and profit_rate_year>=0.0 ''').toPandas()

        # 获取所有标签数据
        labels = hqlContext.sql('''
            select customer_no,
                   stock_industry_n,
                   index_level,
                   rate_level,
                   clever_level,
                   beta_level,
                   stock_hold_ratio_level,
                   open_years_level,
                   stock_concentrate_level,
                   income_level,
                   debit_level,
                   asset_level,
                   total_profit_level,
                   activity_level,
                   cust_devo_level,
                   close_rate_level,
                   profit_rate_year_level,
                   turnover_rate_label,
                   sharp_ratio_label,   
                   profit_volatility_year_label,
                   unit_risk_premium_rate_label,
                   stock_selection_ability_label,
                   percep_in_level,
                   percep_out_level,
                   stock_corr_level,
                   margin_corr_level,
                   hold_risk_level,
                   contri_level,
                   contri_stab_level,
                   up_level,
                   cycle_stable_level,
                   control_loss_level,
                   age_tag,
                   profession,
                   degree_code
            from tag_stat.alldata
            where part_date = ''' + run_date + '''
              and profit_rate_year>=0.0 ''').toPandas()

        # 资产等级
        # 对每个客户获取标签集合
        id_list = indicators['customer_no'].tolist()

        # 广播指标和标签数据
        b_indicators = sc.broadcast(indicators)
        b_labels = sc.broadcast(labels)

        # 返回的结果格式：[t_id, cluster_size, tags_str, id_str]
        interest_based_matrix = sc.parallelize(id_list).map(
            lambda cno: (str(cno), b_indicators.value, b_labels.value)).map(
            lambda t: get_similar_industry(t[0], t[1], t[2])).collect()

        # 基于相似资产水平，存储最终结果
        # 返回的结果格式：[[t_id, cluster_size, tags_str, id_str]]
        asset_based_matrix = sc.parallelize(id_list).map(
            lambda cno: (str(cno), b_indicators.value, b_labels.value)).map(
            lambda t: get_similar_asset(t[0], t[1], t[2])).collect()

        # 基于兴趣
        interest_based_df = hqlContext.createDataFrame(
            pd.DataFrame(interest_based_matrix, columns=['customer_no', 'cluster_size', 'tags', 'ids']))

        # 基于资产水平
        asset_based_df = hqlContext.createDataFrame(
            pd.DataFrame(asset_based_matrix, columns=['customer_no', 'cluster_size', 'tags', 'ids']))

        # 注册成表，便于后期的关联操作
        interest_based_df.registerTempTable('interest_based_table')
        asset_based_df.registerTempTable('asset_based_table')

        # 热门板块客户代表标签
        hot_based_labels = get_hot_industry(indicators, labels)
        hot_based_df = hqlContext.createDataFrame(pd.DataFrame([hot_based_labels],
                                                               columns=['customer_no', 'cluster_size', 'tags', 'ids']))
        hot_based_df.registerTempTable('hot_based_table')  # 注册成表

        # 公司top50客户代表标签
        top_based_labels = get_top(indicators, labels)
        top_based_df = hqlContext.createDataFrame(pd.DataFrame([top_based_labels],
                                                               columns=['customer_no', 'cluster_size', 'tags', 'ids']))
        top_based_df.registerTempTable('top_based_table')  # 注册成表

        # 优质客户推荐
        hqlContext.sql('''
            insert overwrite table tag_stat.rcmd
            partition (part_date=''' + run_date + ''')

            select a.customer_no as customer_no,
                   concat('{"customer_referral":[{', 
                          a.interest_tags, '},{', 
                          b.asset_tags, '},{', 
                          c.hot_tags, '},{', 
                          d.top_tags, '}', 
                          ']}'
                   ) as cust_rcmd
            from
            (
                select t1.customer_no as customer_no,
                       concat('"interest":[{"number":"', nvl(int(t2.cluster_size),0), 
                              '"},{"label":[', nvl(t2.tags, 
                              '""'), 
                              ']}]'
                       ) as interest_tags
                from 
                (
                    select customer_no
                    from tag_stat.alldata
                    where part_date = ''' + run_date + '''
                      and customer_no is not null
                      and trim(customer_no) != ''
                ) t1
                left outer join interest_based_table t2
                on t1.customer_no=t2.customer_no
            ) a
            left outer join
            (
                select t1.customer_no as customer_no,
                       concat('"assets_similar":[{"number":"', nvl(int(t2.cluster_size),0), 
                              '"},{"label":[', nvl(t2.tags, 
                              '""'), 
                              ']}]'
                       ) as asset_tags
                from 
                (
                    select customer_no
                    from tag_stat.alldata
                    where part_date = ''' + run_date + '''
                      and customer_no is not null
                      and trim(customer_no) != ''
                ) t1
                left outer join asset_based_table t2
                on t1.customer_no=t2.customer_no
            ) b
            on a.customer_no = b.customer_no
            left outer join
            (
                select customer_no,
                       concat('"hot_plate":[{"number":"', nvl(int(cluster_size),0), 
                              '"},{"label":[', nvl(tags, 
                              '""'), 
                              ']}]'
                       ) as hot_tags
                from hot_based_table
            ) c
            on 1=1
            left outer join
            (
                select customer_no,
                       concat('"company_top":[{"number":"', nvl(int(cluster_size),0), 
                              '"},{"label":[', nvl(tags, 
                              '""'), 
                              ']}]'
                       ) as top_tags
                from top_based_table
            ) d
            on 1=1 
        ''')

    sc.stop()


