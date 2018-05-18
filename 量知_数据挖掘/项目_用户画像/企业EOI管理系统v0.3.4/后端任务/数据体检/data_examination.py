#coding:utf-8
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import mpld3
import time
import codecs
import json
import re
import pandas as pd
import numpy as np
import matplotlib
import seaborn as sns
import sqlalchemy
import MySQLdb as msdb
from datetime import datetime as dt
from jinja2 import Environment, FileSystemLoader
import sqlalchemy.types as sqltypes
from IPython import get_ipython
from collections import Counter

# 开启inline绘图
#get_ipython().magic(u'matplotlib inline')
 

# 关闭inline绘图(下端代码和上面不能同时出现，否则会出现no display name and no $DISPLAY environment variable)
#%matplotlib auto
#plt.ion()
#plt.ioff()


# In[2]:


def data_health_examination(df, rules=''):
    outlier_param = 2.5

    pat_rep = pd.DataFrame({'pattern': ['how_much', 'length', 'format', 'line_phone', 'mobile_phone',
                                        'form1:only_num', 'form2:only_letter', 'form3:only_num_or_letter',
                                        'form4:exclude_strange_symbol'],
                            "repl": [u'数值大小范围', u'字段长度范围', u'字段格式规定', u'座机电话检测', u'手机号检测',
                                     u'仅包括数字', u'仅包括字母', u'仅包含字母和数字', u'不包含特殊符号']})
    # 判断字段类型
    def get_col_types(df):
        nrow = len(df) #表记录数
        col_types = {} #类型分类字典
        for col in df.columns:
            if df[col].dtype in ['float','float64']: #如果是int呢？
                col_types[col] = 'numeric'
            else:
                len_vals = len(df[col].unique())
                if (len_vals <= 20) and (len_vals <= 0.1*nrow): #即小于20又小于89？
                    col_types[col] = 'category'
                else:
                    col_types[col] = 'character'
        return col_types #返回各个字段以及对应的类型（分类操作）
    
    col_types = get_col_types(df)
    num_cols = [col for col in df.columns if col_types[col]=='numeric']  #列表解析，节省系统空间！
    cate_cols = [col for col in df.columns if col_types[col]=='category']
    
    # 统计缺失值个数及比例
    def na_stat(df, other_na=['NULL', 'null','Null','NA','']):
        nrow = len(df)
        pattern = '|'.join(['^'+s+'$' for s in other_na])
        res = {col: {'num':0, 'prop':0.0, 'num_other':0, 'prop_other':0.0} for col in df.columns}
        for col in df.columns:
            res[col]['num'] = sum(df[col].isnull())
            res[col]['prop'] = res[col]['num'] / float(nrow)
            if df[col].dtype not in ['int','int64','float','float64']:
                try:
                    col_val = df[col].astype('str').str.strip()
                except UnicodeEncodeError:
                    col_val = df[col].str.strip()
                temp = col_val.str.match(pattern)
                res[col]['num_other'] = sum(temp[temp.notnull()])#加个notnull保险点
                res[col]['prop_other'] = res[col]['num_other'] / float(nrow)
        return res
    
    # 数值型字段异常值检测
    # 使用箱线图标准
    def outlier_detect(df, iqr_multi=outlier_param, num_cols=num_cols):
        nrow = len(df)
        assert num_cols is not None
        assert len(num_cols) >= 1 #只有为真才能继续执行下去，否则会报错
        outlier = {col:{'num':0, 'prop':0.0, 'index':'[]'} for col in num_cols} 
        for col in outlier.keys():
            col_val = df[col][df[col].notnull()]
            # quantile函数其实会自动剔除缺失值
            q1, q3 = col_val.quantile([0.25, 0.75]) 
            iqr = q3 - q1
            up, down = q3+iqr_multi*iqr, q1-iqr_multi*iqr
            col_out = col_val[(col_val<down) | (col_val>up)]
            if len(col_out) > 0:
                outlier[col]['num'] = len(col_out)
                outlier[col]['prop'] = len(col_out) / float(nrow)
                outlier[col]['index'] = str(col_out.index.values.tolist())
        return outlier
    
    # 数值型字段的概率分布图及统计量
    def num_plot(df, num_cols=num_cols, figsize=[8,5]):
        assert num_cols is not None
        assert len(num_cols) >= 1
        dist_plot = {col:{'stat':0, 'plot':None} for col in num_cols} 
        sns.set()
        for col in num_cols:
            col_val = df[col][df[col].notnull()]
            dist_plot[col]['stat'] = col_val.describe()
            if dist_plot[col]['stat']['max'] > dist_plot[col]['stat']['min']:
                dist_plot[col]['plot'] = plt.figure(figsize=figsize)
                ax = dist_plot[col]['plot'].add_subplot(111)#?
                sns.distplot(col_val)
        return dist_plot

    # 类别型字段的频数分布图
    def cate_plot(df, cate_cols=cate_cols, figsize=[8,5]):
        assert cate_cols is not None
        assert len(cate_cols) >= 1
        count_plot = {col:{'count':0, 'plot':None} for col in cate_cols}
        sns.set()
        for col in cate_cols:
            col_val = df[col][df[col].notnull()]
            count_plot[col]['count'] = col_val.value_counts()
            if len(count_plot[col]['count']) >= 2:
                count_plot[col]['plot'] = plt.figure(figsize=figsize)
                ax = count_plot[col]['plot'].add_subplot(111)
                sns.countplot(col_val)
        return count_plot
    
    # 中文规则显示
    def cn_rules(rules_list):
        assert type(rules_list) == list
        def en_to_cn(string):
            for i in range(len(pat_rep)):
                string = re.sub(pat_rep.pattern[i], pat_rep.repl[i], string)
            return string
        rules_cn = [''] * len(rules_list)
        for i in range(len(rules_list)):
            rule = en_to_cn(rules_list[i]).split(', ')
            if len(rule) > 4:
                rules_cn[i] = u'库名：'+rule[0]+u', 表名：'+rule[1]+u', 字段名：'+rule[2]+u', '+rule[3]+u'：'+rule[4]
            else:
                rules_cn[i] = u'库名：'+rule[0]+u', 表名：'+rule[1]+u', 字段名：'+rule[2]+u', '+rule[3]
        return rules_cn

    # 规则检测
    def rules_detect(df, rules_list):
        assert type(rules_list) == list
        assert len(rules_list) > 0

        def one_rule_detect(df, rule):
            rule_content = rule.split(', ')
            col = rule_content[2]
            assert col in df.columns
            rule_type = rule_content[3]
            rule_detail = rule_content[4] if len(rule_content) > 4 else None #拆分规则

            if rule_type == 'how_much':
                assert df[col].dtype in ['int','int64','float','float64']
                min_num = float(rule_detail.split('-')[0])
                max_num = float(rule_detail.split('-')[1])
                col_val = df[col][df[col].notnull()] #做检验前都要做一遍去空
                out = (col_val<min_num) | (col_val>max_num)  #out可以看成是个索引，通过该索引带入DataFrame表筛出目标记录

            elif rule_type == 'length':
                min_len = float(rule_detail.split('-')[0])
                max_len = float(rule_detail.split('-')[1])
                col_val = df[col][df[col].notnull()]
                try:
                    col_val = col_val.astype('str')
                except UnicodeEncodeError:
                    pass
                col_len = col_val.apply(len)
                out = (col_len<min_len) | (col_len>max_len)

            elif rule_type == 'format':
                col_val = df[col][df[col].notnull()]
                try:
                    col_val = col_val.astype('str')
                except UnicodeEncodeError:
                    pass
                # 寻找任意非数字
                if "form1" in rule_detail:
                    out = col_val.str.contains('[^\d]')
                # 寻找任意非字母
                elif "form2" in rule_detail:
                    out = col_val.str.contains('[^a-zA-Z]')
                # 寻找任意非数字或字母
                elif "form3" in rule_detail:
                    out = col_val.str.contains('[^a-zA-Z0-9]')
                # 寻找任意非数字或字母或汉字或下划线(特殊字符)
                elif "form4" in rule_detail:
                    out = col_val.str.contains(u'[^\u4e00-\u9fff0-9a-zA-Z_]')
                else:
                    raise ValueError(u'该字符串格式暂不支持检测')

            elif rule_type == 'line_phone':
                col_val = df[col][df[col].notnull()]
                try:
                    col_val = col_val.astype('str')
                except UnicodeEncodeError:
                    pass
                match = col_val.str.match('^\d{4}-{0,1}\d{7}$')
                out = (match==False)

            elif rule_type == 'mobile_phone':
                col_val = ''
                match = col_val.str.match('^\d{11}$')
                out = (match==False)

            else:
                raise ValueError(u'未知检测类型')

            out_indices = out[out].index.tolist()
            res = {'pass':None, 'index':[], 'num':0, 'prop':0.0}
            res['num'] = len(out_indices)
            res['prop'] = res['num'] / float(len(df))
            res['pass'] = u'通过检测' if res['num']==0 else u'未通过检测' #有点意思
            res['index'] = [] if res['num']==0 else out_indices #！
            return res

        all_res = []
        for rule in rules_list:
            all_res.append(one_rule_detect(df=df, rule=rule))
        return all_res #成员为检查结果字典的list
    
    # 字段类型、缺失值统计、异常值检测
    col_types_df = pd.DataFrame({u'字段名': df.columns,
                                 u'字段类型': [col_types[col] for col in df.columns]})

    na_stat_result = na_stat(df)
    na_stat_df = pd.DataFrame(na_stat_result).T
    na_stat_df = na_stat_df[['num','prop','num_other','prop_other']]
    na_stat_df.columns = [u'缺失值数量', u'缺失值占比', u'其他缺失值数量', u'其他缺失值占比']
    na_stat_df[u'字段名'] = na_stat_df.index.tolist()

    if len(num_cols)!=0:
        outlier_result = outlier_detect(df)
        outlier_result_df = pd.DataFrame(outlier_result).T
        outlier_result_df.columns = [u'异常值位置', u'异常值数量', u'异常值占比']
        outlier_result_df[u'字段名'] = outlier_result_df.index.tolist()
    else:
        outlier_result_df = pd.DataFrame(columns=[u'字段名', u'异常值位置', u'异常值数量', u'异常值占比'])

    col_diagnosis_df = col_types_df.merge(na_stat_df, how='left', on=u'字段名').\
        merge(outlier_result_df, how='left', on=u'字段名')
    
    # 数值型字段
    if len(num_cols)!=0:
    #if False:
        num_plot_res = num_plot(df)
        num_stat_json = {col: num_plot_res[col]['stat'].to_json() for col in num_cols} #只有字符串才能输出到html上面
        num_plot_html = {col: mpld3.fig_to_html(num_plot_res[col]['plot']) #mpld3.fig_to_html这玩意儿估计自带绘图功能
                         for col in num_cols if num_plot_res[col]['plot'] is not None} 
        num_stat_json = pd.DataFrame(num_stat_json, index = ['probability_plot_result']).T
        num_plot_html = pd.DataFrame(num_plot_html, index = ['probability_plot_script']).T

        num_if_exist = {col: 0 for col in df.columns}
        for k in num_if_exist.keys():
            if k in num_cols:
                num_if_exist[k] = 1
        num_if_exist = pd.DataFrame(num_if_exist, index = ['if_exist_probability_plot']).T
        num_if_exist = num_if_exist.reset_index().rename(columns = {'index':u'字段名'}) #添加字段是否是概率图

        num_stat_json = num_stat_json.reset_index().rename(columns = {'index':u'字段名'})
        num_plot_html = num_plot_html.reset_index().rename(columns = {'index':u'字段名'})
        col_diagnosis_df = col_diagnosis_df.merge(num_stat_json, how = 'left', on = u'字段名').merge(num_plot_html, how = 'left', on = u'字段名').\
        merge(num_if_exist, how = 'left', on = u'字段名')
    else:
        num_plot_df = pd.DataFrame(columns=[u'字段名', u'if_exist_probability_plot', 
                                            u'probability_plot_result', u'probability_plot_script'])
        num_plot_df['if_exist_probability_plot'] = 0
        col_diagnosis_df = col_diagnosis_df.merge(num_plot_df, how = 'left', on = u'字段名')
    
    # 类别型字段
    if len(cate_cols)!=0:
    #if False:
        cate_plot_res = cate_plot(df)
        cate_stat_json = {col: codecs.decode(cate_plot_res[col]['count'].to_json(), 'unicode_escape')
                          for col in cate_cols}
        cate_plot_html = {col: mpld3.fig_to_html(cate_plot_res[col]['plot']) 
                          for col in cate_cols if cate_plot_res[col]['plot'] is not None}

        cate_stat_json = pd.DataFrame(cate_stat_json, index = ['frequency_plot_result']).T.reset_index().rename(columns = {'index':u'字段名'})
        cate_plot_html = pd.DataFrame(cate_plot_html, index = ['frequency_plot_script']).T.reset_index().rename(columns = {'index':u'字段名'})
        col_diagnosis_df = col_diagnosis_df.merge(cate_stat_json, how = 'left', on = u'字段名').merge(cate_plot_html, how = 'left', on = u'字段名')

        cate_if_exist = {col:0 for col in df.columns}
        for k in cate_if_exist.keys():
            if k in cate_cols:
                cate_if_exist[k] = 1
        cate_if_exist = pd.DataFrame(cate_if_exist, index = ['if_exist_frequency_plot']).T.reset_index().rename(columns = {'index':u'字段名'})
        col_diagnosis_df = col_diagnosis_df.merge(cate_if_exist, how = 'left', on = u'字段名')
    else:
        cate_plot_df = pd.DataFrame(columns=[u'字段名', u'if_exist_frequency_plot', 
                                            u'frequency_plot_result', u'frequency_plot_script'])
        cate_plot_df['if_exist_frequency_plot'] = 0
        col_diagnosis_df = col_diagnosis_df.merge(cate_plot_df, how = 'left', on = u'字段名')
    
    # 规则检测
    if rules!='':
        rules_cn = cn_rules(rules)
        rules_detect_res = rules_detect(df, rules)
        for item in rules_detect_res:
            item['index'] = [float(i) for i in item['index']]
        rules_detect_json = [codecs.decode(json.dumps(r), 'unicode_escape') for r in rules_detect_res]# dumps将所有类型转化成字符串
        
        # 若有重复，merge一下
        merge_rules(rules_cn)
        merge_rules(rules, rules_detect_json)
        
        rules_detect_df = {u'中文显示':pd.Series(rules_cn)
                          ,u'英文显示':pd.Series(rules)
                          ,u'最后结果':pd.Series(rules_detect_json)}
        rules_detect_df = pd.DataFrame(rules_detect_df)
        rules_detect_df[u'字段名'] = [item.split(',')[2].strip() for item in rules]
        
        if_has_rules = {col:0 for col in df.columns}
        for k in if_has_rules.keys():
            if k in rules_detect_df[u'字段名'].tolist():
                if_has_rules[k] = 1
        if_has_rules = pd.DataFrame(if_has_rules, index = ['if_exist_rules']).T.reset_index().rename(columns = {'index':u'字段名'})
    else:
        rules_detect_df = pd.DataFrame(columns=[u'字段名', u'中文显示', u'英文显示', u'最后结果'])
        if_has_rules = pd.DataFrame(columns=[u'字段名', 'if_exist_rules'])
        
    col_diagnosis_df = col_diagnosis_df.merge(rules_detect_df, how = 'left', on = u'字段名').merge(if_has_rules, how = 'left', on = u'字段名')
    
    col_diagnosis_df = col_diagnosis_df.rename(columns={u'字段名':'field_name', u'字段类型':'field_type', 
                          u'缺失值数量':'missing_value_num', u'缺失值占比':'missing_value_prop',
                          u'其他缺失值数量':'other_missing_value_num', u'其他缺失值占比':'other_missing_value_prop', 
                          u'异常值位置':'abnormal_value_index', u'异常值数量':'abnormal_value_num', 
                          u'异常值占比':'abnormal_value_prop', u'中文显示':'show_Chn_rules', 
                          u'最后结果':'rules_result', u'英文显示':'show_Eng_rules'})
    col_diagnosis_df = col_diagnosis_df[['field_name', 'field_type',
           'missing_value_num', 'missing_value_prop', 
           'other_missing_value_num', 'other_missing_value_prop', 
           'abnormal_value_index', 'abnormal_value_num', 'abnormal_value_prop',
           'if_exist_probability_plot', 'probability_plot_result', 'probability_plot_script',
           'if_exist_frequency_plot', 'frequency_plot_result', 'frequency_plot_script',
           'if_exist_rules', 'show_Chn_rules', 'show_Eng_rules', 'rules_result']]
    
    return col_diagnosis_df


# In[3]:


def normalize_health_examination(df, name_lst = {u'库名':np.nan, u'表名':np.nan,u'分区':np.nan}):
    # 判断输入是否规范
    assert isinstance(df, pd.DataFrame)
    assert isinstance(name_lst, dict)#name_lst字典存放库名，表名，分区名
    
    db_name = name_lst[u'库名']
    tb_name = name_lst[u'表名']
    part_date = name_lst[u'分区']
    
    # 加上库名，表名，分区三列
    df_hthexe = df.copy()
    df_hthexe['db_name'] = db_name
    df_hthexe['table_name'] = tb_name
    df_hthexe['part_date'] = part_date
    
    # 打上时间戳
    df_hthexe['create_date'] = dt.now()
    
    # 给列名排序
    df_hthexe = df_hthexe[['db_name']+['table_name']+['part_date']+['create_date']+df.columns.tolist()]
    
    return  df_hthexe
    
    
    


# In[4]:


def script_normalize(df):
    assert 'probability_plot_script' in df.columns.tolist()
    assert 'frequency_plot_script' in df.columns.tolist()
    
    df['frequency_plot_script'] = [item if len(str(item))<2**16-1 else 'too long to show' for item in df['frequency_plot_script']]
    df['probability_plot_script'] = [item if len(str(item))<2**16-1 else 'too long to show' for item in df['probability_plot_script']]
    return df


# In[16]:


def wirte_to_mysqldb(df, result_tb = 'data_health_examination', user='root', \
                     psw='liangzhi123', host='192.168.1.22', db='datamining', \
                     if_exists='append', dtype={u'db_name':sqltypes.NVARCHAR(length=255),
                                                 u'table_name':sqltypes.NVARCHAR(length=255), 
                                                 u'part_date':sqltypes.NVARCHAR(length=255), 
                                                 u'create_date':sqltypes.DateTime(), 
                                                 u'field_name':sqltypes.NVARCHAR(length=255), 
                                                 u'field_type':sqltypes.NVARCHAR(length=255), 
                                                 u'missing_value_num':sqltypes.BigInteger(), 
                                                 u'missing_value_prop':sqltypes.Float(), 
                                                 u'other_missing_value_num':sqltypes.BigInteger(), 
                                                 u'other_missing_value_prop':sqltypes.Float(), 
                                                 u'abnormal_value_index':sqltypes.Text(), 
                                                 u'abnormal_value_num':sqltypes.BigInteger(), 
                                                 u'abnormal_value_prop':sqltypes.Float(), 
                                                 u'if_exist_probability_plot':sqltypes.Integer(), 
                                                 u'probability_plot_result':sqltypes.NVARCHAR(length=255), 
                                                 u'probability_plot_script':sqltypes.Text(), 
                                                 u'if_exist_frequency_plot':sqltypes.Integer(), 
                                                 u'frequency_plot_result':sqltypes.NVARCHAR(length=255), 
                                                 u'frequency_plot_script':sqltypes.Text(), 
                                                 u'if_exist_rules':sqltypes.Integer(), 
                                                 u'show_Chn_rules':sqltypes.NVARCHAR(length=255), 
                                                 u'show_Eng_rules':sqltypes.NVARCHAR(length=255), 
                                                 u'rules_result':sqltypes.NVARCHAR(length=255)}):
    engine = sqlalchemy.create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?%s")\
                                      % (user, psw, host, db, 'charset=utf8'))
    df.to_sql(result_tb, engine, if_exists=if_exists, index=False, dtype=dtype)


# 将重复的规则合并
def merge_rules(rules, rules_detect_json=''):
    #将同字段的rules合为一个
    def merge(rules, start_idx, length):
        result = []
        pattern_dict = {}
        rules_dict = {}
        rules_cn_str = u'规则:'
        tgt_lst = rules[start_idx].split(', ')
        for i in range(3):
            result.append(tgt_lst[i])

        if len(tgt_lst)==5:
            #将pattern都添加到一个字典中并转化为json类型
            for i in range(length):
                tgt_lst = rules[start_idx+i].split(', ')
                pattern_dict['pattern_'+str(i+1)] = tgt_lst[3]
            #将rules都添加到rules_dict中并转化成json类型
            for i in range(length):
                tgt_lst = rules[start_idx+i].split(', ')
                rules_dict['rule_'+str(i+1)] = tgt_lst[4]

            result.append(json.dumps(pattern_dict))
            result.append(json.dumps(rules_dict))
        elif len(tgt_lst)==4:
            #将rules都添加到一个字典中并转化为json类型
            for i in range(length):
                tgt_lst = rules[start_idx+i].split(', ')
                rules_cn_str += ' '+tgt_lst[3]
            result.append(rules_cn_str)
        else:
            raise ValueError('Invalid Length')

        #调整rules的结构
        rules[start_idx] = ', '.join(result)
        del_items = []
        for i in range(1, length):
            del_items.append(rules[start_idx+i])
        for item in del_items:
            rules.remove(item)
    
    # 融合检测结果
    def merge_rules_detect_result(rules_detect_json, start_idx, length):
        detect_result = ''
        del_items = []

        for i in range(start_idx, length+start_idx):
            detect_result+=rules_detect_json[i]+'|'

        for i in range(start_idx+1, length+start_idx):
            del_items.append(rules_detect_json[i])

        rules_detect_json[start_idx] = detect_result

        for item in del_items:
            rules_detect_json.remove(item)

    rules_fields = []
    need_merge_dict = {}
    for item in rules:
        rules_fields.append(item.split(', ')[2])
    for item in rules_fields:
        rule_counts = Counter(rules_fields)[item]
        if rule_counts>1:
            need_merge_dict[item] = rule_counts
    
    for item in need_merge_dict.keys():
        rules_fields = []
        for i in rules:
            rules_fields.append(i.split(', ')[2])
        merge(rules, rules_fields.index(item), need_merge_dict[item])
        
        # 如果有规则检测结果rules_detect_json，merge一下
        if rules_detect_json != '':
            merge_rules_detect_result(rules_detect_json, rules_fields.index(item), need_merge_dict[item])
    return rules 



