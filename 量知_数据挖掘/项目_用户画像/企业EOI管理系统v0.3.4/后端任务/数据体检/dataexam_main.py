
# coding: utf-8

# In[1]:


import data_examination as dte
import data_check_tools as dct
import MySQLdb as msdb
import pandas as pd
import numpy as np
import json

# In[1]:


def data_examination_main(str_list, result_tb = 'data_health_examination', rules='', if_exists='append', rows_num=10000, hqlContext='', host = '192.168.1.22', user = 'root', passwd = 'liangzhi123', db = 'datamining',  port = 3306, charset="utf8"):
    # 预处理输入
    lst = dct.get_dataexe_config(str_list)
    con=msdb.connect(host ,user, passwd, db, port, charset)
    """
    fe: 存在未使用参数
    
    """
    cur = con.cursor()
    cur = con.cursor(cursorclass = msdb.cursors.DictCursor)
    
    for item in lst:
        # 从hive里面拿表
        df = dct.dataexe_generate_df(item, rows_num, hqlContext)

        # 体检
        rules_df = pd.read_sql('''
            select * from examination_rules_new
        ''', con)
        if len(item)<=3 or len(item) in [4,5] and item[3] =='0' or not (item[0] in rules_df['db_name'].tolist() and item[1] in rules_df['table_name'].tolist()):
            df_result = dte.data_health_examination(df)
        else:
            rules = generate_rules(item, con, rules_tb='examination_rules_new')
            detect_fields = [i.strip() for i in item[4].split('|')]
            dlt_fld_in_rules = []
            for i in rules:
                if i.split(',')[2].strip() not in detect_fields:
                    dlt_fld_in_rules.append(i)
            for i in dlt_fld_in_rules:
                del rules[rules.index(i)]
            assert rules!=[]
            df_result = dte.data_health_examination(df, rules)

        # 标准化参数
        namelist = dct.generate_namelst(item)

        # 给结果标准化
        df_result = dte.normalize_health_examination(df_result, namelist)

        # **将超大字符串除掉（待修改）
        dte.script_normalize(df_result)

        # 写入数据库
        dte.wirte_to_mysqldb(df_result, result_tb=result_tb, host=host, user=user, psw=passwd, db=db, if_exists=if_exists)


# In[67]:


def generate_rules_new(lst, con, rules_tb='examination_rules'):
    rules = pd.read_sql('''
        select * from '''+rules_tb+''' where db_name='%s' and table_name='%s'
    '''%(lst[0], lst[1]), con)
    if len(rules)==0:
        return ''
    assert False not in [True if i==1 else False for i in rules['field_name'].value_counts().tolist()] #确保每条规则对应一个规则
    
    rules = rules[(rules['db_name']==lst[0])|(rules['table_name']==lst[1])]
    rules_lst=[]
    for i in range(len(rules)):
        rules_lst.append('%s, %s, %s, %s, %s'%(rules[i:i+1].iat[0,0], rules[i:i+1].iat[0,1], rules[i:i+1].iat[0,2], rules[i:i+1].iat[0,3], rules[i:i+1].iat[0,4]))
    return rules_lst

def generate_rules(lst, con, rules_tb='examination_rules'):
    rules = pd.read_sql('''
        select * from '''+rules_tb+''' where db_name='%s' and table_name='%s'
    '''%(lst[0], lst[1]), con)
    rules = rules[(rules['db_name']==lst[0])|(rules['table_name']==lst[1])]
    
    rules_lst=[]
    for i in range(len(rules)):
        rules_items = json.loads(rules[i:i+1].iat[0,3])
        rules_len = len(rules_items)
        assert rules_len>=1
        for j in rules_items:
            rules_lst.append('%s, %s, %s, %s, %s'%(rules[i:i+1].iat[0,0], rules[i:i+1].iat[0,1], rules[i:i+1].iat[0,2], j['pattern'], j['rules']))
    return rules_lst

#从mysql库中取参
def read_datatst_parameter_old(para_table = "examination_data_health_parameter", host = '192.168.1.22' ,user = 'root', passwd = 'liangzhi123', db = 'datamining',  port = 3306, charset="utf8"):
    
    para_str = []
    
    # 连接mysql数据库
    con = msdb.connect(host = host, user = user, passwd = passwd, db = db,  port = port, charset=charset)
    cur = con.cursor()
    cur = con.cursor(cursorclass = msdb.cursors.DictCursor)
    
    para_df = pd.read_sql('''
        select * from ''' + para_table, con)
    para_df = para_df.fillna('null')
    
    #将paratmeter表中前五列拿出来
    for i in range(len(para_df)):
        tmp = [para_df[i:i+1].iat[0,j] for j in range(5)]
        tmp.append(para_df[i:i+1].iat[0,7])
        para_str.append(tmp)
    
    #去尾
    def del_tails(lst):
        for i in range(len(lst)):
            assert lst[-1]!='null'
            if lst[-2]=='null':
                del lst[-2]
            else:
                break
        return lst
    
    para_df = [del_tails(item) for item in para_str]
    # 将float转化成int
    para_df = [[item if not isinstance(item, float) else int(item) for item in i] for i in para_df]
    
    # list合成str
    para_df = ['.'.join([str(i) for i in item]) for item in para_df]
    return para_df

def read_datatst_parameter(con, para_table = "examination_data_health_parameter"):
    
    para_str = []
    
    # 连接mysql数据库
    #con = msdb.connect(host = host, user = user, passwd = passwd, db = db,  port = port, charset=charset)
    cur = con.cursor()
    
    # 把所有status为1的数据置为-2（中间状态）
    cur.execute('''
        update %s set parameter_status=-2 where parameter_status=1
    '''%para_table)
    con.commit()
    
    #拿出所有status为-2的数据
    para_df = pd.read_sql('''
        select * from %s where parameter_status=-2
    '''%para_table, con)
    con.commit()
    
    # 将所有-2状态置为2（运行中）
    cur.execute('''
        update %s set parameter_status=2 where parameter_status=-2
    '''%para_table)
    con.commit()
    
    para_df = para_df.fillna('null')
    
    #将paratmeter表中前五列拿出来
    for i in range(len(para_df)):
        tmp = [para_df[i:i+1].iat[0,j] for j in range(5)]
        tmp.append(para_df[i:i+1].iat[0,7])
        para_str.append(tmp)
    
    #去尾
    def del_tails(lst):
        for i in range(len(lst)):
            assert lst[-1]!='null'
            if lst[-2]=='null':
                del lst[-2]
            else:
                break
        return lst
    
    para_df = [del_tails(item) for item in para_str]
    # 将float转化成int
    para_df = [[item if not isinstance(item, float) else int(item) for item in i] for i in para_df]
    
    # list合成str
    para_df = ['.'.join([str(i) for i in item]) for item in para_df]
    
    cur.close()
    return para_df

# 体检完毕后根据结果将status状态标记为相应的状态：3存在体检，4输入参数有误无法生成体检表
def change_status_to_tg_status(failed_lst, tg_status, con, tb_name='examination_data_health_parameter'):
    # 补充null字段
    def regain_tails(lst):
        # 去尾
        lst_tmp = [item.split('.')[:-1] for item in lst]

        result = []
        for item in lst_tmp:
            item = item+['null' for i in range(5-len(item))]
            result.append(item)
        [i.append(lst[result.index(i)].split('.')[-1]) for i in result]
        #for i in range(len(result)):
        #    result[i] = [item if item<>'null' else np.nan for item in result[i]]

        return result
    
    lst = regain_tails(failed_lst)
    lst_rst = []
    
    for item in lst:
        lst_tmp = [' = '+"'"+i+"'" if i is not 'null' else ' is '+i for i in item]
        lst_rst.append(lst_tmp)
    
    cur = con.cursor()
    
    for item in lst_rst:
        cur.execute('''
            update %s set parameter_status=%d 
            where db_name%s and table_name%s and part_date%s and if_exist_rules%s and field_name%s and create_date%s
        '''%(tb_name, tg_status, item[0], item[1], item[2], item[3], item[4], item[5]))
    con.commit()
    
    return pd.read_sql('''select * from %s'''%tb_name, con)

# 分析某表体检数据
def examination_analyze(target_tb, con):
    # 拿出指定表最新的体检内容
    def fetch_nearest_test_result(target_tb, con):
        import data_check_tools as dct
        lst = dct.get_dataexe_config(target_tb)[0]
        df = pd.read_sql('''
            select * from examination_data_health where db_name='%s' and table_name='%s' and part_date='%s'
        '''%(lst[0], lst[1], lst[2]), con)

        return df[df['create_date'] == df['create_date'].max()]

    # 无规则分析
    def if_eixts_abnormal_data_at_nonerules(dttst_result):
        # 判断一个列表是否异常
        def if_exits_abnormal(item, tsd_value, bound):
            try:
                if item==0:
                    return False
                elif abs(item-tsd_value)<bound:
                    return False
                else:
                    return True
            except:
                raise Exception("abnormal_lst must be a numric type") 


        # 获取因字段异常未通过的key
        dttst_result[u'abnormal_value_prop'] = dttst_result[u'abnormal_value_prop'].fillna(0)#去空，空值为无异常
        abnormal_lst = [[dttst_result.loc[i]['db_name'], dttst_result.loc[i]['table_name'], dttst_result.loc[i]['part_date'], dttst_result.loc[i]['field_name']] \
                        for i in dttst_result.index if if_exits_abnormal(dttst_result.loc[i]['abnormal_value_prop'], 0, 0.1)]

        # 获取因空值未通过的key
        dttst_result['missing_value_prop'] = dttst_result['missing_value_prop'].fillna(0)
        dttst_result['other_missing_value_prop'] = dttst_result['other_missing_value_prop'].fillna(0)
        null_lst = [[dttst_result.loc[i]['db_name'], dttst_result.loc[i]['table_name'], dttst_result.loc[i]['part_date'], dttst_result.loc[i]['field_name']]\
                    for i in dttst_result.index if if_exits_abnormal(dttst_result.loc[i]['missing_value_prop']+dttst_result.loc[i]['other_missing_value_prop'], 0, 0.1)]

        return {'abnormal_lst':abnormal_lst, 'null_lst':null_lst}

    # 有规则分析
    def if_eixts_abnormal_data_at_rulesdetect(dttst_result):
        # 判断结果是否全部通过
        def resolution_result(rst_str):
            result_lst = []
            rst_str = rst_str.split('|')
            rst_str = [item for item in rst_str if item!='']
            for item in rst_str:
                try:
                    if json.loads(item)['pass']==u'未通过检测':
                        result_lst.append(False)
                    elif json.loads(item)['pass']==u'通过检测':
                        result_lst.append(True)
                    else:
                        result_lst.append(False)
                except:
                    result_lst.append(False)

            if False in result_lst:
                return False
            return True

        # 返回所有规则检测未通过的输入信息
        dttst_result = dttst_result[dttst_result['rules_result'].notnull()]
        ab_lst = [[dttst_result.loc[i]['db_name'], dttst_result.loc[i]['table_name'], dttst_result.loc[i]['part_date'], dttst_result.loc[i]['field_name']] \
         for i in dttst_result.index if not resolution_result(dttst_result.loc[i]['rules_result'])]
        return ab_lst
    
    # 参数定义
    rules_analyze = []
    abnormal_analyze = []
    null_analyze = []
    # 生成待分析的体检数据
    con.commit()
    dttst_result = fetch_nearest_test_result(target_tb, con)
    
    # 如果有规则检测，判断规则检测是否pass
    rules_analyze = if_eixts_abnormal_data_at_rulesdetect(dttst_result)
    none_rules_dict = if_eixts_abnormal_data_at_nonerules(dttst_result)
    abnormal_analyze = none_rules_dict['abnormal_lst']
    null_analyze = none_rules_dict['null_lst']
    
    return {'rules_analyze':rules_analyze ,'abnormal_analyze':abnormal_analyze ,'null_analyze':null_analyze}

