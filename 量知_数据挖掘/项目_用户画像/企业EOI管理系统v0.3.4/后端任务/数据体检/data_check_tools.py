
# coding: utf-8

# In[2]:


import MySQLdb as msdb
import pandas as pd
import numpy as np
import pandas as pd
import sqlalchemy.types as sqltypes


# In[1]:


# 将config参数转化成list类型
def get_dataexe_config(target_tb):
    # 预处理，去空格
    target_tb = target_tb.split(',')
    target_tb = [i.replace(' ', '') for i in target_tb]
    target_tb = [i for i in target_tb if i!='']
    
    target_tb = [i.split('.') for i in target_tb if '.' in i]
    assert len(target_tb)#数据不能为空
    
    # 成员列表中元素必须是2或者3
    tgt_tmp = [len(i) for i in target_tb]
    assert False not in [True if i in range(3,6) else False for i in tgt_tmp]
    
    return target_tb


# In[3]:


#从hive中取出待体检的表并将其转化成pandas.DataFrame类型
def dataexe_generate_df(lst, limit=10, hqlContext=''):
    assert len(lst) in range(3,6)
    if len(lst) in [3,4]:
        rs = hqlContext.sql('''
            select * from %s.%s where part_date = %s limit %s
        ''' % (lst[0], lst[1], lst[2], limit))
    else:
        rs = hqlContext.sql('''
            select %s from %s.%s where part_date = %s limit %s
        ''' % (','.join(lst[4].split('|')), lst[0], lst[1], lst[2], limit))
        
    return rs.toPandas()


# In[4]:


#生成标准化列名字典
def generate_namelst(lst):
    return {u'库名':lst[0], u'表名':lst[1], u'分区':lst[2]}

#更新目标表最新时间
def update_createtime(lst, con):
    df = pd.read_sql('''select max(create_date) as max_cd from data_health_examination where db_name='%s' and table_name='%s' and part_date='%s' '''%(lst[0], lst[1], lst[2]), con)
    max_cd = df['max_cd'][0]
    cur = con.cursor()
    cur.execute('''
        delete from data_health_examination where db_name='%s' and table_name='%s' and part_date='%s' and create_date<>'%s'
    '''%(lst[0], lst[1], lst[2], max_cd))
    con.commit()
