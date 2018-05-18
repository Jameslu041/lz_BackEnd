
# coding: utf-8

# In[1]:


import MySQLdb as msdb
import pandas as pd
import numpy as np
import data_check_tools as dct
import data_examination as dte
import dataexam_main as dm

from pyspark import SparkConf, SparkContext, HiveContext
conf = SparkConf()
sc = SparkContext(conf = conf)
hqlContext = HiveContext(sc)


# In[42]:


# 根据参数列表循环体检
def run_data_tst(para_lst):
    success_lst = []# 跑成功的key
    failed_lst = []# 跑失败的key
    for item in para_lst:
        
        #去掉时间戳
        item_tmp = item.split('.')
        del item_tmp[-1]
        item_tmp = '.'.join(item_tmp)
        
        try:
            dm.data_examination_main(item_tmp, result_tb='examination_data_health',hqlContext=hqlContext, host = '192.168.1.20' , user = 'root', passwd = 'liangzhi123', db = 'dmp')
            success_lst.append(item)
        except:
            failed_lst.append(item)
            
    return {'success_lst':success_lst, 'failed_lst':failed_lst}

# 生成分析结果，这里需要做个预处理，除掉最后的时间戳
"""
fe: 参数有误.
"""
def examination_analyze(success_lst, con):
    analyze_lst = []
    for item in rst_dict['success_lst']:
        item = [i for i in item.split('.')[:-1]]
        item = '.'.join(item)
        analyze_lst.append(dm.examination_analyze(item, con))
    return analyze_lst

# 更新体检检测结果
def update_abnormal_fields(analyze_lst, con):
    # 处理其中一个异常
    def update_item(lst, con, tb_name='examination_abnormal_fields', abnormal_level=u'高', abnormal_detail=u'无'):
        for item_lst in lst:
            con.commit()
            # 保证每个KEY一天只有一个数据
            try:
                cur.execute('''
                    delete from %s 
                    where exam_date=curdate() and db_name='%s' and tbl_name='%s' and part_cols='%s' and field_name='%s'
                '''%(tb_name, item_lst[0], item_lst[1], item_lst[2], item_lst[3]))
                con.commit()
            except:
                pass

            cur.execute('''
                insert into %s (exam_date, db_name, tbl_name, part_cols, field_name, abnormal_level, abnormal_detail)
                values (curdate(), '%s', '%s', '%s', '%s', '%s', '%s')
            '''%(tb_name, item_lst[0], item_lst[1], item_lst[2], item_lst[3], abnormal_level, abnormal_detail))
            con.commit()
                
    cur = con.cursor()
    
    for item_dict in analyze_lst:
        # 如果规则检测有异常，直接判断为异常
        if len(item_dict['rules_analyze'])!=0:
            update_item(item_dict['rules_analyze'], con, abnormal_detail=u'规则检测未通过')
        elif len(item_dict['abnormal_analyze'])!=0:
            update_item(item_dict['abnormal_analyze'], con, abnormal_detail=u'异常值太多')
        else:
            update_item(item_dict['null_analyze'], con, abnormal_detail=u'缺失值太高')
            


# In[44]:


# 连接mysql数据库
con = msdb.connect(host = '192.168.1.22' ,user = 'root', passwd = 'liangzhi123', db = 'datamining',  port = 3306, charset="utf8")
cur = con.cursor()
cur = con.cursor(cursorclass = msdb.cursors.DictCursor)

# 读参
para_lst = dm.read_datatst_parameter(con=con)

# 体检
rst_dict = run_data_tst(para_lst)

# 修改状态
dm.change_status_to_tg_status(rst_dict['success_lst'], 3, con)
dm.change_status_to_tg_status(rst_dict['failed_lst'], 4, con)

con.commit()


# In[45]:


# 分析成功列表中的体检数据，并将结果封装置analyze_lst
con = msdb.connect(host = '192.168.1.20' ,user = 'root', passwd = 'liangzhi123', db = 'dmp',  port = 3306, charset="utf8")
analyze_lst = examination_analyze(rst_dict['success_lst'], con)


# In[46]:


# 更新结果分析表
con = msdb.connect(host = '192.168.1.22' ,user = 'root', passwd = 'liangzhi123', db = 'datamining',  port = 3306, charset="utf8")
update_abnormal_fields(analyze_lst, con)

