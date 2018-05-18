# coding:utf-8
"""
author: james lu
start_time: 18.05.08
api for extract data for page P3-1数据质量核检查-完整报告
"""


from flask import abort, jsonify, redirect, request
from EOI_CODE import app, db
from EOI_CODE.models import Check_Report, Abnormal_Sum, Check_Sum, Rule_Table, Term_info
from sqlalchemy import desc, extract, and_
import json


@app.route('/examination/')
def index():
    return 'hello, tester'


@app.route('/examination/general_report/statistic', methods=['GET','POST'])
def general_report_statistic():
    """
    P3-1 今日检核结果
    返回页面所有统计数字结果
    """
    page = 1
    pageSize = 10

    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')

    get_time = Abnormal_Sum.query.order_by(desc(Abnormal_Sum.exam_date)).first()
    date = get_time.exam_date


    abnormal_data = Abnormal_Sum.query.filter_by(exam_date=date).all()

    CheckSum_data = Check_Sum.query.filter_by(exam_date=date).all()

    fields_sum = 0
    for record in CheckSum_data:
        fields_sum += record.exam_fields_count
    fields_abnormal = len(abnormal_data)
    table_sum = len(CheckSum_data)
    table_abnormal = 0
    for item in CheckSum_data:
        if item.exam_result == '0':
            table_abnormal += 1

    result = {
        'exam_date': date.strftime('%Y-%m-%d %H:%M:%S'),
        'fields_sum': fields_sum,
        'fields_abnormal': fields_abnormal,
        'table_sum': table_sum,
        'abnormal_rate': round(table_abnormal/table_sum, 3)
    }
    output = {'code': 200, 'data': result, 'msg': 'ok', 'total': len(result)}

    return jsonify(output)


@app.route('/examination/general_report/abnormal_table', methods=['GET', 'POST'])
def general_report_abnormal_table():
    """
    P3-1 今日检核结果
    返回警告表格数据
    """
    page = 1
    pageSize = 10

    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')

    get_time = Abnormal_Sum.query.order_by(desc(Abnormal_Sum.exam_date)).first()
    date = get_time.exam_date


    abnormal_data = Abnormal_Sum.query.filter_by(exam_date=date).paginate(page=int(page), per_page=int(pageSize))
    abnormal_data_all = Abnormal_Sum.query.filter_by(exam_date=date).all()

    ab_output = []
    for item in abnormal_data.items:
        ab_output.append({
            'id': item.id,
            'field_name': item.field_name,
            'tbl_name': item.tbl_name,
            'abnormal_level': item.abnormal_level,
            'abnormal_detail': item.abnormal_detail
        })
    output = {'code': 200, 'data': ab_output, 'msg': 'ok', 'total': len(abnormal_data_all)}

    return jsonify(output)


@app.route('/examination/general_report/tables', methods=['GET', 'POST'])
def general_report_tables():
    page = 1
    pageSize = 10

    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')

    get_time = Abnormal_Sum.query.order_by(desc(Abnormal_Sum.exam_date)).first()
    date = get_time.exam_date

    pass




@app.route('/examination/detail_report/statistic', methods=['GET','POST'])
def detail_report_statistic():
    """
    P3-1-1 今日检核结果-查看完整结果报告
    :return:
    """
    page = 1
    pageSize = 10

    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')


    get_time = Abnormal_Sum.query.order_by(desc(Abnormal_Sum.exam_date)).first()
    date = get_time.exam_date

    abnormal_data = Abnormal_Sum.query.filter_by(exam_date=date).all()

    CheckSum_data = Check_Sum.query.filter_by(exam_date=date).all()

    fields_sum = 0
    for record in CheckSum_data:
        fields_sum += record.exam_fields_count
    fields_abnormal = len(abnormal_data)
    table_sum = len(CheckSum_data)
    table_abnormal = 0
    for record in CheckSum_data:
        if record.exam_result == '0':
            table_abnormal += 1

    result = {
        'fields_sum': fields_sum,
        'fields_abnormal': fields_abnormal,
        'table_sum': table_sum,
        'table_abnormal': table_abnormal,
        'abnormal_rate': round(table_abnormal/table_sum, 3)
    }
    output = {'code': 200, 'data': result, 'msg': 'ok', 'total': len(result)}

    return jsonify(output)


@app.route('/examination/detail_report/abnormal_table', methods=['GET','POST'])
def detail_report_abnormal_table():
    """
    P3-1-1 今日检核结果-查看完整结果报告
    返回警告表格数据
    """
    page = 1
    pageSize = 10

    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')

    get_time = Abnormal_Sum.query.order_by(desc(Abnormal_Sum.exam_date)).first()
    date = get_time.exam_date


    abnormal_data = Abnormal_Sum.query.filter_by(exam_date=date).paginate(page=int(page), per_page=int(pageSize))
    abnormal_data_all = Abnormal_Sum.query.filter_by(exam_date=date).all()

    ab_output = []
    for item in abnormal_data.items:
        ab_output.append({
            'id': item.id,
            'field_name': item.field_name,
            'tbl_name': item.tbl_name,
            'abnormal_level': item.abnormal_level,
            'abnormal_detail': item.abnormal_detail
        })
    output = {'code': 200, 'data': ab_output, 'msg': 'ok', 'total': len(abnormal_data_all)}

    return jsonify(output)


@app.route('/examination/detail_report/tables', methods=['GET','POST'])
def detail_report_tables():
    page = 1
    pageSize = 10

    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')


    get_time = Abnormal_Sum.query.order_by(desc(Abnormal_Sum.exam_date)).first()
    date = get_time.exam_date

    CheckSum_data = Check_Sum.query.filter_by(exam_date=date).paginate(page=int(page), per_page=int(pageSize))
    CheckSum_data_all = Check_Sum.query.filter_by(exam_date=date).all()

    checkS_output = []
    for item in CheckSum_data.items:
        checkS_output.append({
            'id': item.id,
            'tbl_name': item.tbl_name,
            'exam_fields_count': item.exam_fields_count,
            'exam_result': item.exam_result
        })

    output = {'code': 200, 'data': checkS_output, 'msg': 'ok', 'total': len(CheckSum_data_all)}

    return jsonify(output)


@app.route('/examination/detail_report/detail_page', methods=['GET','POST'])
def view_detail_report():
    """
    点击 P3-1-2 查看详情 触发
    :param table_name: name of table viewing now, string
    return: json data
    """
    page = 1
    pageSize = 10

    if request.method == 'POST':
        table_name = request.values.get('table_name')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        table_name = request.args.get('table_name')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')
    else:
        return jsonify({"code": 500, "data": [], "msg": "unvalid request!"})

    get_time = Abnormal_Sum.query.order_by(desc(Abnormal_Sum.exam_date)).first()
    date = get_time.exam_date

    data = Check_Report.query.filter_by(tbl_name=table_name, exam_date=date).paginate(page=int(page), per_page=int(pageSize))
    data_all = Check_Report.query.filter_by(tbl_name=table_name, exam_date=date).all()
    if data is None:
        return abort(404)
    else:
        result = []
        for item in data.items:
            result.append(
                {
                'field_en_name': item.field_en_name,
                'missing_value_num': item.missing_value_num,
                'missing_value_prop': item.missing_value_prop,
                'abnormal_value_num': item.abnormal_value_num,
                'abnormal_value_prop': item.abnormal_value_prop,
                'max_value': item.max_value,
                'min_value': item.min_value,
                'mean_value': item.mean_value,
                'std_value': item.std_value,
                'upper_quartile': item.upper_quartile,
                'lower_quartile': item.lower_quartile
                }
            )
        output = {'code': 200, 'data': result, 'msg': 'ok', 'total': len(data_all)}

        return jsonify(output)


@app.route('/examination/history_report', methods=['GET','POST'])
def history_report():
    """
    P3-2 历史报告
    :return:
    """
    page = 1
    pageSize = 10
    if request.method == 'POST':
        year = request.values.get('year')
        month = request.values.get('month')
        page = int(request.values.get('page'))
        pageSize = int(request.values.get('pageSize'))
    elif request.method == 'GET':
        year = request.args.get('year')
        month = request.args.get('month')
        page = int(request.args.get('page'))
        pageSize = int(request.args.get('pageSize'))
    else:
        return jsonify({"code": 500, "data": [], "msg": "unvalid request!"})



    CheckSum_data = Check_Sum.query.filter(and_(extract('year', Check_Sum.exam_date)==year,
                                           extract('month', Check_Sum.exam_date)==month)).all()
    abnormal_data = Abnormal_Sum.query.filter(and_(extract('year', Abnormal_Sum.exam_date)==year,
                                           extract('month', Abnormal_Sum.exam_date)==month)).all()
    if CheckSum_data and abnormal_data is None:
        return abort(404)
    # elif len(CheckSum_data) == 0 or len(abnormal_data) == 0:
    #     return redirect('/')
    else:
        failed_dict = {}
        for item in CheckSum_data:
            if item.exam_result == '0':
                if item.exam_date.strftime('%Y-%m-%d') in failed_dict:
                    failed_dict[item.exam_date.strftime('%Y-%m-%d')] += 1
                else:
                    failed_dict[item.exam_date.strftime('%Y-%m-%d')] = 1

        abnormal_dict = {}
        for item in abnormal_data:
            if item.exam_date.strftime('%Y-%m-%d') in abnormal_dict:
                abnormal_dict[item.exam_date.strftime('%Y-%m-%d')] += 1
            else:
                abnormal_dict[item.exam_date.strftime('%Y-%m-%d')] = 1

        result = []

        for key in abnormal_dict:
            result.append({
                'exam_date': key,
                'abnormal': abnormal_dict[key],
                'failed': failed_dict[key],
                'status': '日常检核'
            })

        page = int(page)
        pageSize = int(pageSize)

        try:
            result_paginate = result[((page)-1)*10:(page-1)*10+pageSize]
        except:
            result_paginate = result[(page-1)*10:-1]

        output = {'code': 200, 'data': result_paginate, 'msg': 'ok', 'total': len(abnormal_dict)}
        return jsonify(output)


@app.route('/examination/rule_set', methods=['GET','POST'])
def rule_set():
    """
    P3-3 规则配置
    :return:
    """
    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')
    else:
        return jsonify({"code": 500, "data": [], "msg": "unvalid request!"})

    # 传参则筛选, 不传则展示所有
    if category:
        data = Check_Report.query.filter_by(category=category).paginate(page=int(page), per_page=int(pageSize))
    else:
        data = Check_Report.query.paginate(page=int(page), per_page=int(pageSize))

    all_data = Check_Report.query.all()

    result = []
    for item in data.items:
        result.append({
            'field_ch_name': item.field_ch_name,
            'belong_to': item.db_name + '/' + item.tbl_name,
            'rule': item.rules_result
        })

    output = {'code': 200, 'data': result, 'msg': 'ok', 'total': len(all_data)}
    return jsonify(output)


@app.route('/examination/rule_set/add_rule', methods=['GET','POST'])
def add_rule():
    """
    P3-3-1 新增规则
    :return:
    """
    if request.method == 'POST':
        # category_all = request.values.get('category')
        field_en_name = request.values.get('field_en_name')
        number_range = request.values.get('number_range')
        length_range = request.values.get('length_range')
        number_format = request.values.get('number_format')
        tel_number_check = request.values.get('tel_number_check')
        phone_number_check = request.values.get('phone_number_check')
    elif request.method == 'GET':
        # category_all = request.args.get('category')
        field_en_name = request.args.get('field_en_name')
        number_range = request.args.get('number_range')
        length_range = request.args.get('length_range')
        number_format = request.args.get('number_format')
        tel_number_check = request.args.get('tel_number_check')
        phone_number_check = request.args.get('phone_number_check')
    else:
        return jsonify({"code": 500, "data": [], "msg": "unvalid request!"})

    import pymysql

    conn = pymysql.connect(
        host="192.168.2.52",
        port=3306,
        user="liangzhi",
        password="liangzhi123",
        database="dmp",
        charset='utf8',
        )
    # 获取游标
    cursor = conn.cursor()
    select_sql = '''SELECT physical_table_name FROM term_view_info where physical_col_name = "%s";''' % field_en_name

    cursor.execute(select_sql)
    result = cursor.fetchone()

    db_name = 'mtoi'
    table_name = result[0]
    field_name = field_en_name
    tmp = {
        'number_range': number_range,
        'length_range': length_range,
        'number_format': number_format,
        'tel_number_check': tel_number_check,
        'phone_number_check': phone_number_check
    }
    rules = json.dumps(tmp)
    valid_datetime_start = '2018-05-12'
    valid_datetime_end = '2018-05-13'

    try:
        # 执行一条insert语句，返回受影响的行数
        # cursor.execute("INSERT INTO para5(name,age) VALUES(%s,%s);",('次牛','12'))
        # 执行多次insert并返回受影响的行数
        insert_sql = '''INSERT INTO examination_rules_new(db_name, table_name, field_name, rules, valid_datetime_start, valid_datetime_end) VALUES('%s','%s','%s','%s','%s','%s');''' % (db_name, table_name, field_name, rules, valid_datetime_start, valid_datetime_end)
        cursor.execute(insert_sql)
        # 提交执行
        conn.commit()
    except Exception as e:
        # 如果执行sql语句出现问题，则执行回滚操作
        conn.rollback()
        print(e)
    finally:
        # 不论try中的代码是否抛出异常，这里都会执行
        # 关闭游标和数据库连接
        cursor.close()
        conn.close()


    return jsonify({'code': 200, 'data': '', 'msg': 'ok', 'total': 0})


@app.route('/examination/field_category', methods=['GET', 'POST'])
def field_category():
    """
    P3-4 字段分类
    :return:
    """
    if request.method == 'POST':
        category = request.values.get('category')
        page = request.values.get('page')
        pageSize = request.values.get('pageSize')
    elif request.method == 'GET':
        category = request.args.get('category')
        page = request.args.get('page')
        pageSize = request.args.get('pageSize')
    else:
        return jsonify({"code": 500, "data": [], "msg": "unvalid request!"})

    # 传参则筛选, 不传则展示所有
    if category:
        data = Check_Report.query.filter_by(category=category).paginate(page=int(page), per_page=int(pageSize))
    else:
        data = Check_Report.query.paginate(page=int(page), per_page=int(pageSize))

    data_all = Check_Report.query.all()
    result = []
    for item in data.items:
        result.append({
            'id': item.id,
            'field_en_name': item.field_en_name,
            'field_ch_name': item.field_ch_name,
            'field_type': item.field_type,
            'detect_or_not': item.detect_or_not
        })

    if len(result):
        msg = 'ok'
    else:
        msg = 'db no record'

    output = {'code': 200, 'data': result, 'msg': msg, 'total': len(data_all)}
    return jsonify(output)







@app.route('/test', methods=['GET', 'POST'])
def test():
    import pymysql

    conn = pymysql.connect(
        host="192.168.2.52",
        port=3306,
        user="liangzhi",
        password="liangzhi123",
        database="dmp",
        charset='utf8',
        )
    # 获取游标
    cursor = conn.cursor()
    db_name = 'mtoi'
    table_name = 'result[0]'
    field_name = 'field_en_name'
    rules = [{
        'number_range': str('number_range'),
        'length_range': str('length_range'),
        'number_format': str('number_format'),
        'tel_number_check': str('tel_number_check'),
        'phone_number_check': str('phone_number_check')
    }]
    valid_datetime_start = 0
    valid_datetime_end = 0
    category = 'category_all'

    try:
        # 执行一条insert语句，返回受影响的行数
        # cursor.execute("INSERT INTO para5(name,age) VALUES(%s,%s);",('次牛','12'))
        # 执行多次insert并返回受影响的行数
        cursor.executemany("INSERT INTO examination_rules_new(db_name, table_name, field_name, rules,"
                           "valid_datetime_start, valid_datetime_end, category) VALUES(%s,%s,%s,%s,%s,%s,%s);",
                           (db_name, table_name, field_name, rules, valid_datetime_start, valid_datetime_end))
        # 提交执行
        conn.commit()
    except Exception as e:
        # 如果执行sql语句出现问题，则执行回滚操作
        conn.rollback()
        print(e)
    finally:
        # 不论try中的代码是否抛出异常，这里都会执行
        # 关闭游标和数据库连接
        cursor.close()
        conn.close()

    return 'test'
