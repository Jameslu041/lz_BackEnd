# coding:utf-8
"""
author: james lu
start_time: 18.05.08
establish ORM for database and Flask_frame

"""

from EOI_CODE import db, app

class Check_Report(db.Model):

    __tablename__ = 'examination_result_fields'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    exam_date = db.Column(db.DateTime)
    category = db.Column(db.String(256))
    field_en_name = db.Column(db.String(256))
    field_ch_name = db.Column(db.String(256))
    db_name = db.Column(db.String(256), unique=True)
    tbl_name = db.Column(db.String(256))
    part_cols = db.Column(db.String(256))
    create_date = db.Column(db.DateTime)
    detect_or_not = db.Column(db.String(256))
    field_type = db.Column(db.String(256))
    missing_value_num = db.Column(db.Integer)
    missing_value_prop = db.Column(db.Float)
    abnormal_value_num = db.Column(db.Integer)
    abnormal_value_prop = db.Column(db.Float)
    max_value = db.Column(db.Float)
    min_value = db.Column(db.Float)
    mean_value = db.Column(db.Float)
    std_value = db.Column(db.Float)
    upper_quartile = db.Column(db.Float)
    lower_quartile = db.Column(db.Float)
    exam_rules = db.Column(db.String(1024))
    rules_result = db.Column(db.String(1024))

    def __init__(self, exam_date, category, field_en_name, field_ch_name, db_name,
                 tbl_name, part_cols, create_date, detect_or_not, field_type,
                 missing_value_num, missing_value_prop, abnormal_value_num, abnormal_value_prop,
                 max_value, min_value, mean_value, std_value, upper_quartile, lower_quartile,
                 exam_rules,rules_result):
        self.exam_date = exam_date
        self.category = category
        self.field_en_name = field_en_name
        self.field_ch_name = field_ch_name
        self.db_name = db_name
        self.tbl_name = tbl_name
        self.part_cols = part_cols
        self.create_date = create_date
        self.detect_or_not = detect_or_not
        self.field_type = field_type
        self.missing_value_num = missing_value_num
        self.missing_value_prop = missing_value_prop
        self.abnormal_value_num = abnormal_value_num
        self.abnormal_value_prop = abnormal_value_prop
        self.max_value = max_value
        self.min_value = min_value
        self.mean_value = mean_value
        self.std_value = std_value
        self.upper_quartile = upper_quartile
        self.lower_quartile = lower_quartile
        self.exam_rules = exam_rules
        self.rules_result = rules_result

class Abnormal_Sum(db.Model):
    __tablename__ = 'examination_abnormal_fields'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    exam_date = db.Column(db.DateTime)
    db_name = db.Column(db.String(256), unique=True)
    tbl_name = db.Column(db.String(256))
    part_cols = db.Column(db.String(256))
    field_name = db.Column(db.String(256))
    abnormal_level = db.Column(db.String(32))
    abnormal_detail = db.Column(db.String(1024))

    def __init__(self, exam_date, db_name, tbl_name, part_cols, field_name, abnormal_level, abnormal_detail):
        self.exam_date = exam_date
        self.db_name = db_name
        self.tbl_name = tbl_name
        self.part_cols = part_cols
        self.field_name = field_name
        self.abnormal_level = abnormal_level
        self.abnormal_detail = abnormal_detail


class Check_Sum(db.Model):
    __tablename__ = 'examination_result_tables'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    exam_date = db.Column(db.DateTime)
    db_name = db.Column(db.String(256), unique=True)
    tbl_name = db.Column(db.String(256))
    part_cols = db.Column(db.String(256))
    exam_fields_count = db.Column(db.Integer)
    exam_result = db.Column(db.String(32))

    def __init__(self, exam_date, db_name, tbl_name, part_cols, exam_fields_count, exam_result):
        self.exam_date = exam_date
        self.db_name = db_name
        self.tbl_name = tbl_name
        self.part_cols = part_cols
        self.exam_fields_count = exam_fields_count
        self.exam_result = exam_result


class Rule_Table(db.Model):
    __tablename__ = 'examination_rules'
    id = db.Column(db.Integer, primary_key=True)
    db_name = db.Column(db.String(256), unique=True)
    table_name = db.Column(db.String(256))
    field_name = db.Column(db.String(256))
    rules = db.Column(db.String(2048))
    valid_datetime_start = db.Column(db.DateTime)
    valid_datetime_end = db.Column(db.DateTime)
    category = db.Column(db.String(256))

    def __init__(self, db_name, table_name, field_name, rules, valid_datetime_start, valid_datetime_end, category):
        self.db_name = db_name
        self.table_name = table_name
        self.field_name= field_name
        self.rules = rules
        self.valid_datetime_start = valid_datetime_start
        self.valid_datetime_end = valid_datetime_end
        self.category = category


class Term_info(db.Model):
    __tablename__ = 'term_view_info'
    id = db.Column(db.Integer, primary_key=True)
    theme = db.Column(db.String(256))
    logical_table_name = db.Column(db.String(256))
    logical_col_name = db.Column(db.String(256))
    physical_db_name = db.Column(db.String(256))
    physical_table_name = db.Column(db.String(256))
    physical_col_name = db.Column(db.String(256))
    join_flag = db.Column(db.String(32))
    update_date = db.Column(db.DateTime)

    def __init__(self, theme, logical_table_name, logical_col_name, physical_db_name, physical_table_name, physical_col_name,
                 join_flag, update_date):
        self.theme = theme
        self.logical_table_name = logical_table_name
        self.logical_col_name = logical_col_name
        self.physical_db_name = physical_db_name
        self.physical_col_name = physical_col_name
        self.physical_table_name = physical_table_name
        self.join_flag = join_flag
        self.update_date = update_date
