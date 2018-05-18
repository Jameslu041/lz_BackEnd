# coding:utf-8
"""
author: james lu
start_time: 18.05.08
for database and flask_frame initialize.

"""

from flask import Flask
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
app.config.from_pyfile('app.conf')
db = SQLAlchemy(app)
app.secret_key = 'james'

from EOI_CODE import api_develop
