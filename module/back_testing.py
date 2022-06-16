from posixpath import split
from flask import Flask, redirect, send_file, url_for, render_template, request, session, flash, Blueprint, current_app
from authlib.integrations.flask_client import OAuth
from datetime import timedelta
import os, requests
from pykrx import stock
import yfinance as yf
import datetime
import matplotlib.pyplot as plt
from pandas_datareader import data as pdr
import pandas as pd
import io
import matplotlib.ticker as ticker
from mpl_finance import candlestick2_ohlc
from DB.portfolioDB import Portfolio
from DB.quantvis import Quantvis
import re,time
#** 블루프린트로 만들 파일에서 Blueprint 모듈을 import
#** "controller.py에서 불러올 때 쓸 변수명" = Blueprint( @app 대신 쓸 이름, __name__, 적용할 url prefix)
btest = Blueprint('backTesting', __name__, url_prefix='/backTesting')

# 포트폴리오 첫 페이지
@btest.route('/')
def back_testing() :
    return render_template('back_testing.html')