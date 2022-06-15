import pandas as pd
from bs4 import BeautifulSoup
import urllib, pymysql, calendar, time, json
from urllib.request import urlopen
from datetime import datetime
from threading import Timer
import pymysql
from pandas_datareader import data as pdr 
import yfinance as yf 
import re
import math
from datetime import date

class DBUpdater:
    def __init__(self):
        # 생성자 DB연결 및 종목코드 딕셔너리 생성
        config = {
        'host':'127.0.0.1',
        'user':'quantvis',
        'password':'quantvis',
        'database':'quantvis',
        'port':3306,
        'charset':'utf8',
        'use_unicode':True
        }
        self.conn = pymysql.connect(**config)
        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
                code VARCHAR(20) primary key,
                company VARCHAR(40),
                last_update DATE
            )
            """
            curs.execute(sql)
            
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
                code varchar(20),
                date date,
                open int(20),
                high int(20),
                low int(20),
                close int(20),
                diff int(20),
                volume int(20),
                primary key (code, date)
            )
            """
            curs.execute(sql)
        self.conn.commit()
        self.codes = dict()
            
    def __del__(self):
        # 소멸자 : DB연결 해제
        self.conn.close()
    
    def read_krx_code(self):
        # KRX로부터 상장기업 목록 파일을 읽어와서 데이터 프레임으로 반환
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
            'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드','회사명']]
        krx = krx.rename(columns={'종목코드':'code','회사명':'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx
    
    def update_comp_info(self):
        # 종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values(idx)] = df['company'].values[idx]
            
        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')
            
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last"\
                        f"_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info"\
                        f"VALUES ({code}, {company}, {today})")
                self.conn.commit()
                print()
    def read_naver(self, code, company, pages_to_fetch):
        # 네이버에서 주식 시세를 읽어서 데이터프레임으로 반환
        try:
            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            with urlopen(url) as doc:
                if doc is None:
                    return None
                html = BeautifulSoup(doc, "lxml")
                pgrr = html.find("td",class_="pgRR")
                if pgrr is None:
                    return None
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                df = df.append(pd.read_html(pg_url, header=0)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page, pages), end="\r")
            df = df.rename(columns={'날짜':'date','종가':'close','전일비':'diff','시가':'open','고가':'high','저가':'low','거래량':'volume'})
            df['date'] = df['date'].replace('.','-')
            df = df.dropna()
            df[['close','diff','open','high','low','volume']] = df[['close',
                                                                   'diff','open','high','low','volume']].astype(int)
            df = df[['date','open','high','low','close','diff','volume']]
        except Exception as e:
            print('Exception occured : ', str(e))
            return None
        return df
    
    def replace_into_db(self,df,num,code,company):
        # 네이버에서 읽어온 주식 시세를 DB에 REPLACE
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = "REPLACE INTO dalily_price VALUES ('{}','{}',{},{}"\
                    ", {}, {}, {}, {})".format(code,r.date, r.open, r.high, r.low, r.close, r.diff, r.volume)
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_'\
                'price [OK]'.format(datetime.now().strftime('%Y-%m-%d'\
                    ' %H:%M'), num+1, company, code, len(df)))
            
    def update_daily_price(self, pages_to_fetch):
        # KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])
            
    def execute_daily(self):
        # 실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트
        self.update_comp_info()
        try:
            with open('config.json','r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json','w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch':1}
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)
        
        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        
        if tmnow.month == 12 and tmnow.dat == lastday:
            tmnext = tmnow.replace(year=tmnow.year+1, month=1, day=1,
                                   hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month+1, day=1, hour=17,
                                   minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day+1, hour=17, minute=0,
                                   second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds
        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()
        

    yf.pdr_override()

    ustotalcodelist = ['a', 'aal', 'aap', 'aapl', 'abbv', 'abc', 'abmd', 'abt', 'acn', 'adbe', 'adi', 'adm', 'adp', 'adsk', 'aee', 'aep', 'aes', 'afl', 'aig', 'aiz', 'ajg', 'akam', 'alb', 'algn', 'alk', 'all', 'alle', 'amat', 'amcr', 'amd', 'ame', 'amgn', 'amp', 'amt', 'amzn', 'anet', 'anss', 'antm', 'aon', 'aos', 'apa', 'apd', 'aph', 'aptv', 'are', 'ato', 'atvi', 'avb', 'avgo', 'avy', 'awk', 'axp', 'azo', 'ba', 'bac', 'bax', 'bbwi', 'bby', 'bdx', 'ben', 'bf-b', 'biib', 'bio', 'bk', 'bkng', 'bkr', 'blk', 'bll', 'bmy', 'br', 'brk-b', 'bro', 'bsx', 'bwa', 'bxp', 'c', 'cag', 'cah', 'carr', 'cat', 'cb', 'cboe', 'cbre', 'cci', 'ccl', 'cday', 'cdns', 'cdw', 'ce', 'cern', 'cf', 'cfg', 'chd', 'chrw', 'chtr', 'ci', 'cinf', 'cl', 'clx', 'cma', 'cmcsa', 'cme', 'cmg', 'cmi', 'cms', 'cnc', 'cnp', 'cof', 'coo', 'cop', 'cost', 'cpb', 'cprt', 'crl', 'crm', 'csco', 'csx', 'ctas', 'ctlt', 'ctra', 'ctsh', 'ctva', 'ctxs', 'cvs', 'cvx', 'czr', 'd', 'dal', 'dd', 'de', 'dfs', 'dg', 'dgx', 'dhi', 'dhr', 'dis', 'dish', 'dlr', 'dltr', 'dov', 'dow', 'dpz', 'dre', 'dri', 'dte', 'duk', 'dva', 'dvn', 'dxc', 'dxcm', 'ea', 'ebay', 'ecl', 'ed', 'efx', 'eix', 'el', 'emn', 'emr', 'enph', 'eog', 'eqix', 'eqr', 'es', 'ess', 'etn', 'etr', 'etsy', 'evrg', 'ew', 'exc', 'expd', 'expe', 'exr', 'f', 'fang', 'fast', 'fb', 'fbhs', 'fcx', 'fdx', 'fe', 'ffiv', 'fis', 'fisv', 'fitb', 'flt', 'fmc', 'fox', 'foxa', 'frc', 'frt', 'ftnt', 'ftv', 'gd', 'ge', 'gild', 'gis', 'gl', 'glw', 'gm', 'gnrc', 'goog', 'googl', 'gpc', 'gpn', 'gps', 'grmn', 'gs', 'gww', 'hal', 'has', 'hban', 'hbi', 'hca', 'hd', 'hes', 'hig', 'hii', 'hlt', 'holx', 'hon', 'hpe', 'hpq', 'hrl', 'hsic', 'hst', 'hsy', 'hum', 'hwm', 'ibm', 'ice', 'idxx', 'iex', 'iff', 'ilmn', 'incy', 'intc', 'intu', 'ip', 'ipg', 'ipgp', 'iqv', 'ir', 'irm', 'isrg', 'it', 'itw', 'ivz', 'j', 'jbht', 'jci', 'jkhy', 'jnj', 'jnpr', 'jpm', 'k', 'key', 'keys', 'khc', 'kim', 'klac', 'kmb', 'kmi', 'kmx', 'ko', 'kr', 'l', 'ldos', 'leg', 'len', 'lh', 'lhx', 'lin', 'lkq', 'lly', 'lmt', 'lnc', 'lnt', 'low', 'lrcx', 'lumn', 'luv', 'lvs', 'lw', 'lyb', 'lyv', 'ma', 'maa', 'mar', 'mas', 'mcd', 'mchp', 'mck', 'mco', 'mdlz', 'mdt', 'met', 'mgm', 'mhk', 'mkc', 'mktx', 'mlm', 'mmc', 'mmm', 'mnst', 'mo', 'mos', 'mpc', 'mpwr', 'mrk', 'mrna', 'mro', 'ms', 'msci', 'msft', 'msi', 'mtb', 'mtch', 'mtd', 'mu', 'nclh', 'ndaq', 'nee', 'nem', 'nflx', 'ni', 'nke', 'nlok', 'nlsn', 'noc', 'now', 'nrg', 'nsc', 'ntap', 'ntrs', 'nue', 'nvda', 'nvr', 'nwl', 'nws', 'nwsa', 'nxpi', 'o', 'odfl', 'ogn', 'oke', 'omc', 'orcl', 'orly', 'otis', 'oxy', 'payc', 'payx', 'pcar', 'peak', 'peg', 'penn', 'pep', 'pfe', 'pfg', 'pg', 'pgr', 'ph', 'phm', 'pkg', 'pki', 'pld', 'pm', 'pnc', 'pnr', 'pnw', 'pool', 'ppg', 'ppl', 'pru', 'psa', 'psx', 'ptc', 'pvh', 'pwr', 'pxd', 'pypl', 'qcom', 'qrvo', 'rcl', 're', 'reg', 'regn', 'rf', 'rhi', 'rjf', 'rl', 'rmd', 'rok', 'rol', 'rop', 'rost', 'rsg', 'rtx', 'sbac', 'sbux', 'schw', 'see', 'shw', 'sivb', 'sjm', 'slb', 'sna', 'snps', 'so', 'spg', 'spgi', 'sre', 'ste', 'stt', 'stx', 'stz', 'swk', 'swks', 'syf', 'syk', 'syy', 't', 'tap', 'tdg', 'tdy', 'tech', 'tel', 'ter', 'tfc', 'tfx', 'tgt', 'tjx', 'tmo', 'tmus', 'tpr', 'trmb', 'trow', 'trv', 'tsco', 'tsla', 'tsn', 'tt', 'ttwo', 'twtr', 'txn', 'txt', 'tyl', 'ua', 'uaa', 'ual', 'udr', 'uhs', 'ulta', 'unh', 'unp', 'ups', 'uri', 'usb', 'v', 'vfc', 'vlo', 'vmc', 'vno', 'vrsk', 'vrsn', 'vrtx', 'vtr', 'vtrs', 'vz', 'wab', 'wat', 'wba', 'wdc', 'wec', 'well', 'wfc', 'whr', 'wm', 'wmb', 'wmt', 'wrb', 'wrk', 'wst', 'wu', 'wy', 'wynn', 'xel', 'xom', 'xray', 'xyl', 'yum', 'zbh', 'zbra', 'zion', 'zts']
    #미국주식 497종목, "-"유지

    def updateData(stock_code):
        config = {
            'host':'127.0.0.1',
            'user':'quantvis',
            'password':'quantvis',
            'database':'quantvis',
            'port':3306,
            'charset':'utf8',
            'use_unicode':True
        }

        try:
            conn = pymysql.connect(**config)
            cursor = conn.cursor()
            code=stock_code.replace("-","_")
            sql = f"select * from `{code}`"
            cursor.execute(sql)
            rows = cursor.fetchall()
            col = ['date', 'open', 'high', 'low', 'close', 'volume', 'company', 'ticker']
            result = pd.DataFrame(rows, columns=col)
            
            today = date.today()
            x_date = result['date'][len(result)-1]
            # 업데이트 필요시
            if x_date < today:                  #brk-b, aapl 
                stock_data= pdr.get_data_yahoo(stock_code,start=f"20{x_date.strftime('%y-%m-%d')}",end=f"20{today.strftime('%y-%m-%d')}")
                ticker= yf.Ticker(f"{stock_code}")
                company= ticker.info.get('longName')
                company= re.sub("[!@#$%^&&*()/`~_.,+=']",'',company)
                d= pd.DataFrame( 
                {   'Open':stock_data['Open'],
                    'High':stock_data['High'],
                    'Low':stock_data['Low'],
                    'Close':stock_data['Close'],
                    'Volume':stock_data['Volume'],
                    'Company': f"{company}", 
                    'Ticker': stock_code.upper()
                    })
                df= d.where(pd.notnull(d), None)
                df = df[2:df.size] # 기준 날은 슬라이싱
                df = df.reset_index()
                
                for i in range(0, len(df)):
                    date2 = df['Date'][i].strftime('%y%m%d')
                    open= df['Open'][i].astype('float')
                    high = df['High'][i].astype('float')
                    low = df['Low'][i].astype('float')
                    close = df['Close'][i].astype('float')
                    volume = df['Volume'][i].astype('float')
                    company = df['Company'][i]
                    ticker = df['Ticker'][i].replace("-","_")
                    
                    sql = f"insert into `{ticker}` values({date2}, {open}, {high}, {low}, {close}, {volume}, '{company}', '{ticker}')"
                    cursor.execute(sql)
                    conn.commit()
            else:
                print("해당 테이블은 업데이트 할 내용이 없습니다.")
            
        except Exception as e:
            print("DB연동 에러 : ", e)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


    count=1 
    for usStock in ustotalcodelist:
        updateData(usStock) 
        print(f"{usStock}을 입력합니다.")
        print(f"{count}/497")
        
        count+=1
        
if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
    