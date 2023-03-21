import pandas as pd
import numpy as np
# from sqlalchemy import create_engine
from functools import reduce
import warnings
import pymysql
import psycopg2
warnings.filterwarnings(action='ignore')
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_columns', 300)
from datetime import datetime, timedelta

now = datetime.now()
print("현재 :" , now)	# 현재 : 2021-01-09 19:41:03.645702
before_one_day = now - timedelta(days=1)
print("1일 전 :", before_one_day)	# 1일 전 : 2021-01-08 19:41:03.645702
after_one_day = now + timedelta(days=1)
print("1일 후 :", after_one_day)	# 1일 후 : 2021-01-10 19:41:03.645702
# PG 
# 데이터베이스 연결
msdb_ = psycopg2.connect (
    host = '175.126.38.47',
    database = 'db_base',
    user = 'orderhero',
    password = 'OhejGL@JFH2023',
    port = '5432'
    )




my_sql = f"""
SELECT * FROM sales_history
"""

with msdb_.cursor() as cursor :
    cursor.execute(my_sql)
    result = cursor.fetchall()
    msdb_.commit()
    cursor.close()
msdb_.close()

    


outlist = []
for utf_ in result :
    col1 = utf_[0]
    col2 = utf_[1]
    col3 = utf_[2]
    col4 = utf_[3]
    col5 = utf_[4]
    col6 = utf_[5]
    col7 = utf_[6]
    col8 = utf_[7]
    col9 = utf_[8]
    col10 = utf_[9]
    col11 = utf_[10]
    col12 = utf_[11]
   


    outlist.append([col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12])


df_pg_sales_history = pd.DataFrame(outlist, columns = ['sales_no', 'cust_id', 'sales_date', 'activity_option', 'count'
                                                        , 'process_state', 'sales_cost', 'detail_memo', 'unique_memo', 'insert_id'
                                                        , 'insert_date', 'sales_activities'])

df_pg_sales_history.info()
df_pg_sales_history.head()
# Maria
### 마지막 index 추출
# 데이터베이스 연결
msdb_ = pymysql.connect(
    user='orderherodl', # 추후 읽기 전용으로 수정 필요 
    passwd='OhejGL@JFH2023',
    host='orderherodl.cafe24.com',
    db='orderherodl',
    charset='utf8',
#     use_unicode=True
)


my_sql = f"""
SELECT max(sales_no) FROM TB_SALES_GRAFANA
"""

with msdb_.cursor() as cursor :
    cursor.execute(my_sql)
    result = cursor.fetchall()
    msdb_.commit()
    cursor.close()
msdb_.close()

    


outlist = []
for utf_ in result :
    col1 = utf_[0]

    outlist.append([col1])


df_pg_sales_history_salesno = pd.DataFrame(outlist, columns = ['sales_no_index'])

df_pg_sales_history_salesno
max_sales_no = df_pg_sales_history_salesno.loc[0, 'sales_no_index']
max_sales_no
## 고객정보
# 데이터베이스 연결
msdb_ = pymysql.connect(
    user='orderherodl', # 추후 읽기 전용으로 수정 필요 
    passwd='OhejGL@JFH2023',
    host='orderherodl.cafe24.com',
    db='orderherodl',
    charset='utf8',
#     use_unicode=True
)


my_sql = f"""
SELECT tc.CUST_ID
, tc.business_name
, tc.AREA_GU
, tec.ER_CTG_NAME
, DATE_FORMAT(tc.REG_DATE, '%Y-%m-%d')
, min(to2.REG_DATE)
FROM TB_CUST tc
INNER JOIN TB_ER_CTG tec ON tec.ER_CTG_CD = tc.CTG_CD
LEFT JOIN TB_ORDER to2 ON to2.CUST_ID = tc.CUST_ID
WHERE DELIV_POSITION IS NOT NULL
AND DELIV_POSITION != ''
AND test_yn = 'N'
GROUP BY tc.CUST_ID
"""

with msdb_.cursor() as cursor :
    cursor.execute(my_sql)
    result = cursor.fetchall()
    msdb_.commit()
    cursor.close()
msdb_.close()

    


outlist = []
for utf_ in result :
    col1 = utf_[0].decode('UTF-8')
    col2 = utf_[1].decode('UTF-8')
    col3 = utf_[2].decode('UTF-8')
    col4 = utf_[3].decode('UTF-8')
    col5 = utf_[4].decode('UTF-8')
    col6 = utf_[5]
    
   


    outlist.append([col1, col2, col3, col4, col5, col6])


df_cust_info = pd.DataFrame(outlist, columns = ['CUST_ID', '매장명', '지역구', '업종', '가입일', '첫발주일'])

df_cust_info

## toi
# 데이터베이스 연결
msdb_ = pymysql.connect(
    user='orderherodl', # 추후 읽기 전용으로 수정 필요 
    passwd='OhejGL@JFH2023',
    host='orderherodl.cafe24.com',
    db='orderherodl',
    charset='utf8',
#     use_unicode=True
)


my_sql = f"""
SELECT order_no 
, PROD_CD 
, order_pay
, prod_order_cnt
, coupon_price
FROM TB_ORDER_ITEM
WHERE ARRIVE_DATE >= '2022-11-01' 
"""

with msdb_.cursor() as cursor :
    cursor.execute(my_sql)
    result = cursor.fetchall()
    msdb_.commit()
    cursor.close()
msdb_.close()

    


outlist = []
for utf_ in result :
    col1 = utf_[0]
    col2 = utf_[1].decode('UTF-8')
    col3 = utf_[2]
    col4 = utf_[3]
    col5 = utf_[4]
   


    outlist.append([col1, col2, col3, col4, col5])


df_toi_raw = pd.DataFrame(outlist, columns = ['ORDER_NO', 'PROD_CD', 'order_pay', 'prod_order_cnt', 'coupon_price'])

df_toi_raw
## 주문일, 주문번호
# 데이터베이스 연결
msdb_ = pymysql.connect(
    user='orderherodl', # 추후 읽기 전용으로 수정 필요 
    passwd='OhejGL@JFH2023',
    host='orderherodl.cafe24.com',
    db='orderherodl',
    charset='utf8',
#     use_unicode=True
)


my_sql = f"""
SELECT tc.CUST_ID
, tc.BUSINESS_NAME
, DATE_FORMAT(tc.REG_DATE, '%Y-%m-%d')
, tc.AREA_GU
, tec.ER_CTG_NAME 
, to2.REG_DATE
, to2.ORDER_NO
FROM (SELECT * FROM TB_ORDER
	  WHERE REG_DATE >= '2022-11-01') to2 
INNER JOIN (SELECT * FROM TB_CUST
			WHERE test_yn = 'N'
			AND DELIV_POSITION IS NOT NULL
			AND DELIV_POSITION != '') tc ON tc.CUST_ID = to2.CUST_ID
LEFT JOIN TB_ER_CTG tec ON tec.ER_CTG_CD = tc.CTG_CD
"""

with msdb_.cursor() as cursor :
    cursor.execute(my_sql)
    result = cursor.fetchall()
    msdb_.commit()
    cursor.close()
msdb_.close()

    


outlist = []
for utf_ in result :
    col1 = utf_[0].decode('UTF-8')
    col2 = utf_[1].decode('UTF-8')
    col3 = utf_[2].decode('UTF-8')
    col4 = utf_[3].decode('UTF-8')
    col5 = utf_[4].decode('UTF-8')
    col6 = utf_[5]
    col7 = utf_[6]
   


    outlist.append([col1, col2, col3, col4, col5, col6, col7])


df_cust_order = pd.DataFrame(outlist, columns = ['CUST_ID', '매장명', '가입일', '지역구', '업종', '주문일', 'ORDER_NO'])

df_cust_order
df_cust_order_prod = pd.merge(df_cust_order, df_toi_raw, how='left', on='ORDER_NO')
df_order_cancel = df_cust_order_prod[df_cust_order_prod['PROD_CD'].isna()]
df_cust_order_complete = df_cust_order_prod[~ df_cust_order_prod['PROD_CD'].isna()]
df_cust_order_complete
df_cust_order_complete['매출'] = df_cust_order_complete['order_pay'] * df_cust_order_complete['prod_order_cnt'] - df_cust_order_complete['coupon_price']
df_cust_order_complete
### 업장의 주문일별 매출
df_cust_orderday = df_cust_order_complete[['CUST_ID', '매장명', '가입일', '지역구', '업종', '주문일', '매출']].groupby(['CUST_ID', '매장명', '가입일', '지역구', '업종', '주문일'], as_index=0).sum()
df_cust_orderday.info()
df_cust_orderday['주문일'] = pd.to_datetime(df_cust_orderday['주문일'])
df_cust_orderday
df_pg_sales_history['sales_date_ymd'] = df_pg_sales_history['sales_date'].dt.strftime('%Y-%m-%d')
df_pg_sales_history['sales_date'] = pd.to_datetime(df_pg_sales_history['sales_date'])
df_pg_sales_history['sales_date_ymd'] = pd.to_datetime(df_pg_sales_history['sales_date_ymd'])
df_pg_sales_3col = df_pg_sales_history[['sales_no', 'cust_id', 'sales_date_ymd', 'activity_option', 'sales_activities']]
df_pg_sales_3col.columns = ['sales_no','CUST_ID', 'sales_date_ymd', 'activity_option', 'sales_activities']
df_pg_sales_3col.head()
len(df_pg_sales_3col)
from datetime import datetime, timedelta
start_p7 = df_pg_sales_3col.loc[0,'sales_date_ymd'] 
end_p7 = df_pg_sales_3col.loc[0,'sales_date_ymd'] + timedelta(days=6)
print(start_p7)
print(end_p7)
temp_df = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p7) & (df_cust_orderday['주문일'] <= end_p7) & (df_cust_orderday['CUST_ID']== '2108701169')]
temp_df['매출'].sum()
## 주차별 매출 구하기
def ragne_sales(df):
    sales_no_list = []
    cust_id_list = []
    sales_param_list = []
    activities_list = []
    activ_day_list = []

    p1w_sales_list = []
    p2w_sales_list = []
    p3w_sales_list = []
    p4w_sales_list = []
    p5w_sales_list = []
    p6w_sales_list = []
    p7w_sales_list = []
    p8w_sales_list = []

    m1w_sales_list = []
    m2w_sales_list = []
    m3w_sales_list = []
    m4w_sales_list = []
    m5w_sales_list = []
    m6w_sales_list = []
    m7w_sales_list = []
    m8w_sales_list = []

    for i in range(len(df)):

        ## 세일링 이후

        start_p1w = df.loc[i,'sales_date_ymd'] 
        end_p1w = df.loc[i,'sales_date_ymd'] + timedelta(days=6)
        temp_df_p1w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p1w) & (df_cust_orderday['주문일'] <= end_p1w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p1w_sales = temp_df_p1w['매출'].sum()

        start_p2w = df.loc[i,'sales_date_ymd'] + timedelta(days=7)
        end_p2w = df.loc[i,'sales_date_ymd'] + timedelta(days=13)
        temp_df_p2w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p2w) & (df_cust_orderday['주문일'] <= end_p2w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p2w_sales = temp_df_p2w['매출'].sum()

        start_p3w = df.loc[i,'sales_date_ymd'] + timedelta(days=14)
        end_p3w = df.loc[i,'sales_date_ymd'] + timedelta(days=20)
        temp_df_p3w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p3w) & (df_cust_orderday['주문일'] <= end_p3w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p3w_sales = temp_df_p3w['매출'].sum()

        start_p4w = df.loc[i,'sales_date_ymd'] + timedelta(days=21)
        end_p4w = df.loc[i,'sales_date_ymd'] + timedelta(days=27)
        temp_df_p4w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p4w) & (df_cust_orderday['주문일'] <= end_p4w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p4w_sales = temp_df_p4w['매출'].sum()

        start_p5w = df.loc[i,'sales_date_ymd'] + timedelta(days=28)
        end_p5w = df.loc[i,'sales_date_ymd'] + timedelta(days=34)
        temp_df_p5w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p5w) & (df_cust_orderday['주문일'] <= end_p5w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p5w_sales = temp_df_p5w['매출'].sum()

        start_p6w = df.loc[i,'sales_date_ymd'] + timedelta(days=35)
        end_p6w = df.loc[i,'sales_date_ymd'] + timedelta(days=41)
        temp_df_p6w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p6w) & (df_cust_orderday['주문일'] <= end_p6w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p6w_sales = temp_df_p6w['매출'].sum()

        start_p7w = df.loc[i,'sales_date_ymd'] + timedelta(days=42)
        end_p7w = df.loc[i,'sales_date_ymd'] + timedelta(days=48)
        temp_df_p7w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p7w) & (df_cust_orderday['주문일'] <= end_p7w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p7w_sales = temp_df_p7w['매출'].sum()

        start_p8w = df.loc[i,'sales_date_ymd'] + timedelta(days=49)
        end_p8w = df.loc[i,'sales_date_ymd'] + timedelta(days=55)
        temp_df_p8w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_p8w) & (df_cust_orderday['주문일'] <= end_p8w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        p8w_sales = temp_df_p8w['매출'].sum()


        ## 세일링 이전 

        start_m1w = df.loc[i,'sales_date_ymd'] - timedelta(days=7)
        end_m1w = df.loc[i,'sales_date_ymd'] - timedelta(days=1)
        temp_df_m1w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m1w) & (df_cust_orderday['주문일'] <= end_m1w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m1w_sales = temp_df_m1w['매출'].sum()

        start_m2w = df.loc[i,'sales_date_ymd'] - timedelta(days=14)
        end_m2w = df.loc[i,'sales_date_ymd'] - timedelta(days=8)
        temp_df_m2w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m2w) & (df_cust_orderday['주문일'] <= end_m2w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m2w_sales = temp_df_m2w['매출'].sum()

        start_m3w = df.loc[i,'sales_date_ymd'] - timedelta(days=21)
        end_m3w = df.loc[i,'sales_date_ymd'] - timedelta(days=15)
        temp_df_m3w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m3w) & (df_cust_orderday['주문일'] <= end_m3w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m3w_sales = temp_df_m3w['매출'].sum()

        start_m4w = df.loc[i,'sales_date_ymd'] - timedelta(days=28)
        end_m4w = df.loc[i,'sales_date_ymd'] - timedelta(days=22)
        temp_df_m4w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m4w) & (df_cust_orderday['주문일'] <= end_m4w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m4w_sales = temp_df_m4w['매출'].sum()

        start_m5w = df.loc[i,'sales_date_ymd'] - timedelta(days=35)
        end_m5w = df.loc[i,'sales_date_ymd'] - timedelta(days=29)
        temp_df_m5w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m5w) & (df_cust_orderday['주문일'] <= end_m5w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m5w_sales = temp_df_m5w['매출'].sum()

        start_m6w = df.loc[i,'sales_date_ymd'] - timedelta(days=42)
        end_m6w = df.loc[i,'sales_date_ymd'] - timedelta(days=36)
        temp_df_m6w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m6w) & (df_cust_orderday['주문일'] <= end_m6w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m6w_sales = temp_df_m6w['매출'].sum()

        start_m7w = df.loc[i,'sales_date_ymd'] - timedelta(days=49)
        end_m7w = df.loc[i,'sales_date_ymd'] - timedelta(days=43)
        temp_df_m7w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m7w) & (df_cust_orderday['주문일'] <= end_m7w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m7w_sales = temp_df_m7w['매출'].sum()

        start_m8w = df.loc[i,'sales_date_ymd'] - timedelta(days=56)
        end_m8w = df.loc[i,'sales_date_ymd'] - timedelta(days=50)
        temp_df_m8w = df_cust_orderday[(df_cust_orderday['주문일'] >= start_m8w) & (df_cust_orderday['주문일'] <= end_m8w) & (df_cust_orderday['CUST_ID']== df.loc[i,'CUST_ID'])]
        m8w_sales = temp_df_m8w['매출'].sum()


        ## 값 리스트에 넣기

        sales_no_list.append(df.loc[i, 'sales_no'])
        cust_id_list.append(df.loc[i,'CUST_ID'])
        sales_param_list.append(df.loc[i, 'activity_option'])
        activ_day_list.append(df.loc[i, 'sales_date_ymd'])
        activities_list.append(df.loc[i, 'sales_activities'])

        p1w_sales_list.append(p1w_sales)
        p2w_sales_list.append(p2w_sales)
        p3w_sales_list.append(p3w_sales)
        p4w_sales_list.append(p4w_sales)
        p5w_sales_list.append(p5w_sales)
        p6w_sales_list.append(p6w_sales)
        p7w_sales_list.append(p7w_sales)
        p8w_sales_list.append(p8w_sales)

        m1w_sales_list.append(m1w_sales)
        m2w_sales_list.append(m2w_sales)
        m3w_sales_list.append(m3w_sales)
        m4w_sales_list.append(m4w_sales)
        m5w_sales_list.append(m5w_sales)
        m6w_sales_list.append(m6w_sales)
        m7w_sales_list.append(m7w_sales)
        m8w_sales_list.append(m8w_sales)


    ## DF 생성
    df_range_sales = pd.DataFrame( {'sales_no' : sales_no_list
                                        , 'CUST_ID' : cust_id_list
                                        , 'activity_option' : sales_param_list
                                        , 'sales_activities' : activities_list
                                        , 'sales_date_ymd' : activ_day_list
                                        , 'm8w_sales' : m8w_sales_list
                                        , 'm7w_sales' : m7w_sales_list
                                        , 'm6w_sales' : m6w_sales_list
                                        , 'm5w_sales' : m5w_sales_list
                                        , 'm4w_sales' : m4w_sales_list
                                        , 'm3w_sales' : m3w_sales_list
                                        , 'm2w_sales' : m2w_sales_list
                                        , 'm1w_sales' : m1w_sales_list        
                                        , 'p1w_sales' : p1w_sales_list
                                        , 'p2w_sales' : p2w_sales_list
                                        , 'p3w_sales' : p3w_sales_list
                                        , 'p4w_sales' : p4w_sales_list
                                        , 'p5w_sales' : p5w_sales_list
                                        , 'p6w_sales' : p6w_sales_list
                                        , 'p7w_sales' : p7w_sales_list
                                        , 'p8w_sales' : p8w_sales_list
                                        }
                                    )
        
    return df_range_sales

cust_sales_pg = ragne_sales(df_pg_sales_3col)
cust_sales_pg.head()
## 주차별 SKU 구하기
df_cust_order_complete
df_cust_sku = df_cust_order_complete[['CUST_ID', '주문일', 'PROD_CD']]
df_cust_sku.reset_index(inplace=True, drop=True)
df_cust_sku
def ragne_sku(df):
    cust_id_list = []
    sales_no_list = []
    sku_param_list = []
    activities_list = []
    activ_day_list = []

    p1w_sku_list = []
    p2w_sku_list = []
    p3w_sku_list = []
    p4w_sku_list = []
    p5w_sku_list = []
    p6w_sku_list = []
    p7w_sku_list = []
    p8w_sku_list = []

    m1w_sku_list = []
    m2w_sku_list = []
    m3w_sku_list = []
    m4w_sku_list = []
    m5w_sku_list = []
    m6w_sku_list = []
    m7w_sku_list = []
    m8w_sku_list = []

    for i in range(len(df)):

        ## 세일링 이후

        start_p1w = df.loc[i,'sales_date_ymd'] 
        end_p1w = df.loc[i,'sales_date_ymd'] + timedelta(days=6)
        temp_df_p1w = df_cust_sku[(df_cust_sku['주문일'] >= start_p1w) & (df_cust_sku['주문일'] <= end_p1w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p1w_sku = temp_df_p1w['PROD_CD'].nunique()

        start_p2w = df.loc[i,'sales_date_ymd'] + timedelta(days=7)
        end_p2w = df.loc[i,'sales_date_ymd'] + timedelta(days=13)
        temp_df_p2w = df_cust_sku[(df_cust_sku['주문일'] >= start_p2w) & (df_cust_sku['주문일'] <= end_p2w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p2w_sku = temp_df_p2w['PROD_CD'].nunique()

        start_p3w = df.loc[i,'sales_date_ymd'] + timedelta(days=14)
        end_p3w = df.loc[i,'sales_date_ymd'] + timedelta(days=20)
        temp_df_p3w = df_cust_sku[(df_cust_sku['주문일'] >= start_p3w) & (df_cust_sku['주문일'] <= end_p3w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p3w_sku = temp_df_p3w['PROD_CD'].nunique()

        start_p4w = df.loc[i,'sales_date_ymd'] + timedelta(days=21)
        end_p4w = df.loc[i,'sales_date_ymd'] + timedelta(days=27)
        temp_df_p4w = df_cust_sku[(df_cust_sku['주문일'] >= start_p4w) & (df_cust_sku['주문일'] <= end_p4w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p4w_sku = temp_df_p4w['PROD_CD'].nunique()

        start_p5w = df.loc[i,'sales_date_ymd'] + timedelta(days=28)
        end_p5w = df.loc[i,'sales_date_ymd'] + timedelta(days=34)
        temp_df_p5w = df_cust_sku[(df_cust_sku['주문일'] >= start_p5w) & (df_cust_sku['주문일'] <= end_p5w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p5w_sku = temp_df_p5w['PROD_CD'].nunique()

        start_p6w = df.loc[i,'sales_date_ymd'] + timedelta(days=35)
        end_p6w = df.loc[i,'sales_date_ymd'] + timedelta(days=41)
        temp_df_p6w = df_cust_sku[(df_cust_sku['주문일'] >= start_p6w) & (df_cust_sku['주문일'] <= end_p6w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p6w_sku = temp_df_p6w['PROD_CD'].nunique()

        start_p7w = df.loc[i,'sales_date_ymd'] + timedelta(days=42)
        end_p7w = df.loc[i,'sales_date_ymd'] + timedelta(days=48)
        temp_df_p7w = df_cust_sku[(df_cust_sku['주문일'] >= start_p7w) & (df_cust_sku['주문일'] <= end_p7w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p7w_sku = temp_df_p7w['PROD_CD'].nunique()

        start_p8w = df.loc[i,'sales_date_ymd'] + timedelta(days=49)
        end_p8w = df.loc[i,'sales_date_ymd'] + timedelta(days=55)
        temp_df_p8w = df_cust_sku[(df_cust_sku['주문일'] >= start_p8w) & (df_cust_sku['주문일'] <= end_p8w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        p8w_sku = temp_df_p8w['PROD_CD'].nunique()


        ## 세일링 이전 

        start_m1w = df.loc[i,'sales_date_ymd'] - timedelta(days=7)
        end_m1w = df.loc[i,'sales_date_ymd'] - timedelta(days=1)
        temp_df_m1w = df_cust_sku[(df_cust_sku['주문일'] >= start_m1w) & (df_cust_sku['주문일'] <= end_m1w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        m1w_sku = temp_df_m1w['PROD_CD'].nunique()

        start_m2w = df.loc[i,'sales_date_ymd'] - timedelta(days=14)
        end_m2w = df.loc[i,'sales_date_ymd'] - timedelta(days=8)
        temp_df_m2w = df_cust_sku[(df_cust_sku['주문일'] >= start_m2w) & (df_cust_sku['주문일'] <= end_m2w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        m2w_sku = temp_df_m2w['PROD_CD'].nunique()

        start_m3w = df.loc[i,'sales_date_ymd'] - timedelta(days=21)
        end_m3w = df.loc[i,'sales_date_ymd'] - timedelta(days=15)
        temp_df_m3w = df_cust_sku[(df_cust_sku['주문일'] >= start_m3w) & (df_cust_sku['주문일'] <= end_m3w) & (df_cust_sku['CUST_ID']== df.loc[i,'CUST_ID'])]
        m3w_sku = temp_df_m3w['PROD_CD'].nunique()

        start_m4w = df.loc[i,'sales_date_ymd'] - timedelta(days=28)
        end_m4w = df.loc[i,'sales_date_ymd'] - timedelta(days=22)
        temp_df_m4w = df_cust_order_complete[(df_cust_order_complete['주문일'] >= start_m4w) & (df_cust_order_complete['주문일'] <= end_m4w) & (df_cust_order_complete['CUST_ID']== df.loc[i,'CUST_ID'])]
        m4w_sku = temp_df_m4w['PROD_CD'].nunique()

        start_m5w = df.loc[i,'sales_date_ymd'] - timedelta(days=35)
        end_m5w = df.loc[i,'sales_date_ymd'] - timedelta(days=29)
        temp_df_m5w = df_cust_order_complete[(df_cust_order_complete['주문일'] >= start_m5w) & (df_cust_order_complete['주문일'] <= end_m5w) & (df_cust_order_complete['CUST_ID']== df.loc[i,'CUST_ID'])]
        m5w_sku = temp_df_m5w['PROD_CD'].nunique()

        start_m6w = df.loc[i,'sales_date_ymd'] - timedelta(days=42)
        end_m6w = df.loc[i,'sales_date_ymd'] - timedelta(days=36)
        temp_df_m6w = df_cust_order_complete[(df_cust_order_complete['주문일'] >= start_m6w) & (df_cust_order_complete['주문일'] <= end_m6w) & (df_cust_order_complete['CUST_ID']== df.loc[i,'CUST_ID'])]
        m6w_sku = temp_df_m6w['PROD_CD'].nunique()

        start_m7w = df.loc[i,'sales_date_ymd'] - timedelta(days=49)
        end_m7w = df.loc[i,'sales_date_ymd'] - timedelta(days=43)
        temp_df_m7w = df_cust_order_complete[(df_cust_order_complete['주문일'] >= start_m7w) & (df_cust_order_complete['주문일'] <= end_m7w) & (df_cust_order_complete['CUST_ID']== df.loc[i,'CUST_ID'])]
        m7w_sku = temp_df_m7w['PROD_CD'].nunique()

        start_m8w = df.loc[i,'sales_date_ymd'] - timedelta(days=56)
        end_m8w = df.loc[i,'sales_date_ymd'] - timedelta(days=50)
        temp_df_m8w = df_cust_order_complete[(df_cust_order_complete['주문일'] >= start_m8w) & (df_cust_order_complete['주문일'] <= end_m8w) & (df_cust_order_complete['CUST_ID']== df.loc[i,'CUST_ID'])]
        m8w_sku = temp_df_m8w['PROD_CD'].nunique()


        ## 값 리스트에 넣기

        cust_id_list.append(df.loc[i,'CUST_ID'])
        sales_no_list.append(df.loc[i, 'sales_no'])
        sku_param_list.append(df.loc[i, 'activity_option'])
        activities_list.append(df.loc[i, 'sales_activities'])
        activ_day_list.append(df.loc[i, 'sales_date_ymd'])

        p1w_sku_list.append(p1w_sku)
        p2w_sku_list.append(p2w_sku)
        p3w_sku_list.append(p3w_sku)
        p4w_sku_list.append(p4w_sku)
        p5w_sku_list.append(p5w_sku)
        p6w_sku_list.append(p6w_sku)
        p7w_sku_list.append(p7w_sku)
        p8w_sku_list.append(p8w_sku)

        m1w_sku_list.append(m1w_sku)
        m2w_sku_list.append(m2w_sku)
        m3w_sku_list.append(m3w_sku)
        m4w_sku_list.append(m4w_sku)
        m5w_sku_list.append(m5w_sku)
        m6w_sku_list.append(m6w_sku)
        m7w_sku_list.append(m7w_sku)
        m8w_sku_list.append(m8w_sku)


    ## DF 생성
    df_range_sku = pd.DataFrame( {'sales_no' : sales_no_list
                                        , 'CUST_ID' : cust_id_list
                                        , 'activity_option' : sku_param_list
                                        , 'sales_activities' : activities_list
                                        , 'sales_date_ymd' : activ_day_list
                                        , 'm8w_sku' : m8w_sku_list
                                        , 'm7w_sku' : m7w_sku_list
                                        , 'm6w_sku' : m6w_sku_list
                                        , 'm5w_sku' : m5w_sku_list
                                        , 'm4w_sku' : m4w_sku_list
                                        , 'm3w_sku' : m3w_sku_list
                                        , 'm2w_sku' : m2w_sku_list
                                        , 'm1w_sku' : m1w_sku_list        
                                        , 'p1w_sku' : p1w_sku_list
                                        , 'p2w_sku' : p2w_sku_list
                                        , 'p3w_sku' : p3w_sku_list
                                        , 'p4w_sku' : p4w_sku_list
                                        , 'p5w_sku' : p5w_sku_list
                                        , 'p6w_sku' : p6w_sku_list
                                        , 'p7w_sku' : p7w_sku_list
                                        , 'p8w_sku' : p8w_sku_list
                                        }
                                    )
        
    return df_range_sku

cust_sku_pg = ragne_sku(df_pg_sales_3col)
cust_sku_pg.head()
## Merge
df_pg_sales_history.head()
cust_sales_sku_pg = pd.merge(cust_sales_pg, cust_sku_pg, how='left', on=['sales_no', 'CUST_ID', 'activity_option', 'sales_activities', 'sales_date_ymd'])
cust_sales_sku_pg_admin = pd.merge(cust_sales_sku_pg, df_pg_sales_history[['sales_no', 'insert_id']], how='left', on= 'sales_no')
cust_sales_sku_pg_admin.head()
update_table = pd.merge(cust_sales_sku_pg_admin, df_cust_info, how='left', on='CUST_ID')
update_table_fn = update_table[~update_table['매장명'].isna()]
update_table_fn.head()
update_table_fn.info()
update_table_fn = update_table_fn[['sales_no', 'CUST_ID', '매장명', '업종', '지역구', '가입일', '첫발주일', 'insert_id', 'activity_option', 'sales_activities', 'sales_date_ymd'
                                   , 'm8w_sales', 'm8w_sku', 'm7w_sales', 'm7w_sku', 'm6w_sales', 'm6w_sku', 'm5w_sales', 'm5w_sku'
                                   , 'm4w_sales', 'm4w_sku', 'm3w_sales', 'm3w_sku', 'm2w_sales', 'm2w_sku', 'm1w_sales', 'm1w_sku'
                                   , 'p1w_sales', 'p1w_sku', 'p2w_sales', 'p2w_sku', 'p3w_sales', 'p3w_sku', 'p4w_sales', 'p4w_sku'
                                   , 'p5w_sales', 'p5w_sku', 'p6w_sales', 'p6w_sku', 'p7w_sales', 'p7w_sku', 'p8w_sales', 'p8w_sku']]

update_table_fn.rename(columns={'매장명':'BUSINESS_NAME'
                                , '업종': 'ER_CTG_NAME'
                                , '지역구' : 'AREA_GU'
                                , '가입일' : 'REG_DATE_TC'
                                , '첫발주일' : 'FIRST_ORDER_DATE'
                                }, inplace=True)
update_table_fn.head()
update_table_fn.info()
update_table_fn.tail()
### 중복 sales_no 제거
df_insert_table = update_table_fn[update_table_fn['sales_no'] > max_sales_no]
df_insert_table
# - test
# Connect DB
import urllib.parse
from sqlalchemy import create_engine


user = 'orderhero'
pwd_ = '@hejgl@jfh)1)3'
pwd = urllib.parse.quote(pwd_)
host = '175.126.82.194:3308'
db = 'db_base'

engine = create_engine(f'mysql+pymysql://{user}:{pwd}@{host}/{db}')
df_insert_table.to_sql('TB_SALES_GRAFANA', con=engine, if_exists='append', index=False)
### Real
# Connect DB
import urllib.parse
from sqlalchemy import create_engine


user = 'orderherodl'
pwd_ = 'OhejGL@JFH2023'
pwd = urllib.parse.quote(pwd_)
host = 'orderherodl.cafe24.com'
db = 'orderherodl'

engine = create_engine(f'mysql+pymysql://{user}:{pwd}@{host}/{db}')
df_insert_table.to_sql('TB_SALES_GRAFANA', con=engine, if_exists='append', index=False)
