# Python 3.6

# 数据库连接配置 格式： user/pass@host:port/orcl
db_config = 'unionlive/unionlive@proxy.unionlive.com:1521/orcl'
# 数据库SQL语句代码块
db_sql = '''
SELECT * 
FROM ULTAB_SYS_USER

'''
# ES服务地址与索引名称 格式：http://host:port/index/type
es_index = 'http://192.168.200.112:9200/sysuser/user'
# 查询结果列名，必须唯一
es_index_id = 'ID'
# 代理访问ES服务,本地测试时使用，ES服务可以直接连接时注释此配置
es_proxies = {'http': 'http://211.152.57.28:39083'}

import cx_Oracle as oracle
import requests
import json, datetime


## DB Select
def select(conn, sql):
    curs = conn.cursor()
    curs.execute(sql)
    rows = curs.fetchall()
    cols = [i[0] for i in curs.description]
    results = []
    for obj in rows:
        row = {}
        for i in range(len(obj)):
            row[cols[i]] = obj[i]
        results.append(row)
    curs.close()
    conn.close()
    return results


print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
print('>> 开始执行 ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
conn = oracle.connect(db_config)
dataList = select(conn, db_sql)
print('>> SQL查询结果数量:', dataList.__len__())
headers = {'Content-Type': 'application/json'}
for d in dataList:
    if not vars().get('es_proxies'): es_proxies = None
    res = requests.put('%s/%s' % (es_index, d[es_index_id]), proxies=es_proxies, headers=headers, data=json.dumps(d))
    print("ES status_code:", res.status_code, " ResText:", res.text)
print('>> 执行完成！ ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
