# Python 3.6

"""
ULTAB_SHP_WEIXIN_USER
ULTAB_BO_VOUCHERINFO
ULTAB_BO_VOUCHERLS
ULTAB_SHP_WEIXIN_USER_EVENT
"""

# 数据库连接配置 格式： user/pass@host:port/orcl
db_config = 'unionlive/unionlive@proxy.unionlive.com:1521/orcl'
# 数据库SQL语句代码块,脚本会自动加分页查询
db_sql = '''
SELECT * FROM ULTAB_SYS_USER
'''
# 分页大小，每页处理数量
db_page_size = 5
# ES服务地址与索引名称 格式：http://host:port/index/type
es_index = 'http://192.168.200.112:9200/sysuser/user'
# 查询结果列名，必须唯一,不能存在中文，多个会做字符串拼接
es_index_ids = ['ID']
# 代理访问ES服务,本地测试时使用，ES服务可以直接连接时注释此配置
proxies = {'http': 'http://211.152.57.28:39083'}

import cx_Oracle as oracle
import requests
import json, datetime


## DB Select
def db_select(conn, sql):
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


# ES index
def es_index_put(dataList,proxies):
    headers = {'Content-Type': 'application/json'}
    for d in dataList:
        if not vars().get('proxies'): proxies = None
        eid = ''
        for c in es_index_ids:
            eid = eid + str(d[c]).strip()
        res = requests.put('%s/%s' % (es_index, eid), proxies=proxies, headers=headers, data=json.dumps(d))
        print("ES status_code:", res.status_code, " ResText:", res.text)


print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
print('>> 开始执行 ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
page_begin = 1
while True:
    page_end = page_begin + db_page_size
    query_sql = 'SELECT * FROM ( SELECT t.*, ROWNUM as RN  FROM ( %s ) t  WHERE ROWNUM < %s )  WHERE RN >= %s' % (
        db_sql, page_end, page_begin)
    page_begin = page_end
    print('>> 执行SQL：', query_sql)
    conn = oracle.connect(db_config)
    dataList = db_select(conn, query_sql)
    print('>> SQL查询结果数量:', dataList.__len__())
    if dataList.__len__() == 0:
        break
    es_index_put(dataList,proxies)
print('>> 执行完成！ ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
