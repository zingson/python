# Python 3.6

import cx_Oracle as oracle
import requests


## SQL查询
def select(conn, sql):
    curs = conn.cursor()
    curs.execute(sql)
    rows = curs.fetchall()
    title = [i[0] for i in curs.description]
    cols = {}
    for i in range(len(title)):
        cols[i] = title[i]

    results = []
    for obj in rows:
        row = {}
        for i in range(len(obj)):
            row[cols[i]] = obj[i]
        results.append(row)

    curs.close()
    conn.close()

    return results


conn = oracle.connect('unionlive/unionlive@proxy.unionlive.com:1521/orcl')
dataList = select(conn, "SELECT * from ULTAB_SYS_USER")
print(dataList)

## 插入数据到ES
es_index = '''
{
  "query": {
    "match_all": {}
  },
  "from": 0,
  "size": 2
}
'''
proxies = {'http': 'http://211.152.57.28:39083'}
headers = {'Content-Type':'application/json'}
res = requests.post('http://192.168.200.112:9200/shop/_search',proxies=proxies,headers=headers,data=es_index,verify=False)
print("status_code:",res.status_code)
print(res.text)
