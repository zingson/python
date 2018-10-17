
# Python 3.6

import cx_Oracle



conn = cx_Oracle.connect('unionlive/unionlive@proxy.unionlive.com:1521/orcl')

curs=conn.cursor()
sql='SELECT * from ULTAB_SYS_USER'
rr=curs.execute (sql)
# row=curs.fetchone()
# print(row)
rows = curs.fetchall()


print(rows)
print(curs.description)
title = [i[0] for i in curs.description]
print(title)

# 列标题
cols = {}
for i in range(len(title)):
    cols[i] = title[i]
print(cols)

# 结果集
results = []
for obj in rows:
    row = {}
    for i in range(len(obj)):
        row[cols[i]] = obj[i]
    results.append(row)

print(results)

curs.close()
conn.close()

def select(conn,sql):
    curs = conn.cursor()
    curs.execute(sql)
    rows = curs.fetchall()
    title = [i[0] for i in curs.description]
    # 列标题
    cols = {}
    for i in range(len(title)):
        cols[i] = title[i]

    results = []
    for obj in rows:
        row = {}
        for i in range(len(obj)):
            row[cols[i]] = obj[i]
        results.append(row)


