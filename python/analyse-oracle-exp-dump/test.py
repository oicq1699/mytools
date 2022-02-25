import cx_Oracle
#连接数据库，下面括号里内容根据自己实际情况填写
conn = cx_Oracle.connect('tztest/tz@10.150.20.30:1521/orcl')
cursor = conn.cursor()  

cursor.execute("select 'hello', 'world!' from dual")  
rows = cursor.fetchall()
for row in rows:
    print(row[0], row[1])
cursor.close()
conn.close()