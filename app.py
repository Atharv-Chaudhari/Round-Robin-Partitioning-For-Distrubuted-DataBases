from flask import Flask, request, url_for, redirect, render_template
from flask_mysqldb import MySQL
import mysql.connector
import html
import collections
import logging

app = Flask(__name__)

mydb = mysql.connector.connect(
    host="remotemysql.com",
    user="Sni3fRlWcx",
    password="vwRjyIQ4WE",
    database="Sni3fRlWcx"
)


@app.route('/')
def database():
    mydb = mysql.connector.connect(
        host="remotemysql.com",
        user="Sni3fRlWcx",
        password="vwRjyIQ4WE",
        database="Sni3fRlWcx"
    )
    mydb2 = mysql.connector.connect(
        host="remotemysql.com",
        user="4NaN7QJzov ",
        password="epsIAkXHKE",
        database="4NaN7QJzov"
    )
    mycursor2 = mydb2.cursor()
    mycursor2.execute("""SELECT CONCAT('DROP TABLE IF EXISTS ', TABLE_NAME, ';') FROM INFORMATION_SCHEMA.tables WHERE TABLE_SCHEMA = '4NaN7QJzov'""")
    mycursor = mydb.cursor()
    tables_names = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='Sni3fRlWcx'"
    mycursor.execute(tables_names)
    tables = mycursor.fetchall()
    myresult = []
    columns = []
    for i in tables:
        names = "SELECT * FROM "+i[0]
        mycursor.execute(names)
        myresult.append(mycursor.fetchall())
        names = "SELECT Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= '" + \
            i[0]+"' ORDER BY ORDINAL_POSITION"
        mycursor.execute(names)
        columns.append(mycursor.fetchall())
    return render_template('index.html', table=tables, column=columns, data=myresult, n=len(tables), table1=[], column1=[], data1=[], n1=0,f=0,cols=[],data2=[],method="")


@app.route('/partition', methods=['POST', 'GET'])
def partition():
    mydb1 = mysql.connector.connect(
        host="remotemysql.com",
        user="4NaN7QJzov ",
        password="epsIAkXHKE",
        database="4NaN7QJzov"
    )
    mycursor1 = mydb1.cursor()
    print(mycursor1)
    mydb = mysql.connector.connect(
        host="remotemysql.com",
        user="Sni3fRlWcx",
        password="vwRjyIQ4WE",
        database="Sni3fRlWcx"
    )
    mycursor = mydb.cursor()
    tables_names = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='Sni3fRlWcx'"
    mycursor.execute(tables_names)
    tables = mycursor.fetchall()
    myresult = []
    columns = []
    for i in tables:
        names = "SELECT * FROM "+i[0]
        mycursor.execute(names)
        myresult.append(mycursor.fetchall())
        names = "SELECT Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= '" + \
            i[0]+"' ORDER BY ORDINAL_POSITION"
        mycursor.execute(names)
        columns.append(mycursor.fetchall())
    total = len(myresult[0])
    z = 3
    for j in range(z):
        mycursor1.execute("DROP TABLE IF EXISTS partition_"+str(j+1))
        s = ""
        for i in columns[0]:
            s = s+"`"+i[0]+"`"+" "+"VARCHAR (50),"
        mycursor1.execute("Create table partition_"+str(j+1)+" ("+s[:-1]+");")
    for i in range(1, total+1):
        if(i % z == 0):
            mycursor1.execute("INSERT INTO `partition_"+str(z) +
                              "` VALUES ('"+str("','".join(myresult[0][i-1]))+"')")
            continue
        mycursor1.execute("INSERT INTO `partition_"+str(((i) % z)) +
                          "` VALUES ('"+str("','".join(myresult[0][i-1]))+"')")
    mydb1.commit()
    tables_names1 = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='4NaN7QJzov'"
    mycursor1.execute(tables_names1)
    tables1 = mycursor1.fetchall()
    myresult1 = []
    columns1 = []
    for i in tables1:
        names1 = "SELECT * FROM "+i[0]
        mycursor1.execute(names1)
        myresult1.append(mycursor1.fetchall())
        names1 = "SELECT Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= '" + \
            i[0]+"' ORDER BY ORDINAL_POSITION"
        mycursor1.execute(names1)
        columns1.append(mycursor1.fetchall())
    return render_template('index.html', table=[], column=[], data=[], n=0, table1=tables1, column1=columns1, data1=myresult1, n1=len(tables1),f=0,cols=[],data2=[],method="")

@app.route('/point_search', methods=['POST', 'GET'])
def point_search():
    mydb1 = mysql.connector.connect(
        host="remotemysql.com",
        user="4NaN7QJzov ",
        password="epsIAkXHKE",
        database="4NaN7QJzov"
    )
    mycursor1 = mydb1.cursor()
    mycursor1.execute("show tables")
    no_tables=len(mycursor1.fetchall())
    names = "SELECT Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= 'partition_1' ORDER BY ORDINAL_POSITION"
    mycursor1.execute(names)
    cols=mycursor1.fetchall()
    column_name="Id"
    id=1
    data=[]
    for i in range(1,no_tables+1):
        mycursor1.execute("select * from partition_"+str(i)+" where "+column_name+"='"+str(id)+"'")
        t=mycursor1.fetchall()
        if(len(t)!=0):
            data.append(t)
    return render_template('index.html', table=[], column=[], data=[], n=0, table1=[], column1=[], data1=[], n1=0,f=1,cols=cols,data2=data[0],method="Result Of Point Query")

@app.route('/range_search', methods=['POST', 'GET'])
def range_search():
    mydb1 = mysql.connector.connect(
        hhost="remotemysql.com",
        user="4NaN7QJzov ",
        password="epsIAkXHKE",
        database="4NaN7QJzov"
    )
    mycursor1 = mydb1.cursor()
    mycursor1.execute("show tables")
    no_tables=len(mycursor1.fetchall())
    names = "SELECT Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= 'partition_1' ORDER BY ORDINAL_POSITION"
    mycursor1.execute(names)
    cols=mycursor1.fetchall()
    column_name="Id"
    range1=10
    range2=40
    data=[]
    for i in range(1,no_tables+1):
        mycursor1.execute("select * from partition_"+str(i)+" where "+column_name+" BETWEEN "+str(range1)+" AND "+str(range2))
        t=mycursor1.fetchall()
        if(len(t)!=0):
            data=data+t
    print(data)
    return render_template('index.html', table=[], column=[], data=[], n=0, table1=[], column1=[], data1=[], n1=0,f=2,cols=cols,data2=data,method="Result Of Range Query")

@app.route('/all_scan_search', methods=['POST', 'GET'])
def all_scan_search():
    mydb1 = mysql.connector.connect(
        host="remotemysql.com",
        user="4NaN7QJzov ",
        password="epsIAkXHKE",
        database="4NaN7QJzov"
    )
    mycursor1 = mydb1.cursor()
    mycursor1.execute("show tables")
    no_tables=len(mycursor1.fetchall())
    names = "SELECT Column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME= 'partition_1' ORDER BY ORDINAL_POSITION"
    mycursor1.execute(names)
    cols=mycursor1.fetchall()
    column_name="Species"
    value="Iris-setosa"
    data=[]
    for i in range(1,no_tables+1):
        mycursor1.execute("select * from partition_"+str(i)+" where "+column_name+"='"+str(value)+"'")
        t=mycursor1.fetchall()
        if(len(t)!=0):
            data=data+t
    return render_template('index.html', table=[], column=[], data=[], n=0, table1=[], column1=[], data1=[], n1=0,f=3,cols=cols,data2=data,method="Result Of All Scan Query")

if __name__ == '__main__':
    app.run(debug=True)
