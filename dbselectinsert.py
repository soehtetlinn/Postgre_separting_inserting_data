from multiprocessing import Pool , TimeoutError

import time
import psycopg2
import calendar


def f(x):
    con = psycopg2.connect("host='localhost' dbname='test' user='pythonspot' password='123'")
    cur = con.cursor()
    print "c0nn g00d"
    print(x)
    m = x
    if(m == 4 or m == 6 or m == 9 or m == 11):
        s = "2018-"+str(x)+"-1 00:00:00 " 
        e = "2018-"+str(x)+"-30 23:59:59.999999"
        print type(s)
        print s
        
        month = str(m)
        cur.execute("insert into invoice_2018_"+month+"_sec select * from invoice_2018_sec where created_at > %s and created_at < %s", (s,e))
        con.commit()
        print ("insert")


    elif( m == 2 ):
        s = "2018-"+str(x)+"-1 00:00:00 "
        e = "2018-"+str(x)+"-28 23:59:59.999999"
        month = str(m)
        cur.execute("insert into invoice_2018_"+month+"_sec select * from invoice_2018_sec where created_at > %s and created_at < %s",(s,e))
        print("insert")
        con.commit()

    else:
        s = "2018-"+str(x)+"-1 00:00:00 "
        e = "2018-"+str(x)+"-31 23:59:59.999999"
        month = str(m)
        cur.execute("insert into invoice_2018_"+month+"_sec elect * from invoice_2018_sec where created_at > %s and created_at < %s" , (s,e))
        print("insert")
        con.commit()

    cur.close() 

def insert_yeartomonth():
    pool = Pool(processes=12)
    pool.map(f, [1 , 2, 3, 4, 5 , 6, 7, 8 ,9, 10,11,12])
    
