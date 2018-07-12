from multiprocessing import Pool, TimeoutError

import time
import psycopg2
import calendar


def f(x):
    con = psycopg2.connect("host='localhost' dbname='test' user='pythonspot' password='123'")
    cur = con.cursor()
    print x
    year = '2018'
    m = x
    if(m == 4 or m == 6 or m == 9 or m == 11):
        i = 1 
        while(i < 31):
            d ="invoice_"+ year+"_"+str(m)+"_sec"
            print d
            s = year+"-"+str(m)+"-"+str(i)+" 00:00:00"
            e = year+"-"+str(m)+"-"+str(i)+" 23:59:59.999999"
            print s
            print e
            ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
            print ins_date
            print ("inserting")
            cur.execute("insert into "+ins_date+"  select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
            con.commit()
            i = i + 1 
    elif( m == 2 ):
        i = 1
        if(calendar.isleap(int(year))):
            while(i < 30):
                d ="invoice_"+ year+"_"+str(m)+"_sec"
                print d
                s = year+"-"+str(m)+"-"+str(i)+" 00:00:00"
                e = year+"-"+str(m)+"-"+str(i)+" 23:59:59.999999"
                print s
                print e
                ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
                print ins_date
                print ("inserting")
                cur.execute("insert into "+ins_date+"  select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
                con.commit()

                i = i + 1 
        else:
            while(i < 29):
                d ="invoice_"+ year+"_"+str(m)+"_sec"
                print d
                s = year+"-"+str(m)+"-"+str(i)+" 00:00:00"
                e = year+"-"+str(m)+"-"+str(i)+" 23:59:59.999999"
                print s
                print e
                ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
                print ins_date
                print ("inserting")
                cur.execute("insert into "+ins_date+"  select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
                con.commit()
                i = i + 1
    else:
        i = 1 
        while(i < 32 ):
            d = "invoice_"+year+"_"+str(m)+"_sec"
            print d
            s = year+"-"+str(m)+"-"+str(i)+" 00:00:00"
            e = year+"-"+str(m)+"-"+str(i)+" 23:59:59.999999"
            print s
            print e
            ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
            print ins_date
            print ("inserting")
            cur.execute("insert into "+ins_date+"  select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
            con.commit()
            i = i + 1

 

    
def insert_monthtoday():
    pool = Pool(processes=12)
    pool.map(f,[1,2,3,4,5,6,7,8,9,10,11,12])
