from multiprocessing import Pool, TimeoutError

import time
import psycopg2
import calendar


def f(x):
    con = psycopg2.connect("host='localhost' dbname='testdb' user='postgres' password='root'")
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
            cur.execute("select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
            rows = cur.fetchall()
            print cur.rowcount
            for row in rows:
                iid = row[0]
                u_id = row[1]
                i_id = row[2]
                p = row[3]
                t_i_p = row[4]
                d_p = row[5]
                d_a = row[6]
                typeitem = row[7]
                t_id = row[8]
                d_m = row[9]
                i_r = row[10]
                c_at = row[11]
                ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
                print ins_date
                print ("inserting")
                cur.execute("insert into "+ins_date+"  (id, user_id, item_id, price, tax_in_percent, discount_percent, discount_amount, type, transaction_id , deduct_money, is_redeem, created_at) values ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ", (iid , u_id, i_id, p , t_i_p, d_p, d_a, typeitem, t_id, d_m, i_r, c_at ))
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
                cur.execute("select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
                rows = cur.fetchall()
                print cur.rowcount
                for row in rows:
                    iid = row[0]
                    u_id = row[1]
                    i_id = row[2]
                    p = row[3]
                    t_i_p = row[4]
                    d_p = row[5]
                    d_a = row[6]
                    typeitem = row[7]
                    t_id = row[8]
                    d_m = row[9]
                    i_r = row[10]
                    c_at = row[11]
                    ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
                    print ins_date
                    print ("inserting")
                    cur.execute("insert into "+ins_date+"  (id, user_id, item_id, price, tax_in_percent, discount_percent, discount_amount, type, transaction_id , deduct_money, is_redeem, created_at) values ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ", (iid , u_id, i_id, p , t_i_p, d_p, d_a, typeitem, t_id, d_m, i_r, c_at ))
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
                cur.execute("select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
                rows = cur.fetchall()
                print cur.rowcount
                for row in rows:
                    iid = row[0]
                    u_id = row[1]
                    i_id = row[2]
                    p = row[3]
                    t_i_p = row[4]
                    d_p = row[5]
                    d_a = row[6]
                    typeitem = row[7]
                    t_id = row[8]
                    d_m = row[9]
                    i_r = row[10]
                    c_at = row[11]
                    ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
                    print ins_date
                    print ("inserting")
                    cur.execute("insert into "+ins_date+"  (id, user_id, item_id, price, tax_in_percent, discount_percent, discount_amount, type, transaction_id , deduct_money, is_redeem, created_at) values ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ", (iid , u_id, i_id, p , t_i_p, d_p, d_a, typeitem, t_id, d_m, i_r, c_at ))
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
            cur.execute("select * from "+d+" where created_at > %s and created_at < %s " , (s, e))
            rows = cur.fetchall()
            print cur.rowcount
            for row in rows:
                iid = row[0]
                u_id = row[1]
                i_id = row[2]
                p = row[3]
                t_i_p = row[4]
                d_p = row[5]
                d_a = row[6]
                typeitem = row[7]
                t_id = row[8]
                d_m = row[9]
                i_r = row[10]
                c_at = row[11]
                ins_date = "invoice_2018_"+str(m)+"_"+str(i)+"_sec"
                print ins_date
                print ("inserting")
                cur.execute("insert into "+ins_date+"  (id, user_id, item_id, price, tax_in_percent, discount_percent, discount_amount, type, transaction_id , deduct_money, is_redeem, created_at) values ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ", (iid , u_id, i_id, p , t_i_p, d_p, d_a, typeitem, t_id, d_m, i_r, c_at ))
                con.commit()

            i = i + 1

 

    

if __name__ == '__main__':
    pool = Pool(processes=12)
    pool.map(f,[1,2,3,4,5,6,7,8,9,10,11,12])
