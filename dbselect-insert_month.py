from multiprocessing import Pool , TimeoutError

import time
import psycopg2
import calendar


def f(x):
    con = psycopg2.connect("host='localhost' dbname='testdb' user='postgres' password='root'")
    cur = con.cursor()
    print "c0nn g00d"
    print(x)
    m = x
    if(m == 4 or m == 6 or m == 9 or m == 11):
        s = "2018-"+str(x)+"-1 00:00:00 " 
        e = "2018-"+str(x)+"-30 23:59:59.999999"
        print type(s)
        print s
        
        cur.execute("select * from invoice_2018_sec where created_at > %s and created_at < %s" , (s,e))
        row = cur.fetchall()
        for rows in row:
            print rows
            iid = rows[0]
            u_id = rows[1]
            i_id = rows[2]
            p = rows[3]
            t_i_p = rows[4]
            d_p = rows[5]
            d_a = rows[6]
            typeitem = rows[7]
            t_id = rows[8]
            d_m = rows[9]
            i_r = rows[10]
            print rows[11]
            c_at = rows[11]
            print type(c_at)
            month = str(m)
            cur.execute("insert into invoice_2018_"+month+"_sec (id , user_id , item_id, price, tax_in_percent, discount_percent, discount_amount , type, transaction_id, deduct_money, is_redeem , created_at)  values (%s , %s,%s , %s , %s , %s  , %s , %s, %s, %s , %s, %s)", (iid ,u_id, i_id, p , t_i_p, d_p,d_a, typeitem, t_id, d_m, i_r, c_at))
            prin("insert")

            con.commit()


    elif( m == 2 ):
        s = "2018-"+str(x)+"-1 00:00:00 "
        e = "2018-"+str(x)+"-28 23:59:59.999999"
        cur.execute("select * from invoice_2018_sec where created_at > %s and created_at < %s" , (s,e))
        row = cur.fetchall()
        for rows in row:
            print rows
            iid = rows[0]
            u_id = rows[1]
            i_id = rows[2]
            p = rows[3]
            t_i_p = rows[4]
            d_p = rows[5]
            d_a = rows[6]
            typeitem = rows[7]
            t_id = rows[8]
            d_m = rows[9]
            i_r = rows[10]
            print rows[11]
            c_at = rows[11]
            print type(c_at)
            month = str(m)
            cur.execute("insert into invoice_2018_"+month+"_sec (id , user_id , item_id, price, tax_in_percent, discount_percent, discount_amount , type, transaction_id, deduct_money, is_redeem , created_at)  values (%s , %s,%s , %s , %s , %s  , %s , %s, %s, %s , %s, %s)", (iid ,u_id, i_id, p , t_i_p, d_p,d_a, typeitem, t_id, d_m, i_r, c_at))
            print("insert")

            con.commit()

    else:
        s = "2018-"+str(x)+"-1 00:00:00 "
        e = "2018-"+str(x)+"-31 23:59:59.999999"
        cur.execute("select * from invoice_2018_sec where created_at > %s and created_at < %s" , (s,e))
        row = cur.fetchall()
        for rows in row:
            print rows
            iid = rows[0]
            u_id = rows[1]
            i_id = rows[2]
            p = rows[3]
            t_i_p = rows[4]
            d_p = rows[5]
            d_a = rows[6]
            typeitem = rows[7]
            t_id = rows[8]
            d_m = rows[9]
            i_r = rows[10]
            print rows[11]
            c_at = rows[11]
            print type(c_at)
            month = str(m)
            cur.execute("insert into invoice_2018_"+month+"_sec (id , user_id , item_id, price, tax_in_percent, discount_percent, discount_amount , type, transaction_id, deduct_money, is_redeem , created_at)  values (%s , %s,%s , %s , %s , %s  , %s , %s, %s, %s , %s, %s)", (iid ,u_id, i_id, p , t_i_p, d_p,d_a, typeitem, t_id, d_m, i_r, c_at))
            print("insert")

            con.commit()

    cur.close() 
if __name__ == '__main__':
    pool = Pool(processes=12)
    pool.map(f, [1 , 2, 3, 4, 5 , 6, 7, 8 ,9, 10,11,12])
    
