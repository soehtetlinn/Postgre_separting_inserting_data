from multiprocessing import Pool, TimeoutError
import time
import os
import string
import random
import psycopg2
import calendar

p = [1000,3000,5000, 10000]
tip = [ 5 , 10, 15, 20]
dp = [ 0.5 , 0.10 , 0.15, 0.20]
is_redeem_data = ['yes', 'no']

def dateTime(year):
    month = random.randint(01,12)

    if(month == 4 or month == 6 or month == 9 or month == 11):
        day = random.randint(01,30)
        hour = random.randint(00,23)
        miniut = random.randint(00,59)
        sec = random.randint(00, 59)
        a = random.randint(0,999999) 
        time = str(hour)+":"+str(miniut)+":"+str(sec)+"."+str(a)
        z = str(year)+"-"+str(month)+"-"+str(day)+" "+time
        return z
    elif( month == 2 ):
        if(calendar.isleap(int(year))):
            day = random.randint(01,29)
            hour = random.randint(00,23)
            miniut = random.randint(00,59)
            sec = random.randint(00, 59)
            a = random.randint(0,999999)
            time = str(hour)+":"+str(miniut)+":"+str(sec)+"."+str(a)
            z = str(year)+"-"+str(month)+"-"+str(day)+" "+time
            return z

        else:
            day = random.randint(01,28)
            hour = random.randint(00,23)
            miniut = random.randint(00,59)
            sec = random.randint(00, 59)
            a = random.randint(0,999999)
            time = str(hour)+":"+str(miniut)+":"+str(sec)+"."+str(a)
            z = str(year)+"-"+str(month)+"-"+str(day)+" "+time
            return z

    else:
        day = random.randint(01,31)
        hour = random.randint(00,23)
        miniut = random.randint(00,59)
        sec = random.randint(00, 59)
        a = random.randint(0,999999)
        time = str(hour)+":"+str(miniut)+":"+str(sec)+"."+str(a)
        z =  str(year)+"-"+str(month)+"-"+str(day)+" "+time
        return z



def f(x):
    i = x * 10000
    j = (x + 1) * 10000
    print i
    print j
    con  = psycopg2.connect("host='localhost' dbname='testdb' user='postgres' password='root'")
    cur = con.cursor()
    print "c0nn g00d"
    while i < j:
        user_id = random.randint(000000000,999999999)
        item_id = random.randint(0,100)
        price = random.choice(p)
        tax_in_percent = random.choice(tip)
        discount_percent = random.choice(dp)
        print discount_percent
        discount_amount = price * discount_percent
        typeitem = random.randint(0,3)
        transaction_id = random.randint(000000000, 999999999)
        deduct_money = price - discount_amount
        is_redeem = random.choice(is_redeem_data)
        c_time = str(dateTime(2018))
        cur.execute("insert into invoice_2018_sec (user_id , item_id, price, tax_in_percent, discount_percent, discount_amount , type, transaction_id, deduct_money, is_redeem , created_at)  values (%s,%s , %s , %s , %s  , %s , %s, %s, %s , %s, %s)", (user_id,item_id, price, tax_in_percent, discount_percent, discount_amount, typeitem , transaction_id, deduct_money, is_redeem, c_time))
        i = i + 1
    con.commit()


if __name__ == '__main__':
    pool = Pool(processes=10)

    pool.map(f, [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9])




