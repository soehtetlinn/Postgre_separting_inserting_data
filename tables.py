import psycopg2
import calendar

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

con = psycopg2.connect(dbname='testdb', user='postgres', host='localhost', password='root')

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = con.cursor()
year = raw_input("enter year")

print year


cur.execute("create table invoice_"+year+ "_sec (id serial primary key,user_id int,item_id int,price float,tax_in_percent int,discount_percent int,discount_amount float,type int,transaction_id int,deduct_money float,is_redeem varchar,created_at timestamp default now(),updated_at timestamp default now() )")

m = 1
while(m < 13):
    cur.execute("create table invoice_"+year+"_"+str(m)+"_sec (id serial primary key,user_id int,item_id int,price float,tax_in_percent int,discount_percent int,discount_amount float,type int,transaction_id int,deduct_money float,is_redeem varchar,created_at timestamp default now(),updated_at timestamp default now() )")   
    if(m == 4 or m == 5 or m == 9 or m == 11):
        d = 1
        while( d < 31 ):
            cur.execute("create table invoice_"+year+"_"+str(m)+"_"+str(d)+"_sec (id serial primary key,user_id int,item_id int,price float,tax_in_percent int,discount_percent int,discount_amount float,type int,transaction_id int,deduct_money float,is_redeem varchar,created_at timestamp default now(),updated_at timestamp default now() )")
            d = d + 1
    elif ( m == 2 ):
        if (calendar.isleap(int(year))):
            d = 1 
            while ( d < 30 ):
                cur.execute("create table invoice_"+year+"_"+str(m)+"_"+str(d)+"_sec (id serial primary key,user_id int,item_id int,price float,tax_in_percent int,discount_percent int,discount_amount float,type int,transaction_id int,deduct_money float,is_redeem varchar,created_at timestamp default now(),updated_at timestamp default now() )")
                d = d + 1
        else:
            d = 1 
            while ( d < 29 ):
                cur.execute("create table invoice_"+year+"_"+str(m)+"_"+str(d)+"_sec (id serial primary key,user_id int,item_id int,price float,tax_in_percent int,discount_percent int,discount_amount float,type int,transaction_id int,deduct_money float,is_redeem varchar,created_at timestamp default now(),updated_at timestamp default now() )")
                d = d + 1
    else:
        d = 1 
        while ( d < 32 ):
            cur.execute("create table invoice_"+year+"_"+str(m)+"_"+str(d)+"_sec (id serial primary key,user_id int,item_id int,price float,tax_in_percent int,discount_percent int,discount_amount float,type int,transaction_id int,deduct_money float,is_redeem varchar,created_at timestamp default now(),updated_at timestamp default now() )")
            d = d + 1

    m = m + 1
