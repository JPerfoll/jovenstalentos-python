from fastapi import FastAPI
import pandas as pd 
import datetime as dt
import json
import sqlite3 as sql
app = FastAPI()
@app.get("/")
async def hello():
    return "hello!"

@app.get("/products")
async def all_products():
    conexao = sql.connect('globalstore_normalizada.db')
    products = pd.read_sql(
        con=conexao, 
        sql='select * from products'
    )
    return products.to_json(orient = 'records')


@app.get("/categories")
async def all_categories():
    conexao = sql.connect('globalstore_normalizada.db')
    categories = pd.read_sql(
        con=conexao, 
        sql='select * from category'
    )
    return categories.to_json(orient = 'records')

@app.get("/products/category/{category_name}")
async def category_products(category_name: str):
    conexao = sql.connect('globalstore_normalizada.db')
    cat_products = pd.read_sql(
        con=conexao, 
        sql="select sub.subcategory, pro.product_id, pro.product_name "   
            "from products pro " + \
            "join subcategory sub on pro.sub_sequence = sub.sub_sequence " 
+
            "join category cat on sub.cat_sequence = cat.cat_sequence " +
            "where cat.category = '" + category_name + "'"
    )
    return cat_products.to_json(orient = 'records')


@app.get("/products/categorydf/{category_name}")
async def category_products(category_name: str):
    conexao = sql.connect('globalstore_normalizada.db')
    products = pd.read_sql(con=conexao, 
sql="select * from products")
    subcategory = pd.read_sql(con=conexao, 
sql="select * from subcategory")
    category = pd.read_sql(con=conexao, 
sql="select * from category")
    products = products.merge(subcategory, on='sub_sequence', how='left')
    products = products.merge(category, on='cat_sequence', how='left')
    return products[
products['category'] == category_name
].to_json(orient = 'records')


@app.get('/orderslist')
async def orders_list(year: int, orderpriority: str):
    conexao = sql.connect('globalstore_normalizada.db')
    orders = pd.read_sql(con=conexao, sql="select * from orders")
    orders['order_date'] = orders['order_date'].astype('datetime64[ns]')
    return orders[ 
        (orders['order_date'].dt.year == year) & 
        (orders['order_priority'] == orderpriority)
    ].to_json(orient = 'records')

@app.get('/orderslistwf')
async def orders_list_wf():
    conexao = sql.connect('globalstore_normalizada.db')
    orders = pd.read_sql(con=conexao, sql="select * from orders")
    orders['order_date'] = orders['order_date'].astype('datetime64[ns]')
    return orders.to_json(orient = 'records')

@app.get('/regions')
async def regions_list():
    conexao = sql.connect('globalstore_normalizada.db')
    regions = pd.read_sql(con=conexao, sql="select * from region")
    return regions.to_json(orient = 'records')

# >> Exercícios

# Criar rota para retornar os clientes dos mercados LATAM, US e Canada;
@app.get('/customersmarket')
async def customers_market():
    conexao = sql.connect('globalstore_normalizada.db')
    customers = pd.read_sql(con=conexao, sql="select distinct c.customer_name \
                                                from orders o \
                                               inner join customer c on c.cst_sequence = o.cst_sequence \
                                               inner join city ci on ci.cty_sequence = c.cty_sequence \
                                               inner join region r on r.reg_sequence = ci.reg_sequence \
                                               where r.market in ('LATAM', 'US', 'Canada')")

    return customers.to_json(orient = 'records')


# Criar rota para retornar lista de pedidos e itens do mercado EU;
@app.get('/ordersitems')
async def orders_items():
    conexao = sql.connect('globalstore_normalizada.db')
    ordersitems = pd.read_sql(con=conexao, sql="select o.ord_sequence, \
                                                     o.order_date, \
                                                     i.items_sequence,   \
                                                     p.product_name,   \
                                                     i.quantity,   \
                                                     i.sales,  \
                                                     i.profit   \
                                                from orders o \
                                               inner join items i on i.ord_sequence = o.ord_sequence     \
                                               inner join products p on p.pro_sequence = i.pro_sequence \
                                               inner join customer c on c.cst_sequence = o.cst_sequence \
                                               inner join city ci on ci.cty_sequence = c.cty_sequence \
                                               inner join region r on r.reg_sequence = ci.reg_sequence \
                                               where r.market in ('EU')")

    return ordersitems.to_json(orient = 'records')


# Criar rota para retornar os pedidos do anos 2013 e 2014 da categoria Furniture no mercado EMEA;
@app.get('/ordersitemsyear')
async def orders_items_year():
    conexao = sql.connect('globalstore_normalizada.db')
    ordersitemsyear = pd.read_sql(con=conexao, sql="select o.ord_sequence, \
                                                     o.order_date, \
                                                     STRFTIME('%d/%m/%Y, %H:%M', o.order_date) date_formatted, \
                                                     i.items_sequence,   \
                                                     p.product_name,   \
                                                     i.quantity,   \
                                                     i.sales,  \
                                                     i.profit   \
                                                from orders o \
                                               inner join items i on i.ord_sequence = o.ord_sequence     \
                                               inner join products p on p.pro_sequence = i.pro_sequence \
                                               inner join subcategory s on s.sub_sequence = p.sub_sequence \
                                               inner join category ct on ct.cat_sequence = s.cat_sequence \
                                               inner join customer c on c.cst_sequence = o.cst_sequence \
                                               inner join city ci on ci.cty_sequence = c.cty_sequence \
                                               inner join region r on r.reg_sequence = ci.reg_sequence \
                                               where r.market = 'EMEA' \
                                                 and o.order_date between '01/01/2013 00:00:01' and '31/12/2014 23:59:59' ")

    return ordersitemsyear.to_json(orient = 'records')