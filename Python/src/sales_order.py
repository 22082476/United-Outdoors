from employee import employee
from customer import customer
from product import product
from tables import aw_paymethod_table, aw_date_table, aw_adress_table, aw_sales_currency, nw_shippers, aw_salesorderreason, aw_sales_territory
from handle import setup_cursor, get_data, insert_data
import os
import pandas as pd


def sales_order ():
    aenc_sales_order = get_data(setup_cursor(os.getenv("aenc")), "Sales_order")
    aenc_sales_order_item = get_data(setup_cursor(os.getenv("aenc")), "Sales_order_item")

    aw_sales_order_header = get_data(setup_cursor(os.getenv("adventureworks")), "Sales.SalesOrderHeader")
    aw_sales_order_detail = get_data(setup_cursor(os.getenv("adventureworks")), "Sales.SalesOrderDetail")

    nw_orders = get_data(setup_cursor(os.getenv("northwind")), "Orders")
    nw_order_details = get_data(setup_cursor(os.getenv("northwind")), "OrderDetails")

    aenc_sales = pd.merge(aenc_sales_order, aenc_sales_order_item, on='id', how='inner')

    for index, row in aenc_sales.iterrows():
        customer = None
        for i, r in customer().iterows():
            if r["customer_id"] == row["cust_id"]:
                customer = r
                break

        employee = employee().get_employee("AC_" + row["sales_rep"])

        product = None
        for i, r in product().iterrows():
            if r["product_id"] == row["product_id"]:
                product = r
                break

        order_date = None
        for i, r in ac_date_table().iterrows():
            if r["order_date"] == row["order_date"]:
                order_date = r
                break

        
        insert_data(setup_cursor(os.getenv("datawharehouse")), "sales_order", [row["id"], row["line_id"]], )




    
