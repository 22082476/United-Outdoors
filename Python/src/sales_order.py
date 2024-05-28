from employee import employee
from customer import customers
from product import products
from handle import setup_cursor, get_data, insert_data, date
from classes import DateTable, AddressTable, ShipMethod, SalesCurrency
from dotenv import load_dotenv
load_dotenv('.env')
import os
import pandas as pd
import numpy as np


def sales_order ():
    aenc()

    #aw_sales_order_header = get_data(setup_cursor(os.getenv("adventureworks")), "Sales.SalesOrderHeader")
    #aw_sales_order_detail = get_data(setup_cursor(os.getenv("adventureworks")), "Sales.SalesOrderDetail")

    #nw_orders = get_data(setup_cursor(os.getenv("northwind")), "Orders")
    #nw_order_details = get_data(setup_cursor(os.getenv("northwind")), "OrderDetails")
      

def aenc ():
    aenc_sales_order = get_data(setup_cursor(os.getenv("aenc")), "Sales_order")
    aenc_sales_order_item = get_data(setup_cursor(os.getenv("aenc")), "Sales_order_item")
    aenc_region = get_data(setup_cursor(os.getenv("aenc")), "Region")

    aenc_sales = pd.merge(aenc_sales_order, aenc_sales_order_item, on='id', how='inner')
    aenc_sales = pd.merge(aenc_sales, aenc_region, on='region', how='inner')
    aenc_sales["customer_id"] = "AC_" + aenc_sales["cust_id"].astype(str)
    aenc_sales = pd.merge(aenc_sales, customers(), left_on='customer_id', right_on="customer_id", how='inner')
    aenc_sales["product_id"] = "AC_" + aenc_sales["prod_id"].astype(str)
    aenc_sales = pd.merge(aenc_sales, products(), left_on='product_id', right_on="product_id", how='inner') 
    aenc_sales.drop(["cust_id", "prod_id"], axis=1, inplace=True)
    print(aenc_sales.columns)
    print(len(aenc_sales))
    aenc_sales = aenc_sales.head(4)

    # Iets anders op bedenken
    for index, row in aenc_sales.iterrows():
        
        row = set_employee(row, "employee", employee().get_employee("AC_" + row["sales_rep"]))
        row.drop("sales_rep", inplace=True)
    
        row = set_date(row, "order_date", date(row["order_date"]))
        row = set_date(row, "ship_date", date(row["ship_date"]))
        row.drop(["order_date", "ship_date"], inplace=True)
        row = set_date(row, "due_date", DateTable(None, None, None, None, None, None, None))
        row["currency_rate_date"] = None

        row = set_address(row, "bill", AddressTable(None, None, None, None, None, None))
        row = set_address(row, "ship", AddressTable(None, None, None, None, None, None))

        row = set_shipmethod(row, ShipMethod(None, None, None, None))

        row["paymethod"] = None

        row = set_currency(row, "from", SalesCurrency(None, None))
        row = set_currency(row, "to", SalesCurrency(None, None))

        row["region_country"] = None
        row["region_state"] = None

        row["company_name"] = None

        row["unit_price"] = float(row["product_list_price"])
        row["revenue"] = float(row["unit_price"]) * int(row["quantity"])
        row["tax_amt"] = None
        row["freight"] = None  
        row["sub_total"] = row["revenue"]
        row["total_due"] = row["revenue"]

        print(row["unit_price"])

    insert_data(setup_cursor(os.getenv("datawharehouse")), "sales_order", ["id", "line_id"], aenc_sales)

def adventure_works():
    cursor = setup_cursor(os.getenv("adventureworks"))
    sales_header = get_data(cursor, "Sales.SalesOrderHeader").loc[:,['SalesOrderID', 'OrderDate','DueDate','ShipDate','SubTotal','TaxAmt','Freight','TotalDue','SalesOrderNumber','CustomerID','SalesPersonID','TerritoryID','BillToAddressID','ShipToAddressID','ShipMethodID','CreditCardID','CurrencyRateID']]
    sales_order = get_data(cursor, "Sales.SalesOrderDetail").loc[:,['SalesOrderID','SalesOrderDetailID','OrderQty','ProductID','UnitPrice','LineTotal']]
    sales_territory = get_data(cursor, "Sales.SalesTerritory").loc[:, ['TerritoryID','Name','CountryRegionCode','SalesYTD','SalesLastYear','CostYTD','CostLastYear']].rename(columns={'Name':'sales_territory_name','CountryRegionCode':'sales_territory_crc','SalesYTD':'sales_territory_YTD','SalesLastYear':'sales_territory_sales_last_year','CostYTD':'sales_territory_cost_YTD','CostLastYear':'sales_territory_cost_last_year'})
    currency = get_data(cursor, "Sales.Currency")
    curreny_rates = get_data(cursor, "Sales.CurrencyRate")
    address = get_data(cursor, 'Person.Address')
    state_province = get_data(cursor, 'Person.StateProvince')
    country_region = get_data(cursor, 'Person.CountryRegion')
    shipmethod = get_data(cursor, "Purchasing.ShipMethod").rename(columns={'Name':'shipmethod_name','ShipBase':'shipmethod_ship_base','ShipRate':'shipmethod_ship_rate'})

    merge1 = pd.merge(curreny_rates, currency, left_on='FromCurrencyCode', right_on='CurrencyCode', how='left').rename(columns={'Name':'from_currency_name'})
    merge2 = pd.merge(merge1, currency, left_on='ToCurrencyCode', right_on='CurrencyCode', how='left').rename(columns={'Name':'to_currency_name'})
    currencies = merge2.loc[:, ['CurrencyRateID', 'CurrencyRateDate','FromCurrencyCode','ToCurrencyCode','from_currency_name','to_currency_name']].rename(columns={'FromCurrencyCode':'from_currency_code','ToCurrencyCode':'to_currency_code'})

    sales_merge = pd.merge(sales_order, sales_header, left_on='SalesOrderID', right_on='SalesOrderID', how='inner')
    sales_merge["ProductID"] = "AW_" + sales_merge["ProductID"].astype(str)
    sales_merge["SalesPersonID"] = "AW_" + sales_merge["SalesPersonID"].astype(str)
    sales_merge["CustomerID"] = "AW_" + sales_merge["CustomerID"].astype(str)

    sales_merge = sales_merge.rename(columns={'SalesOrderID':'id','SalesOrderDetailID':'line_id','UnitPrice':'unit_price','OrderQty':'quantity','TotalDue':'total_due','Freight':'freight','TaxAmt':'tax_amt','SubTotal':'sub_total'})

def set_addresses(address, ids, count, addresses):
    text = 'ship_to_address_' if count < 1 else 'bill_to_address_'
    merge = pd.merge(ids, addresses, left_on=address, right_on='AddressID')
    merge = merge.rename(columns={'city':f"{text}city",'country':f"{text}country",'region':f"{text}region",'postalcode':f"{text}postalcode",'address':text[:-1]})
    merge = merge.drop(columns=['AddressID'])
    return merge
    

def set_currency (row, attribute, currency):
    row[attribute + "_currency_code"] = currency.currency_code
    row[attribute + "_currency_name"] = currency.currency_name

    return row

def set_date (row, attribute, date):
    row[attribute + "_year"] = date.year
    row[attribute + "_quarter"] = date.quarter
    row[attribute + "_month"] = date.month
    row[attribute + "_day"] = date.day
    row[attribute + "_hour"] = date.hour
    row[attribute + "_minute"] = date.minute
    row[attribute + "_date"] = date.date

    return row

def set_address (row, attribute, address):
    row[attribute + "_to_address_country"] = address.country
    row[attribute + "_to_address_region"] = address.region
    row[attribute + "_to_address_city"] = address.city
    row[attribute + "_to_address_postalcode"] = address.postalcode
    row[attribute + "_to_address_street"] = address.street
    row[attribute + "_to_address"] = address.address

    return row

def set_shipmethod (row, shipmethod):
    row["shipmethod_id"] = shipmethod.shipmethod_id
    row["shipmethod_name"] = shipmethod.shipmethod_name
    row["shipmethod_base"] = shipmethod.shipmethod_ship_base
    row["shipmethod_rate"] = shipmethod.shipmethod_ship_rate


    return row

def set_employee(row, attribute, employee):
    row[attribute + "_id"] = employee.employee_id
    row[attribute + "_full_name"] = employee.employee_full_name
    row[attribute + "_extention"] = employee.employee_extention
    row[attribute + "_sales_YTD"] = employee.employee_sales_YTD
    row[attribute + "_sales_last_year"] = employee.employee_sales_last_year
    row[attribute + "_department_head"] = employee.employee_department_head
    row[attribute + "_department"] = employee.employee_department
    row[attribute + "_start_date"] = employee.employee_start_date
    row[attribute + "_birth_date"] = employee.employee_birth_date
    row[attribute + "_salary"] = employee.employee_salary
    row[attribute + "_country"] = employee.employee_country
    row[attribute + "_region"] = employee.employee_region
    row[attribute + "_city"] = employee.employee_city
    row[attribute + "_zip_code"] = employee.employee_zip_code
    row[attribute + "_street_name"] = employee.employee_street_name
    row[attribute + "_house_number"] = employee.employee_house_number
    row[attribute + "_manager"] = employee.employee_manager
    row[attribute + "_health_insurance"] = employee.employee_health_insurance
    row[attribute + "_life_insurance"] = employee.employee_life_insurance
    row[attribute + "_day_care"] = employee.employee_day_care
    row[attribute + "_sex"] = employee.employee_sex
    row[attribute + "_termination_date"] = employee.employee_termination_date
    row[attribute + "_title"] = employee.employee_title
    row[attribute + "_title_of_courtesy"] = employee.employee_title_of_courtesy
    row[attribute + "_group"] = employee.employee_group
    row[attribute + "_territory"] = employee.employee_territory
    row[attribute + "_country_region_code"] = employee.employee_country_region_code
    row[attribute + "_vactions_hours"] = employee.employee_vactions_hours
    row[attribute + "_sick_leave_hours"] = employee.employee_sick_leave_hours
    row[attribute + "_martial_status"] = employee.employee_martial_status
    row[attribute + "_orginanizion_level"] = employee.employee_orginanizion_level
    row[attribute + "_sales_quota"] = employee.employee_sales_quota
    row[attribute + "_bonus"] = employee.employee_bonus
    row[attribute + "_commission_pct"] = employee.employee_commission_pct

    return row


employee_columns = [
    'employee_id',
    'employee_full_name',
    'employee_extention',
    'employee_sales_YTD',
    'employee_sales_last_year',
    'employee_department_head',
    'employee_department',
    'employee_start_date',
    'employee_birth_date',
    'employee_salary',
    'employee_country',
    'employee_region',
    'employee_city',
    'employee_zip_code',
    'employee_street_name',
    'employee_house_number',
    'employee_manager',
    'employee_health_insurance',
    'employee_life_insurance',
    'employee_day_care',
    'employee_sex',
    'employee_termination_date',
    'employee_title',
    'employee_title_of_courtesy',
    'employee_group',
    'employee_territory',
    'employee_country_region_code',
    'employee_vactions_hours',
    'employee_sick_leave_hours',
    'employee_martial_status',
    'employee_orginanizion_level',
    'employee_sales_quota',
    'employee_bonus',
    'employee_commission_pct'
]

