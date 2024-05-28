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

    aenc_sales["paymethod"] = None

    aenc_sales["region_country"] = None
    aenc_sales["region_state"] = None

    aenc_sales["company_name"] = None

    aenc_sales["unit_price"] = aenc_sales["product_list_price"].astype(float)
    aenc_sales["revenue"] = aenc_sales["unit_price"].astype(float) * aenc_sales["quantity"].astype(int)
    aenc_sales["freight"] = None  
    aenc_sales["sub_total"] = aenc_sales["revenue"]
    aenc_sales["total_due"] = aenc_sales["revenue"]

    aenc_sales['order_date'] = pd.to_datetime(aenc_sales['order_date'])
    aenc_sales['due_date'] = None
    aenc_sales['currency_rate_date'] = None
    aenc_sales['ship_date'] = pd.to_datetime(aenc_sales['ship_date'])
    
    for date_col in ['order_date', 'ship_date']:
        aenc_sales[f'{date_col}_year'] = aenc_sales[date_col].dt.year
        aenc_sales[f'{date_col}_quarter'] = aenc_sales[date_col].dt.quarter
        aenc_sales[f'{date_col}_month'] = aenc_sales[date_col].dt.month
        aenc_sales[f'{date_col}_day'] = aenc_sales[date_col].dt.day
        aenc_sales[f'{date_col}_hour'] = aenc_sales[date_col].dt.hour
        aenc_sales[f'{date_col}_minute'] = aenc_sales[date_col].dt.minute

    aenc_sales['currency_rate_date_year'] = aenc_sales['currency_rate_date'].fillna(0).astype(int)
    aenc_sales['currency_rate_date_quarter'] = aenc_sales['currency_rate_date'].fillna(0).astype(int)
    aenc_sales['currency_rate_date_month'] = aenc_sales['currency_rate_date'].fillna(0).astype(int)
    aenc_sales['currency_rate_date_day'] = aenc_sales['currency_rate_date'].fillna(0).astype(int)
    aenc_sales['currency_rate_date_hour'] = aenc_sales['currency_rate_date'].fillna(0).astype(int)
    aenc_sales['currency_rate_date_minute'] = aenc_sales['currency_rate_date'].fillna(0).astype(int)

    aenc_sales['due_date_year'] = aenc_sales['due_date'].fillna(0).astype(int)
    aenc_sales['due_date_quarter'] = aenc_sales['due_date'].fillna(0).astype(int)
    aenc_sales['due_date_month'] = aenc_sales['due_date'].fillna(0).astype(int)
    aenc_sales['due_date_day'] = aenc_sales['due_date'].fillna(0).astype(int)
    aenc_sales['due_date_hour'] = aenc_sales['due_date'].fillna(0).astype(int)
    aenc_sales['due_date_minute'] = aenc_sales['due_date'].fillna(0).astype(int)

    data = []
    employees = employee()
    for x in employees.adventure:
        convert = x.__dict__
        data.append(convert)
    
    df_employees = pd.DataFrame(data)
    products_ac = products()
    aenc_sales = pd.merge(aenc_sales, products_ac, on='product_id')
    aenc_sales = pd.merge(aenc_sales, customers(), on="customer_id", how='inner')
    aenc_sales = pd.merge(aenc_sales, df_employees, left_on="sales_rep", right_on='employee_id', how='left')
    aenc_sales.rename(columns={"sales_rep":"employee_id"}, inplace=True)

    export_cursor = setup_cursor(os.getenv("datawarehouse"))
    insert_data(export_cursor, "sales_order", ["id", "line_id"], aenc_sales.head(4))

    return aenc_sales
    
def aenc1 ():
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
    
        
    aenc_sales = set_employee(aenc_sales, "employee", employee().get_employee("AC_" + aenc_sales["sales_rep"]))
    aenc_sales.drop("sales_rep", inplace=True)

    aenc_sales = set_date(aenc_sales, "order_date", date(aenc_sales["order_date"]))
    aenc_sales = set_date(aenc_sales, "ship_date", date(aenc_sales["ship_date"]))
    aenc_sales.drop(["order_date", "ship_date"], inplace=True)
    aenc_sales = set_date(aenc_sales, "due_date", DateTable(None, None, None, None, None, None, None))
    aenc_sales["currency_rate_date"] = None

    aenc_sales = set_address(aenc_sales, "bill", AddressTable(None, None, None, None, None, None))
    aenc_sales = set_address(aenc_sales, "ship", AddressTable(None, None, None, None, None, None))

    aenc_sales = set_shipmethod(aenc_sales, ShipMethod(None, None, None, None))


    aenc_sales = set_currency(aenc_sales, "from", SalesCurrency(None, None))
    aenc_sales = set_currency(aenc_sales, "to", SalesCurrency(None, None))



    print(aenc_sales["unit_price"])

    insert_data(setup_cursor(os.getenv("datawarehouse")), "sales_order", ["id", "line_id"], aenc_sales)

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
    sales_merge['revenue'] = sales_merge['total_due']
    sales_merge = pd.merge(sales_merge, currencies, left_on='CurrencyRateID', right_on='CurrencyRateID', how='left')
    
    sales_merge['OrderDate'] = pd.to_datetime(sales_merge['OrderDate'])
    sales_merge['DueDate'] = pd.to_datetime(sales_merge['DueDate'])
    sales_merge['ShipDate'] = pd.to_datetime(sales_merge['ShipDate'])
    sales_merge['CurrencyRateDate'] = pd.to_datetime(sales_merge['CurrencyRateDate'])
    sales_merge = sales_merge.rename(columns={'OrderDate': 'order_date', 'DueDate':'due_date','ShipDate':'ship_date', 'CurrencyRateDate':'currency_rate_date'})
    
    for date_col in ['order_date', 'due_date', 'ship_date', 'currency_rate_date']:
        sales_merge[f'{date_col}_year'] = sales_merge[date_col].dt.year
        sales_merge[f'{date_col}_quarter'] = sales_merge[date_col].dt.quarter
        sales_merge[f'{date_col}_month'] = sales_merge[date_col].dt.month
        sales_merge[f'{date_col}_day'] = sales_merge[date_col].dt.day
        sales_merge[f'{date_col}_hour'] = sales_merge[date_col].dt.hour
        sales_merge[f'{date_col}_minute'] = sales_merge[date_col].dt.minute

    sales_merge['currency_rate_date_year'] = sales_merge['currency_rate_date_year'].fillna(0).astype(int)
    sales_merge['currency_rate_date_quarter'] = sales_merge['currency_rate_date_quarter'].fillna(0).astype(int)
    sales_merge['currency_rate_date_month'] = sales_merge['currency_rate_date_month'].fillna(0).astype(int)
    sales_merge['currency_rate_date_day'] = sales_merge['currency_rate_date_day'].fillna(0).astype(int)
    sales_merge['currency_rate_date_hour'] = sales_merge['currency_rate_date_hour'].fillna(0).astype(int)
    sales_merge['currency_rate_date_minute'] = sales_merge['currency_rate_date_minute'].fillna(0).astype(int)

    customer_merge = pd.merge(sales_merge, customers(), left_on='CustomerID', right_on="customer_id", how='inner')
    customer_merge = customer_merge.drop(columns=['CustomerID'])
    
    territory_merge = pd.merge(customer_merge, sales_territory, left_on='TerritoryID', right_on='TerritoryID', how='left')
    territory_merge = territory_merge.rename(columns={'TerritoryID':'sales_territory_id'})
  
    merge = pd.merge(address, state_province, left_on='StateProvinceID', right_on='StateProvinceID', how='left')
    merge2 = pd.merge(merge, country_region, left_on='CountryRegionCode', right_on='CountryRegionCode', how='left')
    addresses = merge2.loc[:, ['AddressID', 'AddressLine1', 'City', 'PostalCode', 'Name_x', 'Name_y']].rename(columns={'AddressLine1':'address', 'City':'city', 'PostalCode':'postalcode', 'Name_x':'region', 'Name_y':'country'})
    
    address_types = ['ShipToAddressID', 'BillToAddressID']
    count = 0
    for x in address_types:
        ids = pd.DataFrame(territory_merge[x].drop_duplicates())
        aight = set_addresses(x, ids, count, addresses)
        territory_merge = pd.merge(territory_merge, aight, left_on=x, right_on=x)
        count += 1

    territory_merge = territory_merge.drop(columns=['BillToAddressID','ShipToAddressID'])

    shipmethod = shipmethod.loc[:,['ShipMethodID','shipmethod_name','shipmethod_ship_base','shipmethod_ship_rate']].rename(columns={'ShipMethodID':'shipmethod_id'})
    shipmethod_merge = pd.merge(territory_merge, shipmethod, left_on='ShipMethodID', right_on='shipmethod_id')
    shipmethod_merge = shipmethod_merge.drop(columns=['ShipMethodID'])
    shipmethod_merge['paymethod'] = np.where(shipmethod_merge['CreditCardID'].notna(), 'creditcard', 'else')
    shipmethod_merge = shipmethod_merge.drop(columns=['CreditCardID'])

    region_merge = shipmethod_merge
    region_merge['region_country'] = None
    region_merge['region_state'] = None
    region_merge['region'] = None
    region_merge['company_name'] = None

    data = []
    employees = employee()
    for x in employees.adventure:
        convert = x.__dict__
        data.append(convert)
    
    df_employees = pd.DataFrame(data)
    products_aw = products()
    products_merge = pd.merge(region_merge, products_aw, left_on='ProductID', right_on='product_id')
    products_merge['SalesPersonID'] = products_merge['SalesPersonID'].str.replace('.0', '', regex=False)

    employee_merge = pd.merge(products_merge, df_employees, left_on='SalesPersonID', right_on='employee_id', how='left')
    employee_merge = employee_merge.drop(columns=['SalesPersonID'])
    
    export_cursor = setup_cursor(os.getenv("datawarehouse"))
    insert_data(export_cursor, "sales_order", ["id", "line_id"], employee_merge)

def set_addresses(address, ids, count, addresses):
    text = 'ship_to_address_' if count < 1 else 'bill_to_address_'
    merge = pd.merge(ids, addresses, left_on=address, right_on='AddressID')
    merge = merge.rename(columns={'city':f"{text}city",'country':f"{text}country",'region':f"{text}region",'postalcode':f"{text}postalcode",'address':text[:-1]})
    merge = merge.drop(columns=['AddressID'])
    return merge
    

def set_currency (data, attribute, currency):
    data[attribute + "_currency_code"] = currency.currency_code
    data[attribute + "_currency_name"] = currency.currency_name

    return data

def set_date (data, attribute, date):
    data[attribute + "_year"] = date.year
    data[attribute + "_quarter"] = date.quarter
    data[attribute + "_month"] = date.month
    data[attribute + "_day"] = date.day
    data[attribute + "_hour"] = date.hour
    data[attribute + "_minute"] = date.minute
    data[attribute + "_date"] = date.date

    return data

def set_address (data, attribute, address):
    data[attribute + "_to_address_region"] = address.region
    data[attribute + "_to_address_city"] = address.city
    data[attribute + "_to_address_postalcode"] = address.postalcode
    data[attribute + "_to_address_street"] = address.street
    data[attribute + "_to_address"] = address.address

    return data

def set_shipmethod (data, shipmethod):
    data["shipmethod_id"] = shipmethod.shipmethod_id
    data["shipmethod_name"] = shipmethod.shipmethod_name
    data["shipmethod_base"] = shipmethod.shipmethod_ship_base
    data["shipmethod_rate"] = shipmethod.shipmethod_ship_rate


    return data

def set_employee(data, attribute, employee):
    data[attribute + "_id"] = employee.employee_id
    data[attribute + "_full_name"] = employee.employee_full_name
    data[attribute + "_extention"] = employee.employee_extention
    data[attribute + "_sales_YTD"] = employee.employee_sales_YTD
    data[attribute + "_sales_last_year"] = employee.employee_sales_last_year
    data[attribute + "_department_head"] = employee.employee_department_head
    data[attribute + "_department"] = employee.employee_department
    data[attribute + "_start_date"] = employee.employee_start_date
    data[attribute + "_birth_date"] = employee.employee_birth_date
    data[attribute + "_salary"] = employee.employee_salary
    data[attribute + "_country"] = employee.employee_country
    data[attribute + "_region"] = employee.employee_region
    data[attribute + "_city"] = employee.employee_city
    data[attribute + "_zip_code"] = employee.employee_zip_code
    data[attribute + "_street_name"] = employee.employee_street_name
    data[attribute + "_house_number"] = employee.employee_house_number
    data[attribute + "_manager"] = employee.employee_manager
    data[attribute + "_health_insurance"] = employee.employee_health_insurance
    data[attribute + "_life_insurance"] = employee.employee_life_insurance
    data[attribute + "_day_care"] = employee.employee_day_care
    data[attribute + "_sex"] = employee.employee_sex
    data[attribute + "_termination_date"] = employee.employee_termination_date
    data[attribute + "_title"] = employee.employee_title
    data[attribute + "_title_of_courtesy"] = employee.employee_title_of_courtesy
    data[attribute + "_group"] = employee.employee_group
    data[attribute + "_territory"] = employee.employee_territory
    data[attribute + "_country_region_code"] = employee.employee_country_region_code
    data[attribute + "_vactions_hours"] = employee.employee_vactions_hours
    data[attribute + "_sick_leave_hours"] = employee.employee_sick_leave_hours
    data[attribute + "_martial_status"] = employee.employee_martial_status
    data[attribute + "_orginanizion_level"] = employee.employee_orginanizion_level
    data[attribute + "_sales_quota"] = employee.employee_sales_quota
    data[attribute + "_bonus"] = employee.employee_bonus
    data[attribute + "_commission_pct"] = employee.employee_commission_pct

    return data


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

