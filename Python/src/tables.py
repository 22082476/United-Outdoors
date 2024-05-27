import pandas as pd
from handle import setup_cursor, get_data
from dotenv import load_dotenv
load_dotenv('.env')
import os



class SalesCurrency:
    def __init__(self, currency_code, currency_name):
        self.currency_code = currency_code
        self.currency_name = currency_name

def aw_sales_currency():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    aw_salescurrency = get_data(cursor_aw, "sales.currency")
    aw_salescurrency = aw_salescurrency.rename(columns={'CurrencyCode': 'currency_code','Name': 'currency_name'})
    sales_currencies = []
    for index, row in aw_salescurrency.iterrows():
        sales_currencies.append(SalesCurrency(row['currency_code'], row['currency_name']))
    
    print(sales_currencies)
    return sales_currencies





class ShipMethod:
    def __init__(self, shipmethod_id, shipmethod_name, shipmethod_ship_base, shipmethod_ship_rate, company_name):
        self.shipmethod_id = shipmethod_id
        self.shipmethod_name = shipmethod_name
        self.shipmethod_ship_base = shipmethod_ship_base
        self.shipmethod_ship_rate = shipmethod_ship_rate
        self.company_name = company_name

def aw_shipmethod():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    cursor_nw = setup_cursor(os.getenv('northwind'))

    nw_shipper = get_data(cursor_nw, "dbo.Shippers")
    aw_shipmethod = get_data(cursor_aw, "Purchasing.ShipMethod")

    nw_shipper = nw_shipper.loc[:, ['ShipperID', 'CompanyName']].rename(columns={'ShipperID':'shipper_id', 'CompanyName':'company_name'})
    aw_shipmethod = aw_shipmethod.loc[:, ['ShipMethodID', 'Name', 'ShipBase', 'ShipRate']].rename(columns={'ShipMethodID':'shipmethod_id', 'Name':'shipmethod_name', 'ShipBase':'shipmethod_ship_base', 'ShipRate':'shipmethod_ship_rate'})

    shipmethod_merge = pd.merge(nw_shipper, aw_shipmethod, left_on='shipper_id', right_on='shipmethod_id', how='inner').drop(columns='shipper_id')

    ship_methods = []
    for index, row in shipmethod_merge.iterrows():
        ship_methods.append(ShipMethod(row['shipmethod_id'], row['shipmethod_name'], row['shipmethod_ship_base'], row['shipmethod_ship_rate'], row['company_name']))
    print(len(ship_methods))
    return ship_methods




class SalesOrderReason:
    def __init__(self, salesorder_id, salesreason_id, salesreason_name, salesreason_type):
        self.salesorder_id = salesorder_id
        self.salesreason_id = salesreason_id
        self.salesreason_name = salesreason_name
        self.salesreason_type = salesreason_type

def aw_salesorderreason():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    aw_salesorderheaderreason = get_data(cursor_aw, "Sales.SalesOrderHeaderSalesReason")
    aw_salesorderreason = get_data(cursor_aw, "Sales.SalesReason")

    aw_salesorderheaderreason = aw_salesorderheaderreason.loc[:, ['SalesOrderID', 'SalesReasonID']].rename(columns={'SalesOrderID':'salesorder_id', 'SalesReasonID':'salesreason_id'})
    aw_salesorderreason = aw_salesorderreason.loc[:, ['SalesReasonID', 'Name', 'ReasonType']].rename(columns={'SalesReasonID':'salesreason_id', 'Name':'salesreason_name', 'ReasonType':'salesreason_type'})
    aw_salesorderheaderreasonmerge = pd.merge(aw_salesorderreason, aw_salesorderheaderreason, on='salesreason_id', how='inner')

    sales_order_reasons = []
    for index, row in aw_salesorderheaderreasonmerge.iterrows():
        sales_order_reasons.append(SalesOrderReason(row["salesorder_id"], row["salesreason_id"], row["salesreason_name"], row["salesreason_type"]))
    print(len(sales_order_reasons))
    return sales_order_reasons




class SalesTerritory:
    def __init__(self, sales_territory_id, sales_territory_name, sales_territory_YTD, sales_territory_sales_last_year, sales_territory_cost_YTD, sales_territory_cost_last_year):
        self.sales_territory_id = sales_territory_id
        self.sales_territory_name = sales_territory_name
        self.sales_territory_YTD = sales_territory_YTD
        self.sales_territory_sales_last_year = sales_territory_sales_last_year
        self.sales_territory_cost_YTD = sales_territory_cost_YTD
        self.sales_territory_cost_last_year = sales_territory_cost_last_year

def aw_sales_territory():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    aw_sales_territory = get_data(cursor_aw, "Sales.SalesTerritory")
    aw_sales_territory = aw_sales_territory.loc[:, ['TerritoryID', 'Name', 'SalesYTD', 'SalesLastYear', 'CostYTD', 'CostLastYear']].rename(columns={'TerritoryID':'sales_territory_id', 'Name':'sales_territory_name', 'SalesYTD':'sales_territory_YTD', 'SalesLastYear':'sales_territory_sales_last_year', 'CostYTD':'sales_territory_cost_YTD', 'CostLastYear':'sales_territory_cost_last_year'})

    sales_territories = []
    for index, row in aw_sales_territory.iterrows():
        sales_territories.append(SalesTerritory(row["sales_territory_id"], row["sales_territory_name"], row["sales_territory_YTD"], row["sales_territory_sales_last_year"], row["sales_territory_cost_YTD"], row["sales_territory_cost_last_year"]))
    print(len(sales_territories))
    return sales_territories





class PayMethod:
    def __init__(self, paymethod_id, creditcard):
        self.paymethod_id = paymethod_id
        self.creditcard = creditcard

def aw_paymethod_table():
    aw_paymethod_columns = ['paymethod_id', 'creditcard']
    aw_paymethod = pd.DataFrame(columns=aw_paymethod_columns)
    aw_paymethod = aw_paymethod.rename(columns={'paymethod_id':'paymethod', 'creditcard':'CreditCard'})

    pay_methods = []
    for index, row in aw_paymethod.iterrows():
        pay_methods.append(PayMethod(row["paymethod"], row["CreditCard"]))
    print(len(pay_methods))
    return pay_methods
