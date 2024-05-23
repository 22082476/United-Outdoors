import pandas as pd
from handle import setup_cursor, get_data
from dotenv import load_dotenv
load_dotenv('.env')
import os


def aw_sales_currency():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    aw_salescurrency = get_data(cursor_aw, "sales.currency")
    aw_salescurrency = aw_salescurrency.rename(columns={'CurrencyCode': 'currency_code',
                                                        'Name': 'currency_name'})
    return aw_salescurrency

def aw_shipmethod():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    cursor_nw = setup_cursor(os.getenv('northwind'))

    nw_shipper = get_data(cursor_nw, "dbo.Shippers")
    aw_shipmethod = get_data(cursor_aw, "Purchasing.ShipMethod")

    nw_shipper = nw_shipper.loc[:, ['ShipperID', 'CompanyName']].rename(columns={'ShipperID':'shipper_id', 'CompanyName':'company_name'})
    aw_shipmethod = aw_shipmethod.loc[:, ['ShipMethodID', 'Name', 'ShipBase', 'ShipRate']].rename(columns={'ShipMethodID':'shipmethod_id', 'Name':'shipmethod_name', 'ShipBase':'shipmethod_ship_base', 'ShipRate':'shipmethod_ship_rate'})

    shipmethod_merge = pd.merge(nw_shipper, aw_shipmethod, left_on='shipper_id', right_on='shipmethod_id', how='inner').drop(columns='shipper_id')
    return shipmethod_merge

def aw_salesorderreason():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    aw_salesorderheaderreason = get_data(cursor_aw, "Sales.SalesOrderHeaderSalesReason")
    aw_salesorderreason = get_data(cursor_aw, "Sales.SalesReason")


    aw_salesorderheaderreason = aw_salesorderheaderreason.loc[:, ['SalesOrderID', 'SalesReasonID']].rename(columns={'SalesOrderID':'salesorder_id', 'SalesReasonID':'salesreason_id'})
    aw_salesorderreason = aw_salesorderreason.loc[:, ['SalesReasonID', 'Name', 'ReasonType']].rename(columns={'SalesReasonID':'salesreason_id', 'Name':'salesreason_name', 'ReasonType':'salesreason_type'})
    aw_salesorderheaderreasonmerge = pd.merge(aw_salesorderreason, aw_salesorderheaderreason, on='salesreason_id', how='inner')
    return aw_salesorderheaderreasonmerge

def aw_sales_territory():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    aw_sales_territory = get_data(cursor_aw, "Sales.SalesTerritory")
    aw_sales_territory = aw_sales_territory.loc[:, ['TerritoryID', 'Name', 'SalesYTD', 'SalesLastYear', 'CostYTD', 'CostLastYear']].rename(columns={'TerritoryID':'sales_territory_id', 'Name':'sales_territory_name', 'SalesYTD':'sales_territory_YTD', 'SalesLastYear':'sales_territory_sales_last_year', 'CostYTD':'sales_territory_cost_YTD', 'CostLastYear':'sales_territory_cost_last_year'})
    return aw_sales_territory

def aw_paymethod_table():
    aw_paymethod_columns = ['paymethod_id', 'creditcard'].rename(columns={'PayMethodID':'paymethod', 'CreditCard':'creditcard'})
    aw_paymethod = pd.DataFrame(columns=aw_paymethod_columns)
    return aw_paymethod