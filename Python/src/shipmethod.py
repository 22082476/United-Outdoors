import pandas as pd
from handle import setup_cursor, get_data
from dotenv import load_dotenv
load_dotenv('.env')
import os


def shipmethod():
    cursor_aw = setup_cursor(os.getenv("adventureworks"))
    cursor_nw = setup_cursor(os.getenv('northwind'))

    nw_shipper = get_data(cursor_nw, "dbo.Shippers")
    aw_shipmethod = get_data(cursor_aw, "Purchasing.ShipMethod")

    nw_shipper = nw_shipper.loc[:, ['ShipperID', 'CompanyName']].rename(columns={'ShipperID':'shipper_id', 'CompanyName':'company_name'})
    aw_shipmethod = aw_shipmethod.loc[:, ['ShipMethodID', 'Name', 'ShipBase', 'ShipRate']].rename(columns={'ShipMethodID':'shipmethod_id', 'Name':'shipmethod_name', 'ShipBase':'shipmethod_ship_base', 'ShipRate':'shipmethod_ship_rate'})

    shipmethod_merge = pd.merge(nw_shipper, aw_shipmethod, left_on='shipper_id', right_on='shipmethod_id', how='inner').drop(columns='shipper_id')
    return shipmethod_merge

