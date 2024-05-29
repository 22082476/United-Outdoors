import pandas as pd
from handle import setup_cursor, get_data, insert_data
from dotenv import load_dotenv
load_dotenv('.env')
import os

def customers():
    # cursor = setup_cursor(os.getenv("datawarehouse"))
    aw = customers_adventureworks()
    nw = customers_northwind()
    aenc = customers_aenc()

    merge1 =  pd.merge(aw, nw, how='outer')
    customers = pd.merge(merge1, aenc, how='outer')

    # insert_data(cursor, "customer_temp", ["customer_id"], customers) #for testing writing to db
    # Return the table and the primary key(s)
    return customers

def customers_adventureworks():
    cursor = setup_cursor(os.getenv("adventureworks"))

    sales_header = get_data(cursor, "Sales.SalesOrderHeader").loc[:, ['CustomerID', 'BillToAddressID']]
    address = get_data(cursor, 'Person.Address')
    state_province = get_data(cursor, 'Person.StateProvince')
    country_region = get_data(cursor, 'Person.CountryRegion')
    
    customer = get_data(cursor, "Sales.Customer").loc[:, ['CustomerID', 'PersonID', 'StoreID', 'TerritoryID']]
    sales_territory = get_data(cursor, "Sales.SalesTerritory").loc[:, ['TerritoryID', 'Name', 'Group', 'CountryRegionCode']].rename(columns={'Name':'customer_territory_name','Group':'customer_group'})
    store = get_data(cursor, "Sales.Store").loc[:, ['BusinessEntityID', 'Name']].rename(columns={'Name':'customer_company_name'})
    person = get_data(cursor, 'Person.Person').loc[:, ['BusinessEntityID', 'PersonType', 'NameStyle', 'Title', 'FirstName', 'MiddleName', 'LastName']]

    sales_header.drop_duplicates(inplace=True)
    sales_header.drop_duplicates(subset=['CustomerID'], keep='last', inplace=True)

    merge = pd.merge(sales_header, address, left_on='BillToAddressID', right_on='AddressID', how='left')
    merge2 = pd.merge(merge, state_province, left_on='StateProvinceID', right_on='StateProvinceID', how='left')
    merge3 = pd.merge(merge2, country_region, left_on='CountryRegionCode', right_on='CountryRegionCode', how='left')
    customerid_address = merge3.loc[:, ['CustomerID', 'AddressLine1', 'City', 'PostalCode', 'Name_x', 'Name_y']].rename(columns={'AddressLine1':'customer_address', 'City':'customer_city', 'PostalCode':'customer_zip_code', 'Name_x':'customer_region', 'Name_y':'customer_country'})

    sales_territory_full = pd.merge(sales_territory, country_region, left_on='CountryRegionCode', right_on='CountryRegionCode', how='left').rename(columns={'Name':'customer_country'})
    merge = pd.merge(customer, sales_territory_full, left_on='TerritoryID', right_on='TerritoryID', how='left').drop(columns=['TerritoryID', 'CountryRegionCode'])
    merge2 = pd.merge(merge, store, left_on='StoreID', right_on='BusinessEntityID', how='left').drop(columns=['StoreID', 'BusinessEntityID', 'ModifiedDate'])
    person['customer_full_name'] = person[['FirstName', 'MiddleName', 'LastName']].apply(lambda x: ' '.join(x.dropna()), axis=1)
    person = person.loc[:, ['BusinessEntityID', 'PersonType', 'NameStyle', 'Title', 'customer_full_name']].rename(columns={'PersonType':'customer_person_type', 'NameStyle':'customer_name_style', 'Title': 'customer_title'})
    merge3 = pd.merge(merge2, person, left_on='PersonID', right_on='BusinessEntityID', how='left').drop(columns=['BusinessEntityID', 'PersonID'])
    
    customers = pd.merge(merge3, customerid_address, left_on='CustomerID', right_on='CustomerID', how='left')
    customers = customers.drop(columns=['customer_country_y'])
    customers = customers.rename(columns={'CustomerID':'customer_id', 'customer_country_x': 'customer_country'})
    
    customers['customer_id'] = 'AW_' + customers['customer_id'].astype(str)
    return customers

def customers_northwind():
    cursor = setup_cursor(os.getenv('northwind'))

    customer = get_data(cursor, "Customers").rename(columns={'CompanyName': 'customer_company_name', 'ContactName':'customer_full_name','ContactTitle':'customer_title','Address':'customer_address','City':'customer_city','Region':'customer_region','PostalCode':'customer_zip_code','Country':'customer_country','CustomerID':'customer_id'})
    customers = customer.drop(columns=['Phone','Fax'])
    
    customers['customer_id'] = 'NW_' + customers['customer_id'].astype(str)
    return customers

def customers_aenc():
    cursor = setup_cursor(os.getenv('aenc'))
    
    customer = get_data(cursor, "Customer").rename(columns={'id':'customer_id','address':'customer_address','city':'customer_city','state':'state_id','zip':'customer_zip_code','company_name':'customer_company_name'})
    state = get_data(cursor, "State").rename(columns={'region':'customer_region', 'country':'customer_country'})

    customer['customer_full_name'] = customer[['fname',  'lname']].apply(lambda x: ' '.join(x.dropna()), axis=1)
    customer = customer.drop(columns=['fname', 'lname', 'phone'])

    merge = pd.merge(customer, state, left_on='state_id', right_on='state_id', how='left')
    customers = merge.drop(columns=['state_name','state_capital','state_id'])

    customers['customer_id'] = 'AC_' + customers['customer_id'].astype(str)
    return customers