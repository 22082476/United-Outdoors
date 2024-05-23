import pandas as pd
from connections import setup_cursors, get_data

def main():
    #Merge the fuckers
    aw = products_adventureworks()
    nw = products_northwind()
    aenc = products_aenc()
    
    merge_1 = pd.merge(aw, nw, how='outer')
    merge_1['Quantity'] = merge_1['Quantity'].fillna(0)
    merge_1['Quantity'] = merge_1['Quantity'].astype(int)
    aenc['Quantity'] = aenc['Quantity'].astype(int)

    products = pd.merge(merge_1, aenc, how='outer')

def products_adventureworks():
    cursor_aw, cursor_nw, cursor_aenc, export_cursor = setup_cursors()

    product = get_data(cursor_aw, "Production.Product")
    sub_category = get_data(cursor_aw, "Production.ProductSubcategory")
    category = get_data(cursor_aw, "Production.ProductCategory")
    
    inventory = get_data(cursor_aw, "Production.ProductInventory")
    
    model = get_data(cursor_aw, "Production.ProductModel")
    description_culture = get_data(cursor_aw, "Production.ProductModelProductDescriptionCulture")
    description = get_data(cursor_aw, "Production.ProductDescription")
    culture = get_data(cursor_aw, "Production.Culture")

    purchase_vendor = get_data(cursor_aw, 'Purchasing.ProductVendor')
    vendor = get_data(cursor_aw, 'Purchasing.Vendor')
    business_entity_address = get_data(cursor_aw, 'Person.BusinessEntityAddress')
    address = get_data(cursor_aw, 'Person.Address')
    state_province = get_data(cursor_aw, 'Person.StateProvince')
    country_region = get_data(cursor_aw, 'Person.CountryRegion')

    categories = pd.merge(sub_category, category, left_on="ProductCategoryID", right_on="ProductCategoryID")
    categories = categories.rename(columns={'Name_x': 'ProductSubCategory', 'Name_y': 'ProductCategory'})
    categories = categories.loc[:, ['ProductSubcategoryID', 'ProductSubCategory', 'ProductCategory']]
    
    # Som van alle hoeveelheden gepakt per ProductID, kan ook anders
    inventory = inventory.groupby('ProductID')['Quantity'].sum().reset_index()
    
    # Models
    merge = model.loc[:, ['ProductModelID', 'Name']].rename(columns={'Name': 'ModelName'})

    product1 = pd.merge(product, merge, left_on='ProductModelID', right_on='ProductModelID', how='left')
    product2 = pd.merge(product1, inventory, left_on='ProductID', right_on='ProductID', how='left')
    product3 = pd.merge(product2, categories, left_on='ProductSubcategoryID', right_on='ProductSubcategoryID', how='left')
   
    business_entity_address = business_entity_address.loc[:, ['BusinessEntityID', 'AddressID']]
    address = address.loc[:, ['AddressID', 'AddressLine1', 'City', 'StateProvinceID', 'PostalCode']].rename(columns={'AddressLine1': 'Vendor_Address', 'City': 'Vendor_City', 'PostalCode': 'Vendor_PostalCode'})
    state_province = state_province.loc[:, ['StateProvinceID', 'Name', 'CountryRegionCode']].rename(columns={'Name':'Vendor_StateProvinceName'})
    
    all_vendors = purchase_vendor.loc[:, ['ProductID', 'BusinessEntityID', 'StandardPrice', 'OnOrderQty']]
    vendors_merge = pd.merge(all_vendors, vendor, left_on='BusinessEntityID', right_on='BusinessEntityID', how='left')
    vendors_merge = vendors_merge.loc[:, ['ProductID', 'BusinessEntityID', 'StandardPrice', 'Name', 'ActiveFlag', 'PreferredVendorStatus', 'OnOrderQty']].rename(columns={'Name':'VendorName', 'StandardPrice':'StandardCost'})

    duplicated_product_ids = vendors_merge[vendors_merge.duplicated('ProductID', keep=False)]['ProductID'].unique()
    duplicated_vendors = vendors_merge[vendors_merge['ProductID'].isin(duplicated_product_ids)]
    unique_vendors = vendors_merge[~vendors_merge['ProductID'].isin(duplicated_product_ids)]

    duplicated_vendors = duplicated_vendors.sort_values(by=['ProductID', 'BusinessEntityID'])
    duplicated_vendors = duplicated_vendors.groupby('ProductID').apply(
        lambda group: group if group['ActiveFlag'].all() and group['PreferredVendorStatus'].all() else group.head(1)
    ).reset_index(drop=True)
    vendors_merge_clean_1 = pd.concat([duplicated_vendors, unique_vendors])
    
    def remove_nan_onorderqty(group):
        if group['OnOrderQty'].notna().any():
            return group.dropna(subset=['OnOrderQty'])
        return group.head(1)
    
    vendors_merge_clean = vendors_merge_clean_1.groupby('ProductID').apply(remove_nan_onorderqty).reset_index(drop=True)
    vendors_merge_clean = vendors_merge_clean.drop_duplicates(subset='ProductID', keep='first')
    vendor_info = vendors_merge_clean.loc[:, ['ProductID', 'BusinessEntityID', 'StandardCost', 'VendorName']]
    vendor_address = pd.merge(vendor_info, business_entity_address, left_on='BusinessEntityID', right_on='BusinessEntityID', how='left')
    vendor_address2 = pd.merge(vendor_address, address, left_on='AddressID', right_on='AddressID', how='left')
    state_country = pd.merge(state_province, country_region, left_on='CountryRegionCode', right_on='CountryRegionCode', how='left')
    state_country = state_country.loc[:, ['StateProvinceID', 'Vendor_StateProvinceName', 'Name']].rename(columns={'Name': 'Vendor_CountryName'})
    vendor_info2 = pd.merge(vendor_address2, state_country, left_on='StateProvinceID', right_on='StateProvinceID', how='left')

    aw_products = pd.merge(product3, vendor_info2, on='ProductID', how='left', suffixes=('_product3', '_vendor_info2'))
    aw_products['StandardCost_vendor_info2'] = aw_products['StandardCost_vendor_info2'].fillna(0.0000)
    aw_products['StandardCost'] = aw_products.apply(
        lambda row: row['StandardCost_vendor_info2'] if int(row['StandardCost_vendor_info2']) != 0 else row['StandardCost_product3'],
        axis=1)

    aw_products = aw_products.drop(columns=['StandardCost_product3', 'StandardCost_vendor_info2'])
    aw_products['ProductID'] = 'AW_' + aw_products['ProductID'].astype(str)

    return aw_products
 
def products_northwind():
    cursor_aw, cursor_nw, cursor_aenc, export_cursor = setup_cursors()

    nw_category = get_data(cursor_nw, "dbo.Categories")
    nw_products = get_data(cursor_nw, "dbo.Products")
    nw_suppliers = get_data(cursor_nw, "dbo.Suppliers")

    categories = nw_category.loc[:, ['CategoryID', 'CategoryName']].rename(columns={'CategoryName':'ProductCategory'})
    products = nw_products.loc[:, ['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'UnitPrice', 'UnitsInStock', 'ReorderLevel', 'Discontinued']]
    suppliers = nw_suppliers.loc[:, ['SupplierID', 'CompanyName','Address','City','Country','PostalCode']]
    
    merge1 = pd.merge(products, categories)
    merge2 = pd.merge(merge1, suppliers)
    merge2 = merge2.loc[:,['ProductID', 'ProductName','UnitPrice','UnitsInStock','ReorderLevel','Discontinued','ProductCategory','CompanyName','Address','City','Country','PostalCode']].rename(columns={'ProductName':'Name','UnitPrice':'ListPrice','UnitsInStock':'Quantity','CompanyName':'VendorName','Address':'Vendor_Address','City':'Vendor_City','PostalCode':'Vendor_PostalCode','Country':'Vendor_CountryName','ReorderLevel':'ReorderPoint'})

    merge2['ProductID'] = 'NW_' + merge2['ProductID'].astype(str)
    return merge2

def products_aenc():
    cursor_aw, cursor_nw, cursor_aenc, export_cursor = setup_cursors()
    products = get_data(cursor_aenc, 'Product')
    products = products.loc[:, ['id', 'name', 'description', 'prod_size','color','quantity','unit_price','Category']].rename(columns={'quantity':'Quantity', 'unit_price':'ListPrice','prod_size':'Size', 'id':'ProductID','description':'Name','name':'ProductSubCategory','Category':'ProductCategory', 'color':'Color'})
    products['ProductID'] = 'AC_' + products['ProductID'].astype(str)

    return products