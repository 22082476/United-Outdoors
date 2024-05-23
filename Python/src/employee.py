from dotenv import load_dotenv
import pandas as pd
import os
from handle import get_data, setup_cursor
load_dotenv('.env')



def employee ():
    #adventure_employee = adventure_employee()
    #aenc_employee = aenc_employee()
    #northwind_employee = northwind_employee()
    adventure_employee()
    aenc_employee()
    northwind_employee()


def adventure_employee ():
    adventure_salesperson = get_data(setup_cursor(os.getenv('adventureworks')), "Sales.SalesPerson")
    adventure_employee = get_data(setup_cursor(os.getenv('adventureworks')), "HumanResources.Employee")
    adventure_payhistory = get_data(setup_cursor(os.getenv('adventureworks')), "HumanResources.EmployeePayHistory")
    adventure_department = get_data(setup_cursor(os.getenv('adventureworks')), "HumanResources.Department")
    adventure_department_history = get_data(setup_cursor(os.getenv('adventureworks')), "HumanResources.EmployeeDepartmentHistory")
    adventure_countryregion = get_data(setup_cursor(os.getenv('adventureworks')), "Person.CountryRegion")
    adventure_business_address = get_data(setup_cursor(os.getenv('adventureworks')), "Person.BusinessEntityAddress")
    adventure_address = get_data(setup_cursor(os.getenv('adventureworks')), "Person.Address")
    adventure_stateprovince = get_data(setup_cursor(os.getenv('adventureworks')), "Person.StateProvince")
    adventure_salesterritory = get_data(setup_cursor(os.getenv('adventureworks')), "Sales.SalesTerritory")
    adventure_salesterritory_history = get_data(setup_cursor(os.getenv('adventureworks')), "Sales.SalesTerritoryHistory")
    adventure_salesstore = get_data(setup_cursor(os.getenv('adventureworks')), "Sales.Store")
    adventure_person = get_data(setup_cursor(os.getenv('adventureworks')), "Person.Person")
    adventure_person_phone = get_data(setup_cursor(os.getenv('adventureworks')), "Person.PersonPhone")

    adventure_salesperson = adventure_salesperson.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_employee = adventure_employee.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_payhistory = adventure_payhistory.drop(["ModifiedDate"], axis=1)
    adventure_department = adventure_department.drop(["ModifiedDate"], axis=1)
    adventure_countryregion = adventure_countryregion.drop(["ModifiedDate"], axis=1)
    adventure_business_address = adventure_business_address.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_address = adventure_address.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_stateprovince = adventure_stateprovince.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_salesterritory = adventure_salesterritory.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_salesstore = adventure_salesstore.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_person = adventure_person.drop(["rowguid", "ModifiedDate"], axis=1)
    adventure_person_phone = adventure_person_phone.drop(["ModifiedDate"], axis=1)

    

    adventure = pd.merge(adventure_person, adventure_employee, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_salesperson, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_payhistory, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_department_history, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_department, on='DepartmentID', how='inner')
    adventure = pd.merge(adventure, adventure_business_address, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_address, on='AddressID', how='inner')
    adventure = pd.merge(adventure, adventure_stateprovince, on='StateProvinceID', how='inner')
    adventure = pd.merge(adventure, adventure_countryregion, on='CountryRegionCode', how='inner')
    adventure.rename(columns={"Name": "country"}, inplace=True)
    adventure = pd.merge(adventure, adventure_salesterritory_history, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_salesterritory, on='TerritoryID', how='inner')
    adventure.rename(columns={"Name": "territory"}, inplace=True)
    #adventure = pd.merge(adventure, adventure_salesstore, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_person_phone, on='BusinessEntityID', how='inner')
    
    print(adventure)




def aenc_employee ():
    aenc_employee = get_data(setup_cursor(os.getenv('aenc')), "employee")
    aenc_department = get_data(setup_cursor(os.getenv('aenc')), "department")
    aenc_state = get_data(setup_cursor(os.getenv('aenc')), "state")

    aenc = pd.merge(aenc_employee, aenc_department, on='dept_id', how='inner')
    aenc = pd.merge(aenc, aenc_state, left_on='state', right_on='state_id', how='inner')

    aenc["full_name"] = aenc["emp_fname"] + " " + aenc["emp_lname"]
    aenc["manager"] = aenc.apply(lambda row: aenc.loc[aenc['emp_id'] == row['manager_id'], 'full_name'].values[0] if pd.notnull(row['manager_id']) else None, axis=1)
    aenc["department_head"] = aenc.apply(lambda row: aenc.loc[aenc['emp_id'] == row['dept_head_id'], 'full_name'].values[0] if pd.notnull(row['manager_id']) else None, axis=1)
    aenc["region"] = aenc["region"].apply(lambda x: "Southern" if x == "South" else x)
    aenc["emp_id"] = "AC_" + aenc["emp_id"].astype(str)
    aenc.rename(columns={"steet": "address", "zip_code": "postal_code", "dept_name":"department", }, inplace=True)
    aenc.drop(['emp_fname', 'emp_lname', "manager_id", "dept_head_id"], axis=1, inplace=True)

    return aenc



def northwind_employee (): 
    northwind_employee = get_data(setup_cursor(os.getenv('northwind')), "employees")
    northwind_employee_territory = get_data(setup_cursor(os.getenv('northwind')), "EmployeeTerritories")
    northwind_territory = get_data(setup_cursor(os.getenv('northwind')), "territories")
    northwind_region = get_data(setup_cursor(os.getenv('northwind')), "region")

    northwind = pd.merge(northwind_employee, northwind_employee_territory, on='EmployeeID', how='inner')
    northwind = pd.merge(northwind, northwind_territory, on='TerritoryID', how='inner')
    northwind = pd.merge(northwind, northwind_region, on='RegionID', how='inner')

    
    northwind["full_name"] = northwind["FirstName"] + " " + northwind["LastName"]
    northwind["manager"] = northwind.apply(lambda row: northwind.loc[northwind['EmployeeID'] == row['ReportsTo'], 'full_name'].values[0] if pd.notnull(row['ReportsTo']) else None, axis=1)
    northwind["EmployeeID"] = "NW_" + northwind["EmployeeID"].astype(str)
    northwind.drop(['FirstName', 'LastName', 'ReportsTo', "Photo", "Notes", "RegionID", "Region"], axis=1, inplace=True)
    northwind.rename(columns={"Address": "address", "PostalCode": "postal_code", "EmployeeID":"emp_id", "HireDate": "start_date", "BirthDate":"birth_date", "City": "city", "Country":"country", "Region":"region", "HomePhone":"home_phone", "Title":"title", "TitleOfCourtesy":"title_of_courtesy", "RegionDescription":"region"}, inplace=True)

    return northwind