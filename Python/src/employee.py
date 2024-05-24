from dotenv import load_dotenv
import pandas as pd
import os
import numpy as np
from handle import get_data, setup_cursor
load_dotenv('.env')

class Employee:
    def __init__(self, employee_id, employee_full_name, employee_ss_number, employee_phone_number, employee_home_phone_number, employee_extention, employee_sales_YTD, employee_sales_last_year, employee_department_head, employee_department, employee_start_date, employee_birth_date, employee_salary, employee_country, employee_region, employee_city, employee_zip_code, employee_street_name, employee_house_number, employee_manager, employee_health_insurance, employee_life_insurance, employee_day_care, employee_sex, employee_termination_date, employee_title, employee_title_of_courtesy, employee_group, employee_territory, employee_country_region_code, employee_salaried_flag, employee_vactions_hours, employee_sick_leave_hours, employee_martial_status, employee_orginanizion_level, employee_demographics, employee_sales_quota, employee_bonus, employee_commission_pct):
        self.employee_id = employee_id
        self.employee_full_name = employee_full_name
        self.employee_ss_number = employee_ss_number
        self.employee_phone_number = employee_phone_number
        self.employee_home_phone_number = employee_home_phone_number
        self.employee_extention = employee_extention
        self.employee_sales_YTD = employee_sales_YTD
        self.employee_sales_last_year = employee_sales_last_year
        self.employee_department_head = employee_department_head
        self.employee_department = employee_department
        self.employee_start_date = employee_start_date
        self.employee_birth_date = employee_birth_date
        self.employee_salary = employee_salary
        self.employee_country = employee_country
        self.employee_region = employee_region
        self.employee_city = employee_city
        self.employee_zip_code = employee_zip_code
        self.employee_street_name = employee_street_name
        self.employee_house_number = employee_house_number
        self.employee_manager = employee_manager
        self.employee_health_insurance = employee_health_insurance
        self.employee_life_insurance = employee_life_insurance
        self.employee_day_care = employee_day_care
        self.employee_sex = employee_sex
        self.employee_termination_date = employee_termination_date
        self.employee_title = employee_title
        self.employee_title_of_courtesy = employee_title_of_courtesy
        self.employee_group = employee_group
        self.employee_territory = employee_territory
        self.employee_country_region_code = employee_country_region_code
        self.employee_salaried_flag = employee_salaried_flag
        self.employee_vactions_hours = employee_vactions_hours
        self.employee_sick_leave_hours = employee_sick_leave_hours
        self.employee_martial_status = employee_martial_status
        self.employee_orginanizion_level = employee_orginanizion_level
        self.employee_demographics = employee_demographics
        self.employee_sales_quota = employee_sales_quota
        self.employee_bonus = employee_bonus
        self.employee_commission_pct = employee_commission_pct


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
    adventure = pd.merge(adventure, adventure_salesperson, on='BusinessEntityID', how='outer')
    adventure = pd.merge(adventure, adventure_payhistory, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_department_history, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_department, on='DepartmentID', how='inner')
    adventure = pd.merge(adventure, adventure_business_address, on='BusinessEntityID', how='inner')
    adventure = pd.merge(adventure, adventure_address, on='AddressID', how='inner')
    adventure = pd.merge(adventure, adventure_stateprovince, on='StateProvinceID', how='inner')
    adventure = pd.merge(adventure, adventure_countryregion, on='CountryRegionCode', how='inner')
    adventure.rename(columns={"Name": "country"}, inplace=True)
    adventure = pd.merge(adventure, adventure_salesterritory_history, on='BusinessEntityID', how='outer')
    adventure = pd.merge(adventure, adventure_salesterritory, on='TerritoryID', how='outer')
    adventure.rename(columns={"Name": "territory"}, inplace=True)
    #adventure = pd.merge(adventure, adventure_salesstore, left_on='BusinessEntityID', right_on='SalesPersonID', how='inner') Demographics mss veranderen
    #adventure = pd.merge(adventure, adventure_person_phone, on='BusinessEntityID', how='inner')
    
    adventure["full_name"] = adventure["FirstName"]+ " " + adventure["MiddleName"] + " " + adventure["LastName"]
    adventure["manager"] = None
    adventure["salary"] = adventure["Rate"] * 36
    adventure["emp_id"] = "AW_" + adventure["BusinessEntityID"].astype(str)
    adventure.drop(['FirstName', "CurrentFlag", "Rate", "PayFrequency", "IsOnlyStateProvinceFlag", "rowguid", "ModifiedDate_y", "ModifiedDate_x", "MiddleName", 'LastName', "Suffix", "NationalIDNumber", "PersonType", "OrganizationNode", "LoginID", "EmailPromotion", "NameStyle", "Title", "BusinessEntityID"], axis=1, inplace=True)
    adventure.rename(columns={"Gender":"sex"})

    print(adventure.columns)




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
    aenc.rename(columns={"steet": "address", "zip_code": "postal_code", "dept_name":"department"}, inplace=True)
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