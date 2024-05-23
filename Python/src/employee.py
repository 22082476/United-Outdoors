from dotenv import load_dotenv
import os
from handle import get_data, setup_cursor
load_dotenv('.env')



def employee ():
    adventure_salesperson = get_data(setup_cursor(os.getenv('adventureworks'), "Sales.SalesPerson"))
    adventure_employee = get_data(setup_cursor(os.getenv('adventureworks'), "HumanResource.Employee"))
    adventure_payhistory = get_data(setup_cursor(os.getenv('adventureworks'), "HumanResource.EmployeePayHistory"))
    adventure_department = get_data(setup_cursor(os.getenv('adventureworks'), "HumanResource.Departments"))
    adventure_countryregion = get_data(setup_cursor(os.getenv('adventureworks'), "Person.CountryRegion"))
    adventure_address = get_data(setup_cursor(os.getenv('adventureworks'), "Person.Address"))
    adventure_stateprovince = get_data(setup_cursor(os.getenv('adventureworks'), "Person.StateProvince"))
    adventure_salesterritory = get_data(setup_cursor(os.getenv('adventureworks'), "Sales.SalesTerritory"))
    adventure_salesstore = get_data(setup_cursor(os.getenv('adventureworks'), "Sales.Store"))
    adventure_person = get_data(setup_cursor(os.getenv('adventureworks'), "Person.Person"))
    adventure_person_phone = get_data(setup_cursor(os.getenv('adventureworks'), "Person.PersonPhone"))

    aenc_employee = get_data(setup_cursor(os.getenv('aenc'), "employee"))
    aenc_department = get_data(setup_cursor(os.getenv('aenc'), "department"))
    aenc_state = get_data(setup_cursor(os.getenv('aenc'), "state"))

    northwind_employee = get_data(setup_cursor(os.getenv('northwind'), "employees"))
    northwind_territory = get_data(setup_cursor(os.getenv('northwind'), "territories"))

    print(adventure_salesperson)
    print(adventure_employee)
    print(adventure_payhistory)
    print(adventure_department)
    print(adventure_countryregion)
    print(adventure_address)
    print(adventure_stateprovince)
    print(adventure_salesterritory)
    print(adventure_salesstore)
    print(adventure_person)
    print(adventure_person_phone)
    print(aenc_employee)
    print(aenc_department)
    print(aenc_state)
    print(northwind_employee)
    print(northwind_territory)

    

