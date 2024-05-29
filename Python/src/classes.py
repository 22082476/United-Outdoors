class DateTable:
    def __init__(self, year, quarter, month, day, hour, minute, date):
        self.year = year
        self.quarter = quarter
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.date = date

class Employees:
    def __init__(self, adventure, aenc, northwind):
        self.adventure = adventure
        self.aenc = aenc
        self.northwind = northwind

    def get_employee(self, id):
        if "AW_" in id:
            matching_employees = [employee for employee in self.adventure if employee.employee_id == id]
            return matching_employees[0] if matching_employees else None
        elif "AC_" in id:
            matching_employees = [employee for employee in self.aenc if employee.employee_id == id]
            return matching_employees[0] if matching_employees else None
        elif "NW_" in id:
            matching_employees = [employee for employee in self.northwind if employee.employee_id == id]
            return matching_employees[0] if matching_employees else None
        else:
            return None
        

class Employee:
    def __init__(self, employee_id, employee_full_name, employee_extention, employee_sales_YTD, employee_sales_last_year, employee_department_head, employee_department, employee_start_date, employee_birth_date, employee_salary, employee_country, employee_region, employee_city, employee_zip_code, employee_street_name, employee_house_number, employee_manager, employee_health_insurance, employee_life_insurance, employee_day_care, employee_sex, employee_termination_date, employee_title, employee_title_of_courtesy, employee_group, employee_territory, employee_country_region_code, employee_vactions_hours, employee_sick_leave_hours, employee_martial_status, employee_orginanizion_level, employee_sales_quota, employee_bonus, employee_commission_pct):
        self.employee_id = employee_id
        self.employee_full_name = employee_full_name
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
        self.employee_vactions_hours = employee_vactions_hours
        self.employee_sick_leave_hours = employee_sick_leave_hours
        self.employee_martial_status = employee_martial_status
        self.employee_orginanizion_level = employee_orginanizion_level
        #self.employee_demographics = employee_demographics #waar moet het vandaan komen?
        self.employee_sales_quota = employee_sales_quota
        self.employee_bonus = employee_bonus
        self.employee_commission_pct = employee_commission_pct

class AddressTable:
    def __init__(self, country, region, city, postalcode, street, address):
        self.country = country
        self.region = region
        self.city = city
        self.postalcode = postalcode
        self.street = street
        self.address = address

class PayMethod:
    def __init__(self, paymethod_id, creditcard):
        self.paymethod_id = paymethod_id
        self.creditcard = creditcard

class SalesTerritory:
    def __init__(self, sales_territory_id, sales_territory_name, sales_territory_YTD, sales_territory_sales_last_year, sales_territory_cost_YTD, sales_territory_cost_last_year):
        self.sales_territory_id = sales_territory_id
        self.sales_territory_name = sales_territory_name
        self.sales_territory_YTD = sales_territory_YTD
        self.sales_territory_sales_last_year = sales_territory_sales_last_year
        self.sales_territory_cost_YTD = sales_territory_cost_YTD
        self.sales_territory_cost_last_year = sales_territory_cost_last_year

class SalesOrderReason:
    def __init__(self, salesorder_id, salesreason_id, salesreason_name, salesreason_type):
        self.salesorder_id = salesorder_id
        self.salesreason_id = salesreason_id
        self.salesreason_name = salesreason_name
        self.salesreason_type = salesreason_type

class Shippers:
    def __init__(self, shipper_id, company_name, Phone):
        self.shipper_id = shipper_id
        self.company_name = company_name
        self.Phone = Phone

class ShipMethod:
    def __init__(self, shipmethod_id, shipmethod_name, shipmethod_ship_base, shipmethod_ship_rate):
        self.shipmethod_id = shipmethod_id
        self.shipmethod_name = shipmethod_name
        self.shipmethod_ship_base = shipmethod_ship_base
        self.shipmethod_ship_rate = shipmethod_ship_rate


class SalesCurrency:
    def __init__(self, currency_code, currency_name):
        self.currency_code = currency_code
        self.currency_name = currency_name

class DateHandler:
    def __init__(self):
        self.month_mapping = {
            "january": 1, "jan": 1, "1": 1,
            "february": 2, "feb": 2, "2": 2,
            "march": 3, "mar": 3, "3": 3,
            "april": 4, "apr": 4, "4": 4,
            "may": 5, "may": 5, "5": 5,
            "june": 6, "jun": 6, "6": 6,
            "july": 7, "jul": 7, "7": 7,
            "august": 8, "aug": 8, "8": 8,
            "september": 9, "sep": 9, "9": 9,
            "october": 10, "oct": 10, "10": 10,
            "november": 11, "nov": 11, "11": 11,
            "december": 12, "dec": 12, "12": 12
        }

    def get_month_number(self, input_month):
        # Convert input to string and lowercase to handle case insensitivity
        input_month_str = str(input_month).strip().lower()
        # Remove leading zeros from numerical strings (e.g., "01" -> "1")
        if input_month_str.isdigit():
            input_month_str = str(int(input_month_str))
        # Look up the month number in the mapping dictionary
        return self.month_mapping.get(input_month_str, None)
    
    def format_date (self, day, month, year, time):
        print(f"{year}-{month}-{day} {time}")
        return f"{year}-{month}-{day} {time}" 