import pandas as pd
import pymysql
from sqlalchemy import create_engine
# ---------------------------   EXTRACT --------------------------------#
#load csv rows data to python dataframe
dataframe_employee = pd.read_csv (r'employees.csv')
dataframe_timesheet = pd.read_csv (r'timesheets.csv')

#create connection to mysql localhost
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost:3306/{db}"
                       .format(user="root",
                               pw="",
                               db="mekari"))

#dataframe to mysql table with Full Capture
dataframe_employee.to_sql(con=engine, name='employees', if_exists='replace')
dataframe_timesheet.to_sql(con=engine, name='timesheets', if_exists='replace')

#try to modify the scheme of table employee and timesheets
conn_enhance = pymysql.connect(host='localhost', port=3306, user='root', passwd='', db='mekari', autocommit=True)
try:
    with conn_enhance.cursor() as cur:
        cur.execute(""" ALTER TABLE mekari.timesheets MODIFY COLUMN checkin TIME DEFAULT NULL NULL;""")
        cur.execute(""" ALTER TABLE mekari.timesheets MODIFY COLUMN checkout TIME DEFAULT NULL NULL;""")
        cur.execute(""" ALTER TABLE mekari.employees MODIFY COLUMN resign_date DATE DEFAULT NULL NULL;""")
        cur.execute(""" ALTER TABLE mekari.employees MODIFY COLUMN join_date DATE DEFAULT NULL NULL;""")
        cur.execute(""" ALTER TABLE mekari.timesheets MODIFY COLUMN date DATE DEFAULT NULL NULL;""")
finally:
    conn_enhance.close()

# just printout the result of ingestion process
print('Table timesheets Success insert to database !')
print('Number of colums in dataframe_timesheets : ', dataframe_timesheet.columns)
print('Number of rows in dataframe_timesheets : ', len(dataframe_timesheet.index))

print('Table employee Success insert to database !')
print('Number of colums in dataframe_employees : ', dataframe_employee.columns)
print('Number of rows in dataframe_employees : ', len(dataframe_employee.index))


# ---------------------------   TRANSFORM  --------------------------------#
#try to make new data set with name summary_timesheets
dataframe_summary_timesheet= pd.read_sql(""" WITH master as(
                                                SELECT 
                                                    t.employee_id ,
                                                    t.date,
                                                    t.date as working_date,
                                                    cast(time_to_sec(timediff(time(t.checkout),time(t.checkin))) / (60 * 60) as decimal(10, 1)) as working_hour,
                                                    e.branch_id ,e.salary ,e.join_date ,e.resign_date 
                                                FROM timesheets t left join employees e
                                                ON (t.employee_id=e.employe_id)
                                                WHERE 1=1 
                                                ORDER BY date)
                                            SELECT DISTINCT(employee_id),
                                                EXTRACT(YEAR_MONTH from date) as period ,
                                                month (date) as month_of_work, 
                                                year (date) as year_of_work, 
                                                sum(working_hour) as hour_in_month, 
                                                salary as salary_per_month,
                                                ROUND(salary/sum(working_hour),2) as fee_per_hour
                                            FROM master 
                                            GROUP BY employee_id,period
                                            ORDER BY employee_id,period DESC;""",engine)


# ---------------------------   LOAD  --------------------------------#
#load dataset to mysql database
dataframe_summary_timesheet.to_sql(con=engine, name='summary_timesheets', if_exists='replace', index=False)

print('Table summary_timesheets Success insert to database !')
print('Number of colums in summary_timesheets : ', dataframe_summary_timesheet.columns)
print('Number of rows in summary_timesheets : ', len(dataframe_summary_timesheet.index))

#try to load dataset to csv file
header=dataframe_summary_timesheet.columns
dataframe_summary_timesheet.to_csv('summary_timesheets.csv', columns = header,index=False) #-------- convert to csv ---------------#

del dataframe_employee
del dataframe_timesheet
del dataframe_summary_timesheet
