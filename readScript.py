import mysql.connector as msc
import pandas as pd


df=pd.read_csv("sample.txt",skiprows=1,delimiter="|",names=["DataType","Customer Name","Customer ID","Customer Open Date","Last Consulted Date","Vaccination Type","Doctor Consulted","State","Country","Post Code","Date of Birth","Active Customer"])
print(df)
list=df.loc[df['DataType']=='D']
for i in range(len(df)) :
  print(df.iloc[i, 0], df.iloc[i, 11])
print(list)
print(df.columns)
try:
    mydb=msc.connect(host="localhost",user="root",password="1970",database="incubyte")
    mycursor=mydb.cursor()

    CreateQuery = """CREATE TABLE IF NOT EXISTS Customer( 
                             CustomerName VARCHAR(255) NOT NULL primary key,
                             CustomerID VARCHAR(18) NOT NULL,
                             CustomerOpenDate DATE,
                             LastConsultedDate DATE,
                             VaccinationType CHAR(5),
                             DoctorConsulted CHAR(255),
                             State CHAR (5),
                             Country CHAR (5),
                             PostCode INT(5),
                             DateofBirth DATE,
                             ActiveCustomer CHAR(1)
                             ) """

    mycursor.execute(CreateQuery)
    print("Customer Table created successfully ")

except msc.Error as error:
    print("Failed to create table in MySQL: {}".format(error))
finally:
    if mydb.is_connected():
        mycursor.close()
        mydb.close()
        print("MySQL connection is closed")
