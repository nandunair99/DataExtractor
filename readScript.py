import mysql.connector as msc
import pandas as pd


def fetch_data():
    df = pd.read_csv("sample.txt", skiprows=1, delimiter="|",
                     names=["DataType", "Customer Name", "Customer ID", "Customer Open Date", "Last Consulted Date",
                            "Vaccination Type", "Doctor Consulted", "State", "Country", "Post Code", "Date of Birth",
                            "Active Customer"])
    df=df[df['DataType'] == 'D']
    print(df.dtypes)
    for i in range(len(df)):
        print(df.iloc[i, 1], df.iloc[i, 11])

    return df

def insert_data(df):
    try:
        mydb = msc.connect(host="localhost", user="root", password="1970", database="incubyte")
        mycursor = mydb.cursor()

        CreateQuery = """CREATE TABLE IF NOT EXISTS Customer( 
                                 CustomerName VARCHAR(255) NOT NULL primary key,
                                 CustomerID VARCHAR(18) NOT NULL,
                                 CustomerOpenDate DATE NOT NULL,
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
        InsertQuery="""
        INSERT INTO Customer (CustomerName, CustomerID, CustomerOpenDate,LastConsultedDate, VaccinationType, DoctorConsulted,State, Country, PostCode,DateofBirth, ActiveCustomer)
         VALUES ("%df.iloc[0, 1]", %df.iloc[0, 2], %df.iloc[0, 3], %df.iloc[0, 4], "%df.iloc[0, 5]", "%df.iloc[0, 6]", "%df.iloc[0, 7]", "%df.iloc[0, 8]", %df.iloc[0, 9], df.iloc[0, 10], "%df.iloc[0, 10]")
         WHERE NOT EXISTS (
         SELECT name FROM CustomerName WHERE CustomerID='%df.iloc[0, 2]'
         )
        """
        mycursor.execute(InsertQuery)
        print("Customer Inserted successfully ")

    except msc.Error as error:
        print("Failed to create table in MySQL: {}".format(error))
    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
            print("MySQL connection is closed")
    print(df)


def main():
    print("hello in ")
    df=fetch_data()
    insert_data(df)
if __name__=="__main__":
    main()