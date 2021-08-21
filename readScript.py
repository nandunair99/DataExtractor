import mysql.connector
import mysql.connector as msc
import pandas as pd

def fetch_data():
    df = pd.read_csv("sample.txt", delimiter="|",
                     names=["Spacing","DataType", "CustomerName", "CustomerID", "CustomerOpenDate", "LastConsultedDate",
                            "VaccinationType", "DoctorConsulted", "State", "Country", "PostCode", "DateofBirth",
                            "ActiveCustomer"])

    df=df.loc[df['DataType'] == 'D']
    df[["LastConsultedDate","PostCode","DateofBirth"]]=df[["LastConsultedDate","PostCode","DateofBirth"]].astype(int)
    print(df.dtypes)
    

    return df

def format_date(i,df):
    x = df.iloc[i, 11]
    x=str(x)
    l=len(x)
    if(l==7):
        #print(7)
        year = x[3:7]
        month = x[1:3]
        date = '0'+x[:1]
    else:
        #print(8)
        year = x[4:8]
        month = x[2:4]
        date = x[:2]


    date=year+month+date
    return date

def insert_data(df):
    mydb = msc.connect(host="localhost", user="root", password="1970", database="incubyte")
    mycursor = mydb.cursor()

    try:
        for i in range(len(df)):

            CreateQuery = """CREATE TABLE IF NOT EXISTS """+df.iloc[i, 9]+"""(
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
            print("Table created successfully ")
            InsertQuery = """INSERT INTO """+df.iloc[i, 9]+""" (CustomerName, CustomerID, CustomerOpenDate,LastConsultedDate, VaccinationType, DoctorConsulted,State, Country, PostCode,DateofBirth, ActiveCustomer) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
            print(df)

            try:
                mycursor.execute(InsertQuery, (
                df.iloc[i, 2], int(df.iloc[i, 3]), int(df.iloc[i, 4]), int(df.iloc[i, 5]), df.iloc[i, 6],
                df.iloc[i, 7],df.iloc[i, 8], df.iloc[i, 9], int(df.iloc[i, 10]), int(format_date(i, df)), df.iloc[i, 12]))

                print("Customer Inserted successfully ")
            except mysql.connector.IntegrityError as exc:
                print("Record already exists...")



    except msc.Error as error:
            print("Failed to create table in MySQL: {}".format(error))
    finally:
            if mydb.is_connected():
                mydb.commit()
                mycursor.close()
                mydb.close()
                print("MySQL connection is closed")



def main():
    print("hello in ")
    df=fetch_data()
    insert_data(df)

if __name__=="__main__":
    main()