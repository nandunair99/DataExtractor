import mysql.connector
import mysql.connector as msc
import pandas as pd

#Extracting data from txt file and fetching required information
def fetch_data():
    df = pd.read_csv("sample.txt", delimiter="|",
                     names=["Spacing","DataType", "CustomerName", "CustomerID", "CustomerOpenDate", "LastConsultedDate",
                            "VaccinationType", "DoctorConsulted", "State", "Country", "PostCode", "DateofBirth",
                            "ActiveCustomer"])


    df=df.loc[df['DataType'] == 'D']
    df=df.drop(['Spacing'], axis=1)

    # handling Null data
    df["LastConsultedDate"].fillna("null", inplace = True)
    df["VaccinationType"].fillna("null", inplace=True)
    df["DoctorConsulted"].fillna("null", inplace=True)
    df["State"].fillna("null", inplace=True)
    df["PostCode"].fillna("null", inplace=True)
    df["DateofBirth"].fillna(0, inplace=True)
    df["ActiveCustomer"].fillna("-", inplace=True)
    df["Country"].fillna("Other", inplace=True) #country not mentioned then default set to "other"
   




    return df

#formating date of birth column values from dd/mm/yyyy to yyyy/mm/dd format
def format_date(i,df):
    x = df.iloc[i, 10]
    if(x!="null"):
        x = int(x)
        x = str(x)
        l = len(x)
        print(l)
        if (l == 7):
            year = x[3:7]
            month = x[1:3]
            date = '0' + x[:1]
        else:
            year = x[4:8]
            month = x[2:4]
            date = x[:2]

        date = year + month + date

        return int(date)
    else:
        return 0





#Inserting data to mysql database tables
def insert_data(df):
    #Connection string
    mydb = msc.connect(host="localhost", user="root", password="1970", database="incubyte")
    mycursor = mydb.cursor()

    try:
        for i in range(len(df)):
            #Creating table if does not exist with table name as country i.e df.iloc[i, 8]
            CreateQuery = """CREATE TABLE IF NOT EXISTS """+df.iloc[i, 8]+"""(
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


            #inserting into database tables
            InsertQuery = """INSERT INTO """+df.iloc[i, 8]+""" (CustomerName, CustomerID, CustomerOpenDate,LastConsultedDate, VaccinationType, DoctorConsulted,State, Country, PostCode,DateofBirth, ActiveCustomer) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
            print(df)
            #handling Integrity constraints
            try:
                mycursor.execute(InsertQuery, (
                df.iloc[i, 1], int(df.iloc[i, 2]), int(df.iloc[i, 3]), 0 if df.iloc[i, 4]=="null" else int(df.iloc[i, 4]), df.iloc[i, 5],
                df.iloc[i, 6],df.iloc[i, 7], df.iloc[i, 8], 0 if df.iloc[i, 9]=="null" else int(df.iloc[i, 9]), (format_date(i, df)), df.iloc[i, 11]))

                print("Customer Inserted successfully ")
            except mysql.connector.IntegrityError as exc:
                print("Record already exists...")



    except msc.Error as error:
        #create table exception
            print("Failed to create table in MySQL: {}".format(error))
    finally:
            if mydb.is_connected():
                mydb.commit()
                mycursor.close()
                mydb.close()
                print("MySQL connection is closed")



def main():

    df=fetch_data()
    insert_data(df)

if __name__=="__main__":
    main()