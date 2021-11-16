import pandas as pd
import pandas as pd1
import logging
from datetime import datetime, timedelta
from mysql.connector import connect, Error
import mysql

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler("practice.log")
ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
fh_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
console_handler.setFormatter(ch_formatter)
file_handler.setFormatter(fh_formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)



#error handling to database from CSV and using functions
data = pd.read_csv (r'C:\Jeremy_code\CSV_To_Database\csvtodb.csv')   
df = pd.DataFrame(data)
data1 = pd1.read_csv (r'C:\Jeremy_code\CSV_To_Database\FAGLL03JUL211.csv', encoding = 'unicode_escape', low_memory=False)
df1 = pd1.DataFrame(data1)
row = None
row1= None
# dd = pd1.to_datetime(data1)
# dd= datetime.strptime('%d/%m/%Y', '%Y-%m-%d')


def insertdb(row): #functions
    try:
        mydb = mysql.connector.connect(
            host="192.168.1.55",
            user="zijiang.tee",
            password="!nttsg2021",
            database="zijiang.tee"
            )

        my_cursor = mydb.cursor()


        for row in df.itertuples():
            my_cursor.execute('''
                    INSERT INTO Practice(FirstName, LastName, Email, Phone, Address, City, State, Zip, Age)
                    VALUES (%s ,%s ,%s, %s ,%s ,%s, %s ,%s ,%s)
                    ''',
                    (row.FirstName,
                    row.LastName,
                    row.Email,
                    row.Phone,
                    row.Address,
                    row.City,
                    row.State,
                    row.Zip,
                    row.Age)
                    )
    except Error:
        print("error")
        logger.error("Unable to insert data", exc_info=True)
    finally:
        mydb.commit()

#insertdb(row) #calling out the functions

def insertdb_fall(row1): #functions
    try:
        mydb1 = mysql.connector.connect(
            host="192.168.1.55",
            user="zijiang.tee",
            password="!nttsg2021",
            database="zijiang.tee"
            )

        my_cursor1 = mydb1.cursor()

        # this command is to fill in the null value as empty string
        df1.Texts = df1.Texts.fillna('')
        df1.CostCenter = df1.CostCenter.fillna('')
        df1.SalesDocument = df1.SalesDocument.fillna('')
        df1.WBS = df1.WBS.fillna('')
        df1.PurchasingDocument = df1.PurchasingDocument.fillna('')
        df1.PersonnelName = df1.PersonnelName.fillna('')
        df1.Orders = df1.Orders.fillna('')
        #df1.to_datetime('DocumentDate')
        
        # df1['DocumentDate'] = datetime.strptime(df1['DocumentDate'], '%d/%m/%Y').strftime('%Y-%m-%d')

        # list(map(lambda x: datetime.datetime.strptime(x,'%d/%m/%Y').strftime('%Y-%m-%d'), df1['DocumentDate']))

        # df1['DocumentDate'] = df1['DocumentDate'].apply(lambda x: datetime.strptime(x, '%d/%m/%y').strftime('%Y-%m-%d'))

        for row1 in df1.itertuples():
            #print(row1.DocumentDate)
            docudate = datetime.strptime(row1.DocumentDate, '%d/%m/%Y').strftime('%Y-%m-%d')
            postdate = datetime.strptime(row1.PostingDate, '%d/%m/%Y').strftime('%Y-%m-%d')
            #print(docudate)
            my_cursor1.execute('''
                    INSERT INTO FAGLL03_test_copy(`Assignment`, `Reference`, `DocuNum`, `CompanyCode`, `DocumentType`, `DocumentDate`, `PostingDate`, `PostingPeriod`, `AccountLocal`,
                    `LocalCurrency`, `AmountDocu`, `DocuCurrency`, `Segment`, `Texts`, `Account`, `CostCenter`, `ProfitCenter`,`SalesDocument`, `SalesItem`, `WBS`, `PurchasingDocument`, `Item`,
                    `UserName`, `PersonnelNumber`, `PersonnelName`, `YearMonth`, `Orders`)
                    VALUES (%s, %s, %s, %s,%s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                    ''',
                    (row1.Assignment,
                    row1.Reference,
                    row1.DocuNum,
                    row1.CompanyCode,
                    row1.DocumentType,
                    docudate,
                    postdate,
                    row1.PostingPeriod,
                    row1.AccountLocal,
                    row1.LocalCurrency,
                    row1.AmountDocu,
                    row1.DocuCurrency,
                    row1.Segment,
                    row1.Texts,
                    row1.Account,
                    row1.CostCenter,
                    row1.ProfitCenter,
                    row1.SalesDocument,
                    row1.SalesItem,
                    row1.WBS,
                    row1.PurchasingDocument,
                    row1.Item,
                    row1.UserName,
                    row1.PersonnelNumber,
                    row1.PersonnelName,
                    row1.YearMonth,
                    row1.Orders)
                    )
    except Error:
        print("error")
        logger.error("Unable to insert data", exc_info=True)
    finally:
        mydb1.commit()

insertdb_fall(row1) #calling out the functions

#for actual report

# def insertdb_fall(row1): #functions
#     try:
#         mydb1 = mysql.connector.connect(
#             host="192.168.1.55",
#             user="weihow.yeo",
#             password="!nttsg2021",
#             database="weihow.yeo"
#             )

#         my_cursor1 = mydb1.cursor()
#         #df1.fillna(0)

#         for row1 in df1.itertuples():
#             my_cursor1.execute('''
#                     INSERT INTO FAGLL03_test(`Assignment`, `Reference`, `DocumentNumber`, `CompanyCode`, `DocumentType`, `DocumentDate`, `PostingDate`, `PostingPeriod`, `AccountLocal`,
#                     `LocalCurrency`, `AmtDoc`, `DocumentCurrency`, `Segment`, `Text`, `Account`, `CostCenter`, `ProfitCenter`, `SalesDocument`, `SalesItem`, `WBS`, `PurchasingDocument`, `Item`,
#                     `UserName`, `PersonnelNumber`, `PersonnelName`, `YearMonth`, `Orders`)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                     ''',
#                     (row1.Assignment,
#                     row1.Reference,
#                     row1.DocumentNumber,
#                     row1.CompanyCode,
#                     row1.DocumentType,
#                     row1.DocumentDate,
#                     row1.PostingDate,
#                     row1.PostingPeriod,
#                     row1.AccountLocal,
#                     row1.LocalCurrency,
#                     row1.AmtDoc,
#                     row1.DocumentCurrency,
#                     row1.Segment,
#                     row1.Text,
#                     row1.Account,
#                     row1.CostCenter,
#                     row1.ProfitCenter,
#                     row1.SalesDocument,
#                     row1.SalesItem,
#                     row1.WBS,
#                     row1.PurchasingDocument,
#                     row1.Item,
#                     row1.UserName,
#                     row1.PersonnelNumber,
#                     row1.PersonnelName,
#                     row1.YearMonth,
#                     row1.Orders)
#                     )
#     except Error:
#         print("error")
#         logger.error("Unable to insert data", exc_info=True)
#     finally:
#         mydb1.commit()

# insertdb_fall(row1) #calling out the functions


# working codes with entered values

# mydb = mysql.connector.connect(
#     host="192.168.1.55",
#     user="zijiang.tee",
#     password="!nttsg2021",
#     database="zijiang.tee"
#     )

# my_cursor = mydb.cursor()


# sqlstuff = "INSERT INTO Practice(FirstName, LastName, Email, Phone, Address, City, State, Zip, Age) VALUES (%s ,%s ,%s, %s ,%s ,%s, %s ,%s ,%s)"
# record1 = ("He", "she", "HEHO.com", 1234567, "SG", "SG", "SG", 123456, 12)

# my_cursor.execute(sqlstuff,record1)
# mydb.commit()

# first = "hee"
# last ="she"
# email = "hello.com"
# tele = 12345678999
# country = "SG"
# city = "SG"
# state = "SG"
# zip = 12345678999
# age = 15

# #record1 = ("He", "she", "HEHE.com", 1234567, "SG", "SG", "SG", 123456, 13)

# def insert_insight(first, last, email, tele, country, city, state, zip, age):
#     try:
#         with connect(
#                 host="dutabot.com",
#                 user="zijiang.tee",
#                 password="!nttsg2021",
#                 database="zijiang.tee",
#         ) as connection:
#             q = """INSERT INTO `zijiang.tee`.Practice (FirstName, LastName, Email, Phone, Address, City, State, Zip, Age
#              ) VALUES %s ,%s ,%s, %s ,%s ,%s, %s ,%s ,%s) """

#             with connection.cursor() as cursor:
#                 #record1 = (first, last, email, tele, country, city, state, zip, age)
#                 cursor.execute(q, (first, last, email, tele, country, city, state, zip, age))
#                 #logger.info(f"{cursor.rowcount} rows inserted into Practice")
#                 connection.commit()
#     except Error:
#         print("error")
#         #logger.error("Unable to insert data", exc_info=True)
#     finally:
#         cursor.close()
#         connection.close()

# insert_insight(first, last, email, tele, country, city, state, zip, age)

# try:
#     with connect(
#             host="dutabot.com",
#             user="zijiang.tee",
#             password="!nttsg2021",
#             database="zijiang.tee",
#     ) as connection:
#         q = """INSERT INTO `zijiang.tee`.Practice (FirstName, LastName, Email, Phone, Address, City, State, Zip, Age
#          ) VALUES %s ,%s ,%s, %s ,%s ,%s, %s ,%s ,%s) """

#         with connection.cursor() as cursor:
#             record1 = ("World", "Hello", "HEllo.com", 1234567, "NY", "NY", "NY", 34333456, 50)
#             cursor.execute(q, record1)
#             #logger.info(f"{cursor.rowcount} rows inserted into Practice")
#             connection.commit()
# except Error:
#     logger.error("Unable to insert data", exc_info=True)
# finally:
#     cursor.close()
#     connection.close()

# #insert_insight(record1, q)



# #Code to insert data into CSV w/o handling

# data = pd.read_csv (r'C:\Jeremy_code\CSV_To_Database\csvtodb.csv')   
# df = pd.DataFrame(data)

# print(df)

# mydb = mysql.connector.connect(
#     host="192.168.1.55",
#     user="zijiang.tee",
#     password="!nttsg2021",
#     database="zijiang.tee"
#     )

# my_cursor = mydb.cursor()


# for row in df.itertuples():
#     my_cursor.execute('''
#                 INSERT INTO Practice(FirstName, LastName, Email, Phone, Address, City, State, Zip, Age)
#                 VALUES (%s ,%s ,%s, %s ,%s ,%s, %s ,%s ,%s)
#                 ''',
#                 (row.FirstName,
#                 row.LastName,
#                 row.Email,
#                 row.Phone,
#                 row.Address,
#                 row.City,
#                 row.State,
#                 row.Zip,
#                 row.Age)
#                 )
# mydb.commit()



# # error handling to database

# data = pd.read_csv (r'C:\Jeremy_code\CSV_To_Database\csvtodb.csv')   
# df = pd.DataFrame(data)

# try:
#     mydb = mysql.connector.connect(
#         host="192.168.1.55",
#         user="zijiang.tee",
#         password="!nttsg2021",
#         database="zijiang.tee"
#         )

#     my_cursor = mydb.cursor()


#     for row in df.itertuples():
#         my_cursor.execute('''
#                 INSERT INTO Practice(FirstName, LastName, Email, Phone, Address, City, State, Zip, Age)
#                 VALUES (%s ,%s ,%s, %s ,%s ,%s, %s ,%s ,%s)
#                 ''',
#                 (row.FirstName,
#                 row.LastName,
#                 row.Email,
#                 row.Phone,
#                 row.Address,
#                 row.City,
#                 row.State,
#                 row.Zip,
#                 row.Age)
#                 )
    
# except Error:
#     print("error")
#     logger.error("Unable to insert data", exc_info=True)
# finally:
#     mydb.commit()


#     my_cursor.execute('''
#                 INSERT INTO Practice(FirstName, LastName, Email, Phone, Address, City, State, Zip, Age)
#                 VALUES (%s ,%s ,%s, %s ,%s ,%s, %s ,%s ,%s)
#                 ''',
#                 (row.FirstName,
#                 row.LastName,
#                 row.Email,
#                 row.Phone,
#                 row.Address,
#                 row.City,
#                 row.State,
#                 row.Zip,
#                 row.Age)
#                 )
# mydb.commit()


