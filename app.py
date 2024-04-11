import os
import pyodbc, struct
from azure import identity

from typing import Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse
import json

class Person(BaseModel):
    first_name:str
    last_name:Union[str,None]=None

connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:slippyandproud.database.windows.net,1433;Database=MySQLPractice;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'


app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
def root():
    print("Root of Business ")

    return "This is the test API for New York Business Improvement District data stored on my AzureSQL server (pulled from https://data.world/city-of-ny/).\n\
It can return specific views of the BID data sets.\n\
I cleaned the data so that the names of the BIDs lined up across years, in the original data, the names would change between years making that column less useful as a key\n\
/CLEAN gives a short description of the different views available."

@app.get("/CLEAN", response_class=PlainTextResponse)
def cleanData():
    print("root of clean data sets")
    return "/CLEAN/TABLE/2016, /CLEAN/TABLE/2017. /CLEAN/TABLE/2018. /CLEAN/TABLE/2019 return the full tables for those years\n\
/CLEAN/BID/<Name of BID> returns data for that BID across all years, type in spaces as %20\n\
/CLEAN/BIDS returns the names of BIDs"

@app.get("/CLEAN/COLUMNS")
def cleanColumns():
    print("Returning list of COlumns")
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'CLEAN2016'")
        columns = '{"columns":["'
        colList = []
        cols = cursor.fetchall()
        for col in cols:
            colList.append(col[0])
        colString = '", "'.join(colList)
        columns = columns + colString + '"]}'
        print(columns)
    jsonColumn = json.loads(columns)
    #print(jsonColumn)
    return jsonColumn
    

@app.get("/CLEAN/TABLE/{year}")
def getYear(year: str):
    with get_conn() as conn:
        
        cursor = conn.cursor()
        if year not in ["2016","2017","2018","2019"]:
            raise HTTPException(status_code=404,
            detail="404 Error: Year not in database")


        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'CLEAN{year}'".format(year=year))
        
        columns = '{"columns":["'
        colList = []
        cols = cursor.fetchall()
        for col in cols:
            colList.append(col[0])
        colString = '", "'.join(colList)
        columns = columns + colString + '"],'
        
        tableString = columns + '"data":['
        
        cursor.execute("SELECT * FROM CLEAN{year}".format(year=year))

        table = cursor.fetchall()

        for row in table:
            tableString+='["'
            rowString = '", "'.join(row)
            tableString = tableString + rowString + '"],'
        tableString=tableString[:-1] + ']}'

        #print(tableString[40230:40250])
        tableString = json.loads(tableString)
        #print(tableString)

       # for row in table:
           # for point 
    return tableString

@app.get("/CLEAN/BID/{bid}")
def get_bid(bid: str):
    bid = bid.replace("%20", " ")
    
    with get_conn() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT BIDName FROM CLEAN2019")
        colList = []
        cols = cursor.fetchall()
        for col in cols:
            colList.append(col[0])


        if bid not in colList:
            raise HTTPException(status_code=404, detail="404 Error no such BID")
       
        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'CLEAN2016'")
       
        
        columns = '{"columns":["'
        colList = []
        cols = cursor.fetchall()
        for col in cols:
            colList.append(col[0])
        colString = '", "'.join(colList)
        columns = columns + colString + '"],'
        
        tableString = columns + '"index":["2016","2017","2018","2019"], "data":['

        bidData = []
        for ele in ['2016','2017','2018','2019']:
           # print("SELECT * FROM CLEAN{yeary} WHERE BIDName = {biddy}".format(yeary = ele, biddy = bid))
            cursor.execute("SELECT * FROM CLEAN{yeary} WHERE BIDName = '{biddy}'".format(yeary = ele, biddy = bid))
            bidData.append(cursor.fetchone())
        #print(bidData)
        for row in bidData:
            tableString+='["'
            rowString = '", "'.join(row)
            tableString = tableString + rowString + '"],'
        tableString=tableString[:-1] + "]}"
        tableString = json.loads(tableString)
    return tableString

@app.get("/CLEAN/BIDS")
def get_bids():
    
    with get_conn() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT BIDName FROM CLEAN2019")
        columns = '{"BIDS":["'
        colList = []
        cols = cursor.fetchall()
        for col in cols:
            colList.append(col[0])
        colString = '", "'.join(colList)
        columns = columns + colString + '"]}'
        
    columns = json.loads(columns)

    return columns



def get_conn():
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn

'''
create_table()
table = get_persons()
print(table)
person1 = Person("Jane", "Fonda")
create_person(person1)
table = get_persons()
print(table)



def get_persons():
    rows = []
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Persons")

        for row in cursor.fetchall():
            print(row.FirstName, row.LastName)
            rows.append(f"{row.ID}, {row.FirstName}, {row.LastName}")
    return rows

def get_person(person_id: int):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Persons WHERE ID = ?", person_id)

        row = cursor.fetchone()
        return f"{row.ID}, {row.FirstName}, {row.LastName}"

def create_person(item: Person):
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Persons WHERE FirstName='{fname}' AND LastName='{lname}'".format(fname=item.first_name,lname=item.last_name))
        row = cursor.fetchone()
        if row is None:
            cursor.execute(f"INSERT INTO Persons (FirstName, LastName) VALUES (?, ?)", item.first_name, item.last_name)
            conn.commit()
        else:
            print("name in db")
        

    return item

def create_table():
    try:
        conn = get_conn()
        cursor = conn.cursor()

        # Table should be created ahead of time in production app.
        cursor.execute("""
            CREATE TABLE Persons (
                ID int NOT NULL PRIMARY KEY IDENTITY,
                FirstName varchar(255),
                LastName varchar(255)
            );
        """)

        conn.commit()
    except Exception as e:
        # Table may already exist
        print(e)


'''
