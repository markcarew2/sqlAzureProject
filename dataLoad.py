import pyodbc, struct
from azure import identity
import pandas as pd
from csv import reader
import string


connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:slippyandproud.database.windows.net,1433;Database=MySQLPractice;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'




def get_conn():
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn



"""
Get data and generate the shared column index, which will be used to generate Tables that only have shared columns
Commented out code was used to identify and fix inconsistencies with the index/primary key row "BID Name:" changes were made to the csvs
"""

fileStrings = ["fy16-bid-trends-report-data-1.csv", "fy17-bid-trends-report-data-1.csv", "fy18-bid-trends-report-data-1.csv", "fy19-bid-trends-report-data-1.csv"]
firstDF = pd.read_csv("fy16-bid-trends-report-data-1.csv")
for fileName in fileStrings:
    df = pd.read_csv(fileName)
    df.set_index('BID Name:', inplace=True)
    if fileName == "fy18-bid-trends-report-data-1.csv":
        df = df.iloc[:,:63]
    
    if fileName != "fy16-bid-trends-report-data-1.csv":
        sharedCol = theseCols.intersection(df.columns)
        #prevMissingRows = df.index.difference(prevIndex)
        #nextMissingRows = prevIndex.difference(df.index)
        #print("The previous year didn't have:" )
        #print(prevMissingRows)
        #print("This year didn't have:" )
        #print(nextMissingRows)

    if fileName == "fy16-bid-trends-report-data-1.csv":    
        theseCols = df.columns
        #prevIndex = df.index
        #theseRows = df.index
    else:
        theseCols = sharedCol
        #prevIndex = df.index
        #theseRows = missingRows
        #print(missingRows)

#create csv files with full set of files
cleanFileStrings = []
for fileName in fileStrings:
    df = pd.read_csv(fileName)
    df.set_index('BID Name:', inplace=True)
    df = df[sharedCol]
    sqlString = "CLEAN" + fileName[:4].replace('fy','20')
    cleanFileStrings.append(sqlString + ".csv")
    df.to_csv(sqlString + ".csv")


#Generate tables that only have shared columns
with get_conn() as conn:
    cursor = conn.cursor()
    
    for fileName in cleanFileStrings:
        
        try:
            cursor.execute(f"DROP TABLE {fileName[:-4]}")

        except:
            pass

        with open(fileName) as f:
            newReader = reader(f)
            columns = next(newReader)
            columnsStr = ", ".join(columns)
            createTable = f"CREATE TABLE {fileName[:-4]} ("
            for column in columns:
                column = column.translate(str.maketrans('', '', string.punctuation))
                column = column.replace(" ","")
                createTable += f"{column} varchar(255),"
            createTable = createTable[:-1]+")"

            #print(createTable)
            
            cursor.execute(createTable)

            for i, row in enumerate(newReader):
                for i, entry in enumerate(row):
                    row[i] =entry.replace("'","")
                rowString = "'"
                rowString +=  "', '".join(row)
                rowString+="'"
                #print (f"{i}" + rowString)
                insertSQL = f"INSERT INTO {fileName[:-4]} VALUES ({rowString})"
                #print(insertSQL)
                cursor.execute(insertSQL)
    
    cursor.execute("SELECT * FROM CLEAN2016")
    #print(cursor.fetchall())

#Generate Tables with full data
with get_conn() as conn:
    cursor = conn.cursor()
    
    for fileName in fileStrings:
        
        tableName = "FULL"+fileName[:4]

        try:
            cursor.execute(f"DROP TABLE {tableName}")

        except:
            pass

        with open(fileName) as f:
            newReader = reader(f)
            columns = next(newReader)
            columnsStr = ", ".join(columns)
            createTable = f"CREATE TABLE {tableName} ("
            for column in columns:
                column = column.translate(str.maketrans('', '', string.punctuation))
                column = column.replace(" ","")
                createTable += f"{column} varchar(255),"
            createTable = createTable[:-1]+")"

            #print(createTable)
            
            cursor.execute(createTable)

            for i, row in enumerate(newReader):
                for i, entry in enumerate(row):
                    row[i] =entry.replace("'","")

                rowString = "'"
                rowString +=  "', '".join(row)
                rowString+="'"
                #print (f"{i}" + rowString)
                insertSQL = f"INSERT INTO {tableName} VALUES ({rowString})"
                #print(insertSQL)
                #print(insertSQL)
                cursor.execute(insertSQL)
    
    cursor.execute("SELECT * FROM FULLfy16")
    print(cursor.fetchall())

"""
#Generate tables with the full set of data
with get_conn() as conn:
    for fileName in fileStrings:
        df = pd.read_csv(fileName)
        df.set_index('BID Name:', inplace=True)
        sqlString = "FULL" + fileName[:4].replace('fy','20')
        df.to_sql(sqlString, conn,if_exists='replace')

"""


