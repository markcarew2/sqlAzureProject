See app.py for the API part of the project.

See dataLoad.py for the data management part of the project.

Data pulled from https://data.world/city-of-ny/gt6r-wh7c, cleaned using pandas, and uploaded to an Azure SQL Database. Data is about Business improvement districts in New York City.

The API written in app.py was deployed as an azure app connected to the relevant Azure SQL Database. My trial subscription to azure ended so the database and site don't exist anymore.

The root of the API was running here: https://bidsqldata1.azurewebsites.net/

Endpoints include:
https://bidsqldata1.azurewebsites.net/CLEAN/TABLE/2016
https://bidsqldata1.azurewebsites.net/CLEAN/TABLE/2017
https://bidsqldata1.azurewebsites.net/CLEAN/TABLE/2018
https://bidsqldata1.azurewebsites.net/CLEAN/TABLE/2019 [returns the full tables for these years in JSON, format of JSON mirrored on pandas df.to_json function]


https://bidsqldata1.azurewebsites.net/CLEAN/BIDS [returns list of business improvement districts]
https://bidsqldata1.azurewebsites.net/CLEAN/BID/<Name of BID> [returns data for that BID across all years, type in spaces as %20]
https://bidsqldata1.azurewebsites.net/CLEAN/COLUMNS [returns columns of the data]
