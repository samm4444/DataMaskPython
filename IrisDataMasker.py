#!/usr/bin/env python3
import requests
import arguably
import mysql.connector
import json
from getpass import getpass
from tqdm import tqdm
from partial import partial
from redact import redact
from regex import regex
import logging
from scrambleInt import scrambleInt
import setupFields

logger = logging.getLogger(__name__)

__version__ = "1.0.0"

# open secrets file and import all client secrets.
secrets = {}
with open("secrets", "w+") as f:
    for line in f.readlines():
        ld = line.split("=")
        secrets[ld[0]] = ld[1]

@arguably.command
def mask(inputDB: str, outputDB: str, config: str, *, logLevel: str = "INFO"):
    '''
    Mask the sensitive data in your mySQL database

    Args:
        inputDB (str): The address of the input database. Format: host:database:table
        outputDB (str): The address for the output database. Format: host:database:table
        config (str): JSON file containing masking options for each field in the tables. This can be generated using the setup command.
        logLevel (str, optional): [-L] Log level for output (e.g., DEBUG, INFO, WARNING) 
    '''
    # set up logger to specified log level
    logger.setLevel(logLevel)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "{levelname} - {message}",
        style="{"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # read config file
    try:
        configData = json.loads(open(config).read())["fields"]
    except FileNotFoundError:
        logger.error("Could not find config file: " + config)
        return

    inputDBdata = inputDB.split(":")
    try:
        inputDBHost = inputDBdata[0]
        inputDatabaseName = inputDBdata[1]
        inputDBTable = inputDBdata[2]
    except IndexError:
        logger.error("Input database malformed: " + inputDB)
        return
    # get username and password input for db
    logger.info("Logging into " + inputDBHost)
    inputDBUsername = input("username =>")
    inputDBPassword = getpass()

    # establish database connection with credentials
    logger.info("Connecting...")
    inputDBconnection = mysql.connector.connect(
    host=inputDBHost,
    user=inputDBUsername,
    password=inputDBPassword,
    database=inputDatabaseName
    )
    logger.info("Connection Successful")

    inputDBCursor = inputDBconnection.cursor()
    logger.debug("Created input database cursor")

    logger.info("Loading data from table: " + inputDBTable)
    
    # get the names of each field and their index within a row
    inputDBCursor.execute("DESCRIBE "+inputDBTable+";")
    columns = {}
    for i,column in enumerate(inputDBCursor.fetchall()):
        columns[column[0]] = i
    logger.debug("Got input database column names: " + str(len(columns.items())) + " columns")

    # get rows of data to be masked
    inputDBCursor.execute("SELECT * FROM "+inputDBTable+";")
    inputDBRows = inputDBCursor.fetchall()
    logger.debug("Got " + str(len(inputDBRows)) + " rows")

    outputTable = []
    

    for row in tqdm(inputDBRows, total=len(inputDBRows),desc="Masking data"):
        outputRow = {}
        # print(columns)
        for columnName,i in columns.items():
            inputData = row[i]
            if columnName not in configData.keys(): outputRow[columnName] = inputData; continue; # ignore fields not specified for masking

            maskSettings = configData[columnName] # get masking settings for this column

            maskingType = maskSettings["maskingType"]
            if maskingType == None: raise ValueError("No Masking type for field: " + columnName)

            if maskingType == "regex": # Mask data with REGEX
                pattern = maskSettings["pattern"]
                replacement = maskSettings["replacement"]
                outputData = regex(inputData,pattern,replacement)
                outputRow[columnName] = outputData

            elif maskingType == "redact": # Mask data with Redact
                replacement = maskSettings["replacement"]
                outputData = redact(inputData,replacement)
                outputRow[columnName] = outputData

            elif maskingType == "partial": # Mask data with Partial
                visiblePrefix = maskSettings["visiblePrefix"]
                visibleSuffix = maskSettings["visibleSuffix"]
                replacement = maskSettings["replacement"]
                outputData = partial(inputData,visiblePrefix,visibleSuffix,replacement)
                outputRow[columnName] = outputData
            elif maskingType == "scrambleInt":
                min = maskSettings["min"]
                max = maskSettings["max"]
                length = None
                if maskSettings["length"].lower() != "none":
                    length = int(maskSettings["length"])
                outputData = scrambleInt(inputData,min,max,length)
                outputRow[columnName] = outputData

            else: # Fail due to unknown masking type in config file
                logger.error("Unsupported Masking Type: " + maskingType)
                return
                        
        outputTable.append(outputRow)


    outputDBdata = outputDB.split(":")
    try:
        outputDBHost = outputDBdata[0]
        outputDatabaseName = outputDBdata[1]
        outputDBTable = outputDBdata[2]
    except IndexError:
        logger.error("Output database malformed: " + outputDB)
        return

    # get username and password input for db if the host is different to the input db
    logger.info("Logging into " + outputDBHost)
    outputDBUsername = ""
    outputDBPassword = ""
    if outputDBHost == inputDBHost:
        outputDBUsername = inputDBUsername
        outputDBPassword = inputDBPassword
    else:
        outputDBUsername = input("username =>")
        outputDBPassword = getpass()

    # establish database connection with credentials
    logger.info("Connecting...")
    outputDBconnection = mysql.connector.connect(
    host=outputDBHost,
    user=outputDBUsername,
    password=outputDBPassword,
    database=outputDatabaseName
    )
    logger.info("Connection Successful")

    outputDBCursor = outputDBconnection.cursor()
    insertFails = []
    for row in tqdm(outputTable,total=len(outputTable),desc="Writing output"):
        outputColumns = "("
        outputValues = "("
        first = True
        for columnName,columnValue in row.items():
            if columnValue == None: continue;
            if not first: 
                outputColumns += ',' + columnName
                outputValues += ',"' + str(columnValue) + '"'
                continue
            else:
                outputColumns += columnName
                outputValues += '"' + str(columnValue) + '"'
                first = False
        outputColumns += ")"
        outputValues += ")"

        try:
            outputDBCursor.execute("INSERT INTO " + outputDBTable + " " + outputColumns + " VALUES " + outputValues)
            
            outputDBconnection.commit()
        except mysql.connector.errors.IntegrityError:
            insertFails.append({"row": row, "error": "IntegrityError"})

    if len(insertFails) > 0:
        logger.error(str(len(insertFails)) + " rows couldn't be inserted")
                
@arguably.command
def setup(filename: str):
    '''
    Set the masking type for each field 

    Args:
        filename (str): The filename for the file to be created
    '''
    config = {}

    while True:
        fieldName = input("Field Name (q to end) =>")
        if fieldName == "q": break
        fieldConfig = setupFields.getMaskConfig()
        config[fieldName] = fieldConfig

    file = {"fields": config}
    jsonData = json.dumps(file)


    with open(filename,"w+") as f:
        f.write(jsonData)
@arguably.command
def clean(database: str):
    '''
    Remove all the records from the table 

    Args:
        database (str): The address of the input database. Format: host:database:table
    '''

    DBdata = database.split(":")
    try:
        DBHost = DBdata[0]
        DatabaseName = DBdata[1]
        DBTable = DBdata[2]
    except IndexError:
        logger.error("Input database malformed: " + database)
        return
    # get username and password input for db
    logger.info("Logging into " + DBHost)
    DBUsername = input("username =>")
    DBPassword = getpass()

    # establish database connection with credentials
    logger.info("Connecting...")
    DBconnection = mysql.connector.connect(
    host=DBHost,
    user=DBUsername,
    password=DBPassword,
    database=DatabaseName
    )
    logger.info("Connection Successful")

    DBCursor = DBconnection.cursor()
    logger.debug("Created input database cursor")

    logger.info("Loading data from table: " + DBTable)
    
    print("Are you sure you want to remove all records from " + DatabaseName + " - " + DBTable + "? Type YES to confirm.")
    confirmation = input()
    if confirmation == "YES":
        DBCursor.execute("REMOVE FROM "+DBTable+";")
        DBconnection.commit()




# def address(IN: list) -> dict:
#     url = "https://my.api.mockaroo.com/address.json"

#     payload = {}
#     headers = {
#     'X-API-Key': secrets["mockaroo"]
#     }

#     response = requests.request("GET", url, headers=headers, data=payload)
#     data = response.text.split(",")
#     addressData = {}
#     addressData["streetNumber"] = data[0].strip()
#     addressData["streetName"] = data[1].strip()
#     addressData["streetSuffix"] = data[2].strip()
#     addressData["street"] = addressData["streetName"] + " " + addressData["streetSuffix"]
#     addressData["streetAddress"] = addressData["streetNumber"] + " " + addressData["street"]
#     addressData["city"] = data[3].strip()
#     addressData["country"] = data[4].strip()
#     addressData["postcode"] = data[5].strip()

#     OUT = {}
#     for i in IN:
#         OUT[i] = addressData[i]
#     return OUT
    

if __name__ == "__main__":
    
    arguably.run()

