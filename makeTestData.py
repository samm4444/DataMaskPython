import mysql.connector
from tqdm import tqdm
import os
import subprocess
import arguably
import math
from getpass import getpass

@arguably.command
def create(outputDB, records: int):
  '''
  Create some mock data

  Args:
    outputDB (str): The address for the output database. Format: host:database:table
    records (int): Number of records to create (will be rounded up to 5k)
  '''
  filename = "insert.sql"

  open(filename, 'w').close()

  outputDBdata = outputDB.split(":")
  try:
      outputDBHost = outputDBdata[0]
      outputDatabaseName = outputDBdata[1]
      outputDBTable = outputDBdata[2]
  except IndexError:
      return
  # get username and password input for db
  outputDBUsername = input("Username:")
  outputDBPassword = getpass()

  connection = mysql.connector.connect(
      host=outputDBHost,
      user=outputDBUsername,
      password=outputDBPassword,
      database=outputDatabaseName
  )
  
  n = math.ceil((records / 5000))
  for _ in tqdm(range(0,n),desc="Downloading"):
    os.system('curl -s "https://api.mockaroo.com/api/46f0e100?count=5000&key=b886c2d0" >> "'+filename+'"')


  cursor = connection.cursor()

  print("Inserting into database")
  with open(filename) as f:
    for line in tqdm(f.readlines(),desc="Inserting"):
      cursor.execute(line)

  connection.commit()
  connection.close()


if __name__ == "__main__":
    arguably.run()