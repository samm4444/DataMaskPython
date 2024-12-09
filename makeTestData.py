import mysql.connector
from tqdm import tqdm
import os
import subprocess

filename = "insert.sql"

open(filename, 'w').close()

connection = mysql.connector.connect(
    host="192.168.1.115",
    user="hackcamp",
    password="Password1",
    database="HackCampSensitive"
    )

n = 15 # 5000 * n records will be made
for _ in tqdm(range(0,n),desc="Downloading"):
  # subprocess.call('curl "https://api.mockaroo.com/api/46f0e100?count=5000&key=b886c2d0" >> "'+filename+'"')
  os.system('curl -s "https://api.mockaroo.com/api/46f0e100?count=5000&key=b886c2d0" >> "'+filename+'"')


cursor = connection.cursor()

print("Inserting into database")
with open(filename) as f:
  for line in tqdm(f.readlines(),desc="Inserting"):
    cursor.execute(line)

connection.commit()
connection.close()