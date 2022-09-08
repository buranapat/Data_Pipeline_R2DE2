import pymysql
import pandas as pd
import requests

# Connect to the database
connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)

# List tables (check table in the database)
with connection.cursor() as cursor:
  cursor.execute("show tables;")
  tables = cursor.fetchall()
  print(tables)

# Query data using cursor and convert it to dataframe by pandas
with connection.cursor() as cursor:
  cursor.execute("SELECT * FROM audible_data;")
  result = cursor.fetchall()
audible_data = pd.DataFrame(result)

# Query data using pandas
sql = "SELECT * FROM audible_transaction"
audible_transaction = pd.read_sql(sql, connection)

# Join Table
transaction = audible_transaction.merge(audible_data, how="left", 
                                        left_on="book_id", right_on="Book_ID")

# Get data from REST API and convert it to dataframe by pandas
url = ""
r = requests.get(url) 
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)

# Create column index to make it easier to join
conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})

# Join table between transaction and conversion rate
transaction['date'] = transaction['timestamp'] # Create new column name "date" by copying column "timestamp" to join table
transaction['date'] = pd.to_datetime(transaction['date']).dt.date # Convert from "yyyy-MM-DD hh:mm:ss" to "yyyy-MM-DD"
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date
final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")

# Covert "String" to "Float" to convert currency form $ to ฿
final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1) # eleminate sting "$"
final_df["Price"] = final_df["Price"].astype(float)

# Create column "THBPrice"
final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]

#Drop Column "date" (We only use it to join table)
final_df = final_df.drop("date", axis=1)

#Export as CSV
final_df.to_csv("output.csv", index=False)
