from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

MYSQL_CONNECTION = ""   # named of connection that set on Airflow
CONVERSION_RATE_URL = ""

# Output path (store in google cloud storage)
mysql_output_path = ""
conversion_rate_output_path = ""
final_output_path = ""


def get_data_from_mysql(transaction_path):
    
    #Connect to Database
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query from database like workshop1 (Data Collection)
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * \
                                                     FROM audible_transaction")
    

    
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", 
                                   right_on="Book_ID")

    
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


def get_conversion_rate(conversion_rate_path):
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)

    # Get data from REST API like workshop1 (Data Collection)
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(conversion_rate_path, index=False)
    print(f"Output to {conversion_rate_path}")


def merge_data(transaction_path, conversion_rate_path, output_path):
    # join table like workshop1 (Data Collection)
    transaction = pd.read_csv(transaction_path)
    conversion_rate = pd.read_csv(conversion_rate_path)

    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

  
    final_df = transaction.merge(conversion_rate, how="left", 
                                 left_on="date", right_on="date")
    
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)

    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop(["date", "book_id"], axis=1)

    
    final_df.to_csv(output_path, index=False)
    print(f"Output to {output_path}")
    print("== End of Workshop 4 ʕ•́ᴥ•̀ʔっ♡ ==")

# Set Airflow
with DAG(
    "exercise4_final_dag",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["workshop"]
) as dag:
    
    dag.doc_md = """ write description of this DAG """
    
    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql, # use function "get_data_from_mysql"
        op_kwargs={
            "transaction_path": mysql_output_path, #input of function
        },
    )

    t2 = PythonOperator(
        task_id="get_conversion_rate",
        python_callable=get_conversion_rate, # use function "get_conversion_rate"
        op_kwargs={
            "conversion_rate_path": conversion_rate_output_path, #input of function
        },
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,  # use function "merge_data"
        op_kwargs={ #input of function
            "transaction_path": mysql_output_path,
            "conversion_rate_path": conversion_rate_output_path,
            "output_path" : final_output_path,
        },
    )
    # Load data into data warehouse (BigQuery)
    t4 = BashOperator(
        task_id="load_to_bq",
        bash_command="bq load \
                     --source_format=CSV \
                     --autodetect \
                     DataSet_name.Table_name \
                     gs://bucket_name/data/file_name.csv"
    )
    
    [t1,t2] >> t3 >> t4 # Set up Dependencies 

    