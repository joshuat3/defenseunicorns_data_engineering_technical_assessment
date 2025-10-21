import pandas as pd
import json
from logs import setup_logging, get_logger
from comms import test_connections, check_schema, get_sqlalchemy_engine
from dim_builder import db_cleaner, load_components, load_orders, load_parts, load_users, build_analytics_views, check_dimensions_on_orders

log = get_logger()

DATA_PATH = "../data"
TABLES = ['raw_batch_data','raw_streaming_data']

def get_batch_data() -> pd.DataFrame:
   """Returns a pandas dataframe for the batch orders parquet"""
   return pd.read_parquet(f"{DATA_PATH}/batch_orders.parquet")

def get_streaming_json() -> list:
   """Returns a list of dictionaries of the streaming messages"""
   with open(f"{DATA_PATH}/streaming_orders.json", "r") as fh:
      messages = json.load(fh)
   return messages

def ingest_summary_batch_data():
   df = get_batch_data()
   log.info(f'Batch data head:\n{df.head()}')
   log.info(f'Batch data info:\n{df.info()}')
   messages = get_streaming_json()

def ingest_batch_data():
    df = get_batch_data()
    log.info(f'Loading '+str(len(df))+ ' batch messages')
    raw_data_loader(df, TABLES[0])
    log.info(f'Batch data loaded in: ' + TABLES[0])

def ingest_streaming_data():
   messages = get_streaming_json()
   log.info(f'Loading '+str(len(messages))+ ' streaming messages')

   for record in messages:
      if 'details'not in record:
         record['details'] = [{}]
      elif isinstance(record['details'],dict):
         record['details'] = [record['details']]
   log.info(f'Json payload structure standardized')
   messages = pd.json_normalize(
      messages,
      record_path= 'details',
      meta = ['order_uuid','datetime','status']
   )
   log.info(f'Json payloads flatten')
   
   raw_data_loader(messages, TABLES[1])
   log.info(f'Streaming messaged loaded in: ' + TABLES[1])

def raw_data_loader (data, table):
   data.to_sql(
      name = table,
      con = get_sqlalchemy_engine(),
      if_exists = 'replace',
      index = False
   )

if __name__ == "__main__":
   setup_logging()
   test_connections()
   check_schema()
   #ingest_summary_batch_data()
   ingest_batch_data()
   ingest_streaming_data()
   db_cleaner()
   load_components()
   load_parts()
   load_users()
   load_orders()
   build_analytics_views()
   check_dimensions_on_orders()