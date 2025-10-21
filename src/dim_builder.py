import pandas as pd
from logs import setup_logging, get_logger
from comms import test_connections, check_schema, get_sqlalchemy_engine
from sqlalchemy.sql import text, select

log = get_logger()


def db_cleaner():
   """Truncates dimension tables"""
   log.info(f"Running postgres/db_cleaner.sql")
   eng = get_sqlalchemy_engine()
   with open("../postgres/db_cleaner.sql", "r") as fh:
      sql = text(fh.read())
   with eng.connect() as con:
      con.execute(sql)
      con.commit()
   log.info(f"Completed postgres/db_cleaner.sql")    

def load_components():
   log.info(f"Running postgres/load_components.sql")
   eng = get_sqlalchemy_engine()
   with open("../postgres/load_components.sql", "r") as fh:
      sql = text(fh.read())
   with eng.connect() as con:
      con.execute(sql)
      con.commit() 
   sql = "select count(*) record_cnt from components"
   records = pd.read_sql(sql, eng)
   log.info(f"Loaded: " +str(records['record_cnt'][0])+ " records with postgres/load_components.sql")

def load_parts():
   log.info(f"Running postgres/load_parts.sql")
   eng = get_sqlalchemy_engine()
   with open("../postgres/load_parts.sql", "r") as fh:
      sql = text(fh.read())
   with eng.connect() as con:
      con.execute(sql)
      con.commit() 
   sql = "select count(*) record_cnt from parts"
   records = pd.read_sql(sql, eng)
   log.info(f"Loaded: " +str(records['record_cnt'][0])+ " records with postgres/load_parts.sql")

def load_users():
   log.info(f"Running postgres/load_users.sql")
   eng = get_sqlalchemy_engine()
   with open("../postgres/load_users.sql", "r") as fh:
      sql = text(fh.read())
   with eng.connect() as con:
      con.execute(sql)
      con.commit() 
   sql = "select count(*) record_cnt from users"
   records = pd.read_sql(sql, eng)
   log.info(f"Loaded: " +str(records['record_cnt'][0])+ " records with postgres/load_users.sql")

def load_orders():
   log.info(f"Running postgres/load_orders.sql")
   eng = get_sqlalchemy_engine()
   with open("../postgres/load_orders.sql", "r") as fh:
      sql = text(fh.read())
   with eng.connect() as con:
      con.execute(sql)
      con.commit() 
   sql = "select count(*) record_cnt from orders"
   records = pd.read_sql(sql, eng)
   log.info(f"Loaded: " +str(records['record_cnt'][0])+ " records with postgres/load_orders.sql")

def build_analytics_views():
   log.info(f"Running postgres/analytics.sql")
   eng = get_sqlalchemy_engine()
   with open("../postgres/analytics.sql", "r") as fh:
      sql = text(fh.read())
   with eng.connect() as con:
      con.execute(sql)
      con.commit() 
   sql = "select count(*) record_cnt from legacy_data"
   records = pd.read_sql(sql, eng)
   log.info(f"View legacy_data is available with: " +str(records['record_cnt'][0])+ " records.")

def check_dimensions_on_orders():
   log.info(f"Checking dimensions on orders")

   eng = get_sqlalchemy_engine()
   sql = "select supplier_uuid from orders where order_date is null"
   records = pd.read_sql(sql, eng)
   log.info(f"Orders missing the status ORDERED for ordered_date: " +str(records['supplier_uuid'][:]))

   sql = "select o.part_id from orders o left join parts p on o.part_id = p.part_id where p.part_id is null"
   records = pd.read_sql(sql, eng)
   if len(records) == 0:
      log.info(f"All part numbers on orders resolve")
   else:
      log.info(f"Parts on orders that are missing from the parts table: " +str(records['part_id'][:]))

   sql = "select o.component_id from orders o left join components c on o.component_id = c.component_id where c.component_id is null"
   records = pd.read_sql(sql, eng)
   if len(records) == 0:
      log.info(f"All components on orders resolve")
   else:
      log.info(f"Components on orders that are missing from the components table: " +str(records['component_id'][:]))     

   sql = "select o.ordered_by from orders o left join users u on o.ordered_by = u.user_id where u.user_id is null"
   records = pd.read_sql(sql, eng)
   if len(records) == 0:
      log.info(f"All users on orders resolve")
   else:
      log.info(f"Users on orders that are missing from the users table: " +str(records['order_by'][:]))  