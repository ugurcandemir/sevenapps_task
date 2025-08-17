import pandas as pd

# In this Python script we do the first step of the task. We generate a dummy dataset, save it to the environment
# and upload to the locally deployed PostGRESQL server. We do it so that we can practice SQL and find the conversion rates
# for our funnel analysis.


# We begin by importing the necessary libraries. 
import random
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import sql

# We initiate a Faker object and create our fake data. We pay special attetion to the relationship between users and events.
# We do it in a way so that some users have only the PageView and some only PageView and Download , and goes on like that.
# So we simulate a realistic user journey data of 10.000 events.

fake = Faker()

num_events = 10000 
start_date = datetime.now() - timedelta(days=180)

event_funnel = ["PageView", "Download", "Install", "Purchase"]

funnel_probs = [0.5, 0.2, 0.2, 0.1]  

platforms = ["ios", "android"]
device_types = ["phone", "tablet"]

data = []
event_count = 0

while event_count < num_events:
    user_id = fake.uuid4()
    platform = random.choice(platforms)
    device_type = random.choice(device_types)
    
    max_stage = random.choices([1, 2, 3, 4], weights=funnel_probs, k=1)[0]
    
    base_time = start_date + timedelta(seconds=random.randint(0, 180*24*60*60))
    
    for stage in range(max_stage):
        if event_count >= num_events:
            break  
        
        event_time = base_time + timedelta(minutes=stage * random.randint(5, 60))
        
        row = {
            "event_id": fake.uuid4(),
            "user_id": user_id,
            "event_name": event_funnel[stage],
            "platform": platform,
            "device_type": device_type,
            "timestamp": event_time
        }
        
        data.append(row)
        event_count += 1

df = pd.DataFrame(data)

# We write down our generated data.
df.to_csv("user_events.csv", index=False)
print(f"Dataset generated: user_events.csv with {len(df)} rows")


# Now we are creating a database , a table and upload the data to taht table.
db_url = "postgresql://postgres:mysecret@localhost:5432"

def create_database_if_not_exists(dbname):
    try:
        conn = psycopg2.connect(db_url + "/postgres")
        conn.autocommit = True
        cur = conn.cursor()

        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            print(f"Database '{dbname}' created.")
        else:
            print(f"Database '{dbname}' already exists.")

        cur.close()
        conn.close()

    except Exception as e:
        print("Error creating database:", e)


def create_table_if_not_exists(dbname, tablename, columns: dict):

    try:
        conn = psycopg2.connect(db_url + f"/{dbname}")
        cur = conn.cursor()

        # We drop table if it exists
        drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
            sql.Identifier(tablename)
        )
        cur.execute(drop_query)

        # We build CREATE TABLE statement dynamically
        column_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        create_query = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(tablename),
            sql.SQL(column_defs)
        )

        cur.execute(create_query)
        conn.commit()
        print(f"Table '{tablename}' dropped (if existed) and recreated with schema: {columns}")

        cur.close()
        conn.close()

    except Exception as e:
        print("Error creating table:", e)

dbname = "sevenapps"
tablename = "user_events"

# Giving column names and data types is a good practice in general.
schema = {
    "event_id": "TEXT PRIMARY KEY",
    "user_id": "TEXT",
    "event_name": "TEXT",
    "platform": "TEXT",
    "device_type": "TEXT",
    "timestamp": "TIMESTAMP"
}

create_database_if_not_exists(dbname)
create_table_if_not_exists(dbname, tablename, schema)

db_url2  = db_url + "/sevenapps" 
engine = create_engine(db_url2)

# We Upload DataFrame to user_events table
df.to_sql(
    tablename, 
    engine,            
    if_exists="append", 
    index=False        
)

print(f"Data uploaded successfully to '{tablename}'")