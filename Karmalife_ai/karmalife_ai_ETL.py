"""
    Description : Reads data from CSV files and writes in parquet file by partitioning data
                    Coded simple ETL Job.

                    For creating a ETL pipeline, we can simply create a airflow DAG where we can run this job hourly or daily.
                    Running job daily will update the table daily by adding new date_key partition in tables.
                    Running job hourly will update the table hourly by adding new hourly partition. Here we can get live data hourly.
"""
#importing requeired libraries
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa

"""
Assuming we have 2 tables called de_events_op and user_level_info.
Currently in this code for extracting data I am using CSV file but in actual scenerio we can query this 2 tables and can create dataframe from query output.
Same as readng CSV file.
"""

class KarmaLife:
    def read_data(self):
        """
            Assuming we have 2 tables called de_events_op and user_level_info.
            Currently in this code for extracting data I am using CSV file but in actual scenerio we can query this 2 tables and can create dataframe from query output.
            Same as readng CSV file.
        """
        print("Reading table data..")
        # In ETL process, we extract data by writing SQL query or by reading a underlying file.
        # Query for hourly job : select * from db_name.de_events_op where event_date = '{event_date}' and hour = '{hour}'
        # Query for daily job : select * from db_name.de_events_op where event_date = '{event_date}'
        # We can change the DB and table name accordingly.
        self.de_events_op = pd.read_csv("de_events_op.csv", encoding= 'unicode_escape')
        self.user_level_info = pd.read_csv("user_level_info.csv", encoding= 'unicode_escape')

    def write_de_events_op_table(self):
        """
            Saving data written in dataframe into de_events_op table in datalake
        """
        print("Writing de_events_op data..")
        # Extracting hour from event_timestamp.
        self.de_events_op['hour'] = pd.to_datetime(self.de_events_op['event_timestamp']) .dt.strftime('%H')
        df = pd.DataFrame(self.de_events_op)
        table = pa.Table.from_pandas(df)
        # Writing data at location karmalife/de_events_op by partitioning with event_date and hour in parquet format
        pq.write_to_dataset(
            table,
            root_path='karmalife_ai_db/de_events_op',
            partition_cols=['event_date','hour'],
        )
        print("Table Updated")

    def write_user_level_info_table(self):
        """
            Saving data written in dataframe into user_level_info table in datalake
        """
        print("Writing user_level_info data..")
        # Extracting sign_up_date_key and hour from event_timestamp.
        self.user_level_info['sign_up_date_key'] = pd.to_datetime(self.user_level_info['signup']) .dt.strftime('%Y%m%d')
        self.user_level_info['hour'] = pd.to_datetime(self.user_level_info['signup']) .dt.strftime('%H')
        df = pd.DataFrame(self.user_level_info)
        table = pa.Table.from_pandas(df)
        # Writing data at location karmalife/user_level_info by partitioning with sign_up_date_key and hour in parquet format
        pq.write_to_dataset(
            table,
            root_path='karmalife_ai_db/user_level_info',
            partition_cols=['sign_up_date_key','hour'],
        )
        print("Table Updated")

    def complete(self):
        print("ETL Process completed.")
        print("Done :)")


glue_job = KarmaLife()
# Reading data from CSV or by running SQL query.
# Exctration of Data
glue_job.read_data()

# Transforming loaded data and writing final data at de_events_op table
glue_job.write_de_events_op_table()

# Transforming loaded data and writing final data at user_level_info table
glue_job.write_user_level_info_table()

# ETL Process completed.
glue_job.complete()



best_medium_for_partners_by_dates = """with best_medium_for_partners as (
    SELECT partner_id,utm_medium,   
    ROW_NUMBER() OVER(PARTITION BY partner_id order by unique_users_installed desc) AS row_num  
FROM de_events_op where event_date = '{event_date}'
)
select partner_id,utm_medium from best_medium_for_partners where row_num = 1"""

best_medium_for_partners_of_all_time = """with medium_counts_for_partners as (
    SELECT partner_id,utm_medium,sum(unique_users_installed) as count from de_events_op group by 1,2
    ),
    best_medium_for_partners as (
        select partner_id,utm_medium,
            ROW_NUMBER() OVER(PARTITION BY partner_id order by count desc) AS row_num 
        from medium_counts_for_partners
    )    
    ROW_NUMBER() OVER(PARTITION BY partner_id order by unique_users_installed desc) AS row_num  
FROM de_events_op where event_date = '{event_date}'
)
select partner_id,utm_medium from best_medium_for_partners where row_num = 1"""





discard_utm_campaign = """select utm_campaign, sum(unique_users_installed) from de_events_op group by 1 order by 2 asc limit 1"""



best_time_for_user_medium = """select utm_medium, hour, sum(unique_users_installed) from de_events_op group by 1,2 desc limit 1"""