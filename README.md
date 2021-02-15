# Sparkify Data project

The purpose of this project is to create a framework for Sparkify to move their processes and data to the cloud. Currently, Sparkify data reside in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In the project, an ETL pipeline will be designed to extracts data from S3, stage them in Redshift and transform them into a
set of dimensional tables for the analytics teams to continue finding insights in what songs their users are listening to. 

###### Project Files
sql_queries.py - This python script contains query statements for creating, inserting, droping and retrieving data rows from the songplay, user, song, artist, time and staging event tables that will be created. It also contain the scripts for copying data from the S3 folders into the staging tables. 

create_tables.py - This python script contains the procedures for creating database connection, creating tables, dropping tables and closing the database connection. All the procedures are called in the main function. 

dwh.cfg - This is configuration files that contains the custer properties that need to be populated. 

etl.py - This python script is used to load the whole datasets. It contains procedures for creating database connection, processing log files and closing the connection. 

###### Steps To Read and Load Data 
Spin up a cluster
Enter the cluster properties on the dwh.cfg file
Open the cmd terminal in the Launcher
Execute the create_tables python script to create the sparkifydb database, which other files links to.
Execute the etl process to create the tables, read data from the log files and load the data into newly created tables.
Validate that the data is loaded into the tables - songplay, user, song, artist, time. 
