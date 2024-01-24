This file provides more context on the directory structure and the project's workflow

- The main DAG file is called main_dag.py and it is located in the dags folder
    - The dags folder also contains two subfolders named 'queries' and 'functions'
        - The queries subfolder contains two queries
            - The query used to extract data from the clickhouse db [clickhouse_query.sql].
            - The query used to load data into the sqlite db [sql_insert_query.sql].
        - The functions subfolder contains one function named 'clickhouse_reader.py'
            - This function performs the tasks below
                - Connects to the clickhouse db
                - opens and reads the sql file.
                - uses the sql file to read data from the clickhouse db and stores it as a pandas dataframe.
                - performs minor transformation on the pandas dataframe.
                - generates an "INSERT QUERY" string from the dataframe by looping through the rows.
                - writes the generated INSERT QUERY string to an sql file in the queries folder called sql_insert_query.sql.
        

- The tasks performed by the DAG are as follows:
    - data extraction from the clickhouse db and generation of a sql_insert_query.sql file.
    - sql table creation.
    - sql truncation of existing table records.
    - loading the insert query into the dag file.
    - loading of data into the sqlite table using the sql_insert_query.sql file.


- Other considerations
    - Please ensure that you are in the airflow directory before running the webserver and the scheduler