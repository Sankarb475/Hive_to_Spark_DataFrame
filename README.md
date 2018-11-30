# Hive_to_Spark_DataFrame
Hive query conversion to Spark DataFrame API using Scala

You can run your Hive queries in spark-shell with some easy steps, which will drastically reduce the execution time. 
I will show you one Hive query which generally takes at least 20 minutes to execute in Hive cli which when converted 
into spark dataframe API takes around 3 minutes to run.

Steps ::
1) Registering the Hive table into dataframe
2) converting the Hive query into equivalent scala code
3) run the code into spark-shell

-- In case you have Nested selects in your Hive query, the easiest way to convert it into equivalent scala is to have each 
nested select query first registered as dataframe, and then use those dataframes to reach to the final output dataframe.
