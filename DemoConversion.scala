    
//Registering hive tables into spark-shell
val dummy1 : DataFrame = spark.table("master_database.dummy_table1")
      .select("playlist_unique_Id","code_Id","supplier_id","available_date","Visible_Override_Ind_Episode","pass_details")

val dummy2 : DataFrame = spark.table("master_database.dummy_table2")

val dummy3 : DataFrame = spark.table("master_database.dummy_table3")

val dummy4 : DataFrame = spark.table("master_database.dummy_table4")

val dummy5 : DataFrame = spark.table("master_database.dummy_table5")


//Equivalent dataframe API to the Hive query but better performance

val outputDataFrame : DataFrame = dummy1.alias("src").join(dummy2.alias("psd"), Seq("country_details","Playlist_Id"), "inner")
      .join(dummy3.alias("ba"), Seq("brand_details"), "left_outer")
      .join(dummy4.alias("sf"), Seq("country_details"), "inner")
      .join(dummy5.alias("geo"),Seq("code"), "inner")
      .withColumn("completed_sign_dummy", when(dummy2("completed_sign").isNull, 0).otherwise(dummy2("completed_sign")))
      .withColumn("avail_dt", to_date(from_unixtime(unix_timestamp(dummy1("available_date")) + (dummy5("value")*3600),"yyyy-MM-dd HH:mm:ss")))
      .withColumn("Change_dt", to_date(from_unixtime(unix_timestamp(dummy2("Change_ts")) + (dummy5("value")*3600),"yyyy-MM-dd HH:mm:ss")))
      .where(s"Change_dt = '2018-04-01'")
      .where(dummy1("Season_Pass_Type_Id")=== 0 && $"completed_sign_dummy" === 1)
      .groupBy(dummy1("unique_Id"), dummy1("playlist_unique_Id"),dummy1("type_name"),dummy1("code_Id"),dummy1("Episode_Name"),dummy1("supplier_id")
        ,dummy1("supplier_name"),dummy1("available_date"),dummy1("Unavailable_date"),dummy1("Visible_Override_Ind_Episode"),dummy1("Cleared_For_Sale_Ind_Episode"),dummy1("pass_details")
        ,$"completed_sign_dummy",dummy4("shop_Id"),dummy4("shop_Name"),dummy1("code_Id"),dummy4("code")
        ,$"avail_dt", $"Change_dt", dummy3("Display_Name"))
      .agg(dummy1("unique_Id"), dummy1("playlist_unique_Id"),dummy1("type_name"),dummy1("code_Id"),dummy1("Episode_Name"),dummy1("supplier_id")
        ,dummy1("supplier_name"),dummy1("available_date"),dummy1("Unavailable_date"),dummy1("Visible_Override_Ind_Episode"),dummy1("Cleared_For_Sale_Ind_Episode"),dummy1("pass_details")
        ,$"completed_sign_dummy",dummy4("shop_Id"),dummy4("shop_Name"),dummy1("code_Id"),dummy4("code")
        ,$"avail_dt", $"Change_dt", dummy3("Display_Name"))
      .select(dummy1("unique_Id"), dummy1("playlist_unique_Id"),dummy1("type_name"),dummy1("code_Id"),dummy1("Episode_Name"),dummy1("supplier_id")
        ,dummy1("supplier_name"),dummy1("available_date"),dummy1("Unavailable_date"),dummy1("Visible_Override_Ind_Episode"),dummy1("Cleared_For_Sale_Ind_Episode"),dummy1("pass_details")
        ,$"completed_sign_dummy",dummy4("shop_Id"),dummy4("shop_Name"),dummy1("code_Id"),dummy4("code")
        ,$"avail_dt",$"Change_dt", dummy3("Display_Name"))
      .withColumn("hive_partition_id", lit(1))
      .withColumnRenamed("Display_Name","Season_Pass_Network_Name")
      .withColumnRenamed("completed_sign_dummy","completed_sign")


outputDataFrame.show(10)
      
      
      






