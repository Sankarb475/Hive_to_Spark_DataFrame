    
//Registering hive tables into spark-shell
val dummy1 : DataFrame = spark.table("master_database.dummy_table1")
      .select("Playlist_Id", "lpr_id", "fpr_id", "Adam_Id","Season_Pass_Id")

val dummy2 : DataFrame = spark.table("master_database.dummy_table2")
      .select("Playlist_Id", "lpr_id", "fpr_id", "Adam_Id","Season_Pass_Id")

val dummy3 : DataFrame = spark.table("master_database.dummy_table3")
      .select("dummyColumn1", "lpr_id", "fpr_id", "Adam_Id","Season_Pass_Id")

val dummy4 : DataFrame = spark.table("master_database.dummy_table4")
      .select("Playlist_Id", "lpr_id", "fpr_id", "Adam_Id","Season_Pass_Id")

val dummy5 : DataFrame = spark.table("master_database.dummy_table5")
      .select("Playlist_Id", "lpr_id", "fpr_id", "Adam_Id","Season_Pass_Id")

val hiveDataFrame : DataFrame = dummy1.alias("src").join(dummy2.alias("psd"), Seq("Its_Country_Id","Playlist_Id"), "inner")
      .join(dummy3.alias("ba"), Seq("Branding_Company_Id"), "left_outer")
      .join(dummy4.alias("sf"), Seq("Its_Country_Id"), "inner")
      .join(dummy5.alias("geo"),Seq("Geo_Cd"), "inner")
      .withColumn("Tv_Season_Completed_Ind_dummy", when(dummy2("Tv_Season_Completed_Ind").isNull, 0).otherwise(dummy2("Tv_Season_Completed_Ind")))
      .withColumn("avail_dt", to_date(from_unixtime(unix_timestamp(dummy1("Avail_Ts")) + (dummy5("System_Offset_Val")*3600),"yyyy-MM-dd HH:mm:ss")))
      .withColumn("Change_dt", to_date(from_unixtime(unix_timestamp(dummy2("Change_ts")) + (dummy5("System_Offset_Val")*3600),"yyyy-MM-dd HH:mm:ss")))
      .where(s"Change_dt = '2018-04-01'")
      .where(dummy1("Season_Pass_Type_Id")=== 0 && $"Tv_Season_Completed_Ind_dummy" === 1)
      .groupBy(dummy1("Season_Pass_Adam_Id"), dummy1("Playlist_Adam_Id"),dummy1("Season_Pass_Type_Name"),dummy1("Episode_Adam_Id"),dummy1("Episode_Name"),dummy1("content_Provider_Id")
        ,dummy1("Season_Pass_Provider_Name"),dummy1("Avail_Ts"),dummy1("Unavail_Ts"),dummy1("Visible_Override_Ind_Episode"),dummy1("Cleared_For_Sale_Ind_Episode"),dummy1("Season_Pass_Name")
        ,$"Tv_Season_Completed_Ind_dummy",dummy4("Store_Front_Id"),dummy4("Store_Front_Name"),dummy1("Episode_Adam_Id"),dummy4("Geo_Cd")
        ,$"avail_dt", $"Change_dt", dummy3("Display_Name"))
      .agg(dummy1("Season_Pass_Adam_Id"), dummy1("Playlist_Adam_Id"),dummy1("Season_Pass_Type_Name"),dummy1("Episode_Adam_Id"),dummy1("Episode_Name"),dummy1("content_Provider_Id")
        ,dummy1("Season_Pass_Provider_Name"),dummy1("Avail_Ts"),dummy1("Unavail_Ts"),dummy1("Visible_Override_Ind_Episode"),dummy1("Cleared_For_Sale_Ind_Episode"),dummy1("Season_Pass_Name")
        ,$"Tv_Season_Completed_Ind_dummy",dummy4("Store_Front_Id"),dummy4("Store_Front_Name"),dummy1("Episode_Adam_Id"),dummy4("Geo_Cd")
        ,$"avail_dt", $"Change_dt", dummy3("Display_Name"))
      .select(dummy1("Season_Pass_Adam_Id"), dummy1("Playlist_Adam_Id"),dummy1("Season_Pass_Type_Name"),dummy1("Episode_Adam_Id"),dummy1("Episode_Name"),dummy1("content_Provider_Id")
        ,dummy1("Season_Pass_Provider_Name"),dummy1("Avail_Ts"),dummy1("Unavail_Ts"),dummy1("Visible_Override_Ind_Episode"),dummy1("Cleared_For_Sale_Ind_Episode"),dummy1("Season_Pass_Name")
        ,$"Tv_Season_Completed_Ind_dummy",dummy4("Store_Front_Id"),dummy4("Store_Front_Name"),dummy1("Episode_Adam_Id"),dummy4("Geo_Cd")
        ,$"avail_dt",$"Change_dt", dummy3("Display_Name"))
      .withColumn("etl_process_run_id", lit(1))
      .withColumnRenamed("Display_Name","Season_Pass_Network_Name")
      .withColumnRenamed("Tv_Season_Completed_Ind_dummy","Tv_Season_Completed_Ind") 




