val DataframeDummy = wrkSeasonPassExp3Data.alias("src").join(itsPlaylistSeasonDtlData.alias("psd"), Seq("Its_Country_Id","Playlist_Id"), "inner")
      .join(itsBrandingCompanyData.alias("ba"), Seq("Branding_Company_Id"), "left_outer")
      .join(itsStoreFrontRefData.alias("sf"), Seq("Its_Country_Id"), "inner")
      .join(itsGeoCodeData.alias("geo"),Seq("Geo_Cd"), "inner")
      .withColumn("Tv_Season_Completed_Ind", when(itsPlaylistSeasonDtlData("Tv_Season_Completed_Ind").isNull, 0.asInstanceOf[BigInt]).otherwise(itsPlaylistSeasonDtlData("Tv_Season_Completed_Ind")))
      .withColumn("avail_date", to_date(from_unixtime(unix_timestamp($"src.Avail_Ts") + ($"geo.System_Offset_Val"*3600),"yyyy-MM-dd HH:mm:ss")))
      .where(s"avail_date>'2018-03-31' AND avail_date<'2018-04-01' ")
      .where($"src.Visible_Override_Ind_Episode"===1 && $"src.Cleared_For_Sale_Ind_Episode"===1 && $"src.Season_Pass_Type_Id" ===2)
      .groupBy($"src.Season_Pass_Adam_Id",$"src.Season_Pass_Type_Name",$"src.Playlist_Adam_Id",$"src.Season_Pass_Name",$"src.Episode_Adam_Id",$"src.Episode_Name",$"src.content_Provider_Id"
        ,$"src.Season_Pass_Provider_Name",$"src.Avail_Ts",$"src.Unavail_Ts",$"src.Visible_Override_Ind_Episode",$"src.Cleared_For_Sale_Ind_Episode"
        ,$"Tv_Season_Completed_Ind",$"sf.Store_Front_Id",$"sf.Store_Front_Name",$"sf.Geo_Cd",$"avail_date",$"ba.Display_Name")
      .agg($"src.Season_Pass_Adam_Id",$"src.Season_Pass_Type_Name",$"src.Playlist_Adam_Id",$"src.Season_Pass_Name",$"src.Episode_Adam_Id",$"src.Episode_Name",$"src.content_Provider_Id"
        ,$"src.Season_Pass_Provider_Name",$"src.Avail_Ts",$"src.Unavail_Ts",$"src.Visible_Override_Ind_Episode",$"src.Cleared_For_Sale_Ind_Episode"
        ,$"Tv_Season_Completed_Ind",$"sf.Store_Front_Id",$"sf.Store_Front_Name",$"sf.Geo_Cd",$"avail_date",$"ba.Display_Name")
      .select($"src.Season_Pass_Adam_Id",$"src.Season_Pass_Type_Name",$"src.Playlist_Adam_Id",$"src.Season_Pass_Name",$"src.Episode_Adam_Id",$"src.Episode_Name",$"src.content_Provider_Id"
        ,$"src.Season_Pass_Provider_Name",$"src.Avail_Ts",$"src.Unavail_Ts",$"src.Visible_Override_Ind_Episode",$"src.Cleared_For_Sale_Ind_Episode"
        ,$"Tv_Season_Completed_Ind",$"sf.Store_Front_Id",$"sf.Store_Front_Name",$"sf.Geo_Cd",$"ba.Display_Name",$"avail_date")
        .withColumnRenamed("Display_Name","Season_Pass_Network_Name")
      .withColumn("etl_process_run_id", lit(1))
