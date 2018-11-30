SELECT
src.Season_Pass_Adam_Id,
src.Playlist_Adam_Id,
src.Season_Pass_Type_Name,
src.Avail_Ts,
src.Unavail_Ts,
(CASE WHEN psd.Tv_Season_Completed_Ind IS NULL THEN Cast(0 As BIGINT) ELSE psd.Tv_Season_Completed_Ind END) Tv_Season_Completed_Ind,
src.Season_Pass_Name,
src.Episode_Name,
sf.Store_Front_Id,
sf.Store_Front_Name,
sf.Geo_Cd,
src.Episode_Adam_Id,
To_Date((from_unixtime((unix_timestamp(src.Avail_Ts) + geo.System_Offset_Val*3600),'yyyy-MM-dd HH:mm:ss'))) Avail_Date_Episode,
To_Date((from_unixtime((unix_timestamp(psd.Change_ts) + geo.System_Offset_Val*3600),'yyyy-MM-dd HH:mm:ss'))) Change_dt,
src.Season_Pass_Provider_Name,
ba.Display_Name Season_Pass_Network_Name,
src.content_Provider_Id,
1067998209 As etl_process_run_id
FROM seasonPassExp3VW src
INNER JOIN its_playlist_season_dtl_mio_svw psd ON (psd.Its_Country_Id = src.Its_Country_Id and src.Playlist_Id=psd.Playlist_Id)
LEFT OUTER JOIN its_branding_company_svw ba on(psd.Branding_Company_Id = ba.Branding_Company_Id)
INNER JOIN its_store_front_ref_svw sf ON(sf.Its_Country_Id = psd.Its_Country_Id)
INNER JOIN its_geo_code_svw geo ON (sf.Geo_Cd= geo.Geo_Cd )
WHERE Tv_Season_Completed_Ind=1 AND
To_Date((from_unixtime((unix_timestamp(psd.Change_ts) + geo.System_Offset_Val*3600),'yyyy-MM-dd HH:mm:ss'))) = date_sub(to_date("2018-07-20 00:00:00"),1)
and src.Season_Pass_Type_Id =0
Group By src.Season_Pass_Adam_Id,
src.Playlist_Adam_Id,
src.Season_Pass_Type_Name,
src.Avail_Ts,
src.Unavail_Ts,
(CASE WHEN psd.Tv_Season_Completed_Ind IS NULL THEN Cast(0 As BIGINT) ELSE psd.Tv_Season_Completed_Ind END),
src.Season_Pass_Name,
src.Episode_Name,
sf.Store_Front_Id,
sf.Store_Front_Name,
sf.Geo_Cd,
src.Episode_Adam_Id,
To_Date((from_unixtime((unix_timestamp(src.Avail_Ts) + geo.System_Offset_Val*3600),'yyyy-MM-dd HH:mm:ss'))),
To_Date((from_unixtime((unix_timestamp(psd.Change_ts) + geo.System_Offset_Val*3600),'yyyy-MM-dd HH:mm:ss'))),
src.Season_Pass_Provider_Name,
ba.Display_Name,
src.content_Provider_Id
