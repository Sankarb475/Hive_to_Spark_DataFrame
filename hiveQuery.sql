SELECT
src.unique_Id,
src.playlist_unique_Id,
src.type_name,
src.available_date,
src.Unavailable_date,
(CASE WHEN psd.completed_sign IS NULL THEN Cast(0 As BIGINT) ELSE psd.completed_sign END) completed_sign,
src.pass_details,
src.Episode_Name,
sf.shop_Id,
sf.shop_Name,
sf.code,
src.code_Id,
To_Date((from_unixtime((unix_timestamp(src.available_date) + geo.value*3600),'yyyy-MM-dd HH:mm:ss'))) some_date,
To_Date((from_unixtime((unix_timestamp(psd.Change_ts) + geo.value*3600),'yyyy-MM-dd HH:mm:ss'))) Change_dt,
src.supplier_name,
ba.Display_Name Season_Pass_Network_Name,
src.supplier_id,
1067998209 As hive_partition_id
FROM seasonPassExp3VW src
INNER JOIN its_playlist_season_dtl_mio_svw psd ON (psd.country_details = src.country_details and src.Playlist_Id=psd.Playlist_Id)
LEFT OUTER JOIN its_brand_details_svw ba on(psd.brand_details = ba.brand_details)
INNER JOIN its_store_front_ref_svw sf ON(sf.country_details = psd.country_details)
INNER JOIN its_geo_code_svw geo ON (sf.code= geo.code )
WHERE completed_sign=1 AND
To_Date((from_unixtime((unix_timestamp(psd.Change_ts) + geo.value*3600),'yyyy-MM-dd HH:mm:ss'))) = date_sub(to_date("2018-07-20 00:00:00"),1)
and src.Season_Pass_Type_Id =0
Group By src.unique_Id,
src.playlist_unique_Id,
src.type_name,
src.available_date,
src.Unavailable_date,
(CASE WHEN psd.completed_sign IS NULL THEN Cast(0 As BIGINT) ELSE psd.completed_sign END),
src.pass_details,
src.Episode_Name,
sf.shop_Id,
sf.shop_Name,
sf.code,
src.code_Id,
To_Date((from_unixtime((unix_timestamp(src.available_date) + geo.value*3600),'yyyy-MM-dd HH:mm:ss'))),
To_Date((from_unixtime((unix_timestamp(psd.Change_ts) + geo.value*3600),'yyyy-MM-dd HH:mm:ss'))),
src.supplier_name,
ba.Display_Name,
src.supplier_id
