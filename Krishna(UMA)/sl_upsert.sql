-- TEMPLATE:
--     MERGE
--     INTO {Dst_Schema_nm}.{Dst_tbl_nm}
--     USING
--     (
--     {col_str1}
--     ) MERGE_SUBQUERY
--     ON
--     ({join_cond_str})
--     WHEN MATCHED AND {Dst_Schema_nm}.{Dst_tbl_nm}.checksum != MERGE_SUBQUERY.checksum
--     THEN
--     UPDATE SET {col_str4},LastLoadDate = current_timestamp()
--     WHEN NOT MATCHED THEN
--     INSERT
--     (
--     {col_str2},LastLoadDate
--     ) values (
--     {col_str3}, current_timestamp()
--     );


MERGE
    INTO kpi_cloud_sl.Hed_Contact
    USING
    (
    select 
      contactID
      , contactName
      , contactGender
      , contactEmail
      , contactPhone
      , LastLoadDate
      , DataSourceID
      , checksum 
      from source1_contact
          ) MERGE_SUBQUERY
    ON
    (kpi_cloud_sl.Hed_Contact.Hed_contactID_c = MERGE_SUBQUERY.contactID)
    WHEN MATCHED AND kpi_cloud_sl.Hed_Contact.checksum != MERGE_SUBQUERY.checksum
    THEN
      UPDATE SET kpi_cloud_sl.Hed_Contact.Hed_contactID_c=MERGE_SUBQUERY.contactID
      ,kpi_cloud_sl.Hed_Contact.Hed_Choosen_full_name_c=MERGE_SUBQUERY.contactName
      ,kpi_cloud_sl.Hed_Contact.Hed_contactGender_c=MERGE_SUBQUERY.contactGender
      ,kpi_cloud_sl.Hed_Contact.Hed_contactEmail_c=MERGE_SUBQUERY.contactEmail
      ,kpi_cloud_sl.Hed_Contact.Hed_contactPhone_c=MERGE_SUBQUERY.contactPhone
      ,kpi_cloud_sl.Hed_Contact.Hed_LastLoadDate_c=MERGE_SUBQUERY.LastLoadDate
      ,kpi_cloud_sl.Hed_Contact.DataSourceID=MERGE_SUBQUERY.DataSourceID
      ,kpi_cloud_sl.Hed_Contact.checksum=MERGE_SUBQUERY.checksum,LastLoadDate = current_timestamp()
    WHEN NOT MATCHED THEN
    INSERT
    (
      kpi_cloud_sl.Hed_Contact.Hed_contactID_c
      ,kpi_cloud_sl.Hed_Contact.Hed_Choosen_full_name_c
      ,kpi_cloud_sl.Hed_Contact.Hed_contactGender_c
      ,kpi_cloud_sl.Hed_Contact.Hed_contactEmail_c
      ,kpi_cloud_sl.Hed_Contact.Hed_contactPhone_c
      ,kpi_cloud_sl.Hed_Contact.Hed_LastLoadDate_c
      ,kpi_cloud_sl.Hed_Contact.DataSourceID
      ,kpi_cloud_sl.Hed_Contact.checksum,LastLoadDate
    ) values (
      MERGE_SUBQUERY.contactID
      ,MERGE_SUBQUERY.contactName
      ,MERGE_SUBQUERY.contactGender
      ,MERGE_SUBQUERY.contactEmail
      ,MERGE_SUBQUERY.contactPhone
      ,MERGE_SUBQUERY.LastLoadDate
      ,MERGE_SUBQUERY.DataSourceID
      ,MERGE_SUBQUERY.checksum, current_timestamp()
          );
