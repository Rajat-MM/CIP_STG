/* Truncate Stage Table edw_staging.cip_sfmc_send_log_bounces */
Truncate table edw_staging.cip_sfmc_send_log_bounces;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_bounces from Source Table ext_edap_staging.Bounces */
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_bounces(
			clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			bouncecategory,
			smtpcode,
			bouncereason,
			batchid,
			triggeredsendexternalkey,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.clientid,
			SRC.sendid,
			SRC.subscriberkey,
			SRC.emailaddress,
			SRC.subscriberid,
			SRC.listid,
			SRC.eventdate,
			SRC.eventtype,
			SRC.bouncecategory,
			SRC.smtpcode,
			SRC.bouncereason,
			SRC.batchid,
			SRC.triggeredsendexternalkey,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM
			(SELECT *,ROW_NUMBER() OVER(PARTITION BY listid, batchid, subscriberid, eventdate ORDER BY eventdate) as row_num 
			from EDW_CIP.cip_sfmc_send_log_bounces) SRC
	LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
		ADT.record_aligner = 1
		and ADT.batch_name = 'SFMC_Send_Log_to_CIP'
	WHERE SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_bounces_extension */
Truncate table edw_staging.cip_sfmc_send_log_bounces_extension;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_bounces_extension from Source Table ext_edap_staging.Bounces_Extension_Extract */
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_bounces_extension(
			accountid,
			oybaccountid,
			jobid,
			listid,
			batchid,
			subscriberid,
			subscriberkey,
			eventdate,
			isunique,
			domain,
			bouncecategoryid,
			bouncecategory,
			bouncesubcategoryid,
			bouncesubcategory,
			bouncetypeid,
			bouncetype,
			smtpbouncereason,
			smtpmessage,
			smtpcode,
			triggerersenddefinitionobjectid,
			triggeredsendcustomerkey,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.accountid,
			SRC.oybaccountid,
			SRC.jobid,
			SRC.listid,
			SRC.batchid,
			SRC.subscriberid,
			SRC.subscriberkey,
			SRC.eventdate,
			SRC.isunique,
			SRC.domain,
			SRC.bouncecategoryid,
			SRC.bouncecategory,
			SRC.bouncesubcategoryid,
			SRC.bouncesubcategory,
			SRC.bouncetypeid,
			SRC.bouncetype,
			SRC.smtpbouncereason,
			SRC.smtpmessage,
			SRC.smtpcode,
			SRC.triggerersenddefinitionobjectid,
			SRC.triggeredsendcustomerkey,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM
			(SELECT *,ROW_NUMBER() OVER(PARTITION BY listid, batchid, subscriberid, eventdate ORDER BY eventdate) as row_num 
			from EDW_CIP.cip_sfmc_send_log_bounces_extension )SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT
		ON ADT.record_aligner = 1
		and ADT.batch_name = 'SFMC_Send_Log_to_CIP'
		WHERE SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_campaign */
Truncate table edw_staging.cip_sfmc_send_log_campaign;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_campaign from Source Table ext_edap_staging.MM_Campaign_Table_Extract */
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_campaign(
			campaign_name,
			purpose,
			cta,
			product,
			line_of_business,
			hierarchy_1,
			hierarchy_2,
			hierarchy_3,
			hierarchy_4,
			medium,
			campaign_id,
			date_added,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.campaign_name,
			SRC.purpose,
			SRC.cta,
			SRC.product,
			SRC.line_of_business,
			SRC.hierarchy_1,
			SRC.hierarchy_2,
			SRC.hierarchy_3,
			SRC.hierarchy_4,
			SRC.medium,
			SRC.campaign_id,
			SRC.date_added,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM ( SELECT *, ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY date_added) as row_num 
			from EDW_CIP.cip_sfmc_send_log_campaign) SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			AND ADT.batch_name = 'SFMC_Send_Log_to_CIP'
			WHERE SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate the satging table */
TRUNCATE TABLE edw_staging.cip_sfmc_send_log_clicks;

/* Insert the edw_cip data into Staging table */
INSERT INTO edw_staging.cip_sfmc_send_log_clicks
(clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			sendurlid,
			urlid,
			url,
			alias,
			batchid,
			triggeredsendexternalkey,
			isunique,
			isuniqueforurl,
			ipaddress,
			country,
			region,
			city,
			latitude,
			longitude,
			metrocode,
			areacode,
			browser,
			emailclient,
			operatingsystem,
			device,
			row_process_dtm,
			audit_id,
			source_system_id)
SELECT
			SRC.clientid,
			SRC.sendid,
			SRC.subscriberkey,
			SRC.emailaddress,
			SRC.subscriberid,
			SRC.listid,
			SRC.eventdate,
			SRC.eventtype,
			SRC.sendurlid,
			SRC.urlid,
			SRC.url,
			SRC.alias,
			SRC.batchid,
			SRC.triggeredsendexternalkey,
			SRC.isunique,
			SRC.isuniqueforurl,
			SRC.ipaddress,
			SRC.country,
			SRC.region,
			SRC.city,
			SRC.latitude,
			SRC.longitude,
			SRC.metrocode,
			SRC.areacode,
			SRC.browser,
			SRC.emailclient,
			SRC.operatingsystem,
			SRC.device,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY sendid,listid,batchid,subscriberid,sendurlid,url,urlid,ipaddress,eventdate ORDER BY eventdate) as row_num 
			 FROM edw_cip.cip_sfmc_send_log_clicks) SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			and ADT.batch_name = 'SFMC_Send_Log_to_CIP'
		WHERE SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/*Truncate the staging table*/
truncate table edw_staging.cip_sfmc_send_log_clicks_extension;

/*Insert Values in to Stage Table edw_cip.cip_sfmc_send_log_clicks_extension from Source Table ext_edap_staging.Clicks_Extension_Extract */
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_clicks_extension(
			accountid,
			oybaccountid,
			jobid,
			listid,
			batchid,
			subscriberid,
			subscriberkey,
			eventdate,
			"domain",
			url,
			linkname,
			linkcontent,
			isunique,
			triggerersenddefinitionobjectid,
			triggeredsendcustomerkey,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			accountid,
			oybaccountid,
			jobid,
			listid,
			batchid,
			subscriberid,
			subscriberkey,
			eventdate,
			domain,
			url,
			linkname,
			linkcontent,
			isunique,
			triggerersenddefinitionobjectid,
			triggeredsendcustomerkey,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			source_system_id
		FROM
			(SELECT *,ROW_NUMBER() OVER(PARTITION BY listid,batchid,subscriberid,eventdate,linkcontent ORDER BY eventdate) as row_num 
			from edw_cip.cip_sfmc_send_log_clicks_extension )SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT
		ON ADT.record_aligner = 1
		and ADT.batch_name = 'SFMC_Send_Log_to_CIP' 
		WHERE SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_complaints */
Truncate table edw_staging.cip_sfmc_send_log_complaints;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_complaints from Source Table ext_edap_staging.Complaints */
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_complaints(
			clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			batchid,
			triggeredsendexternalkey,
			"domain",
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.clientid,
			SRC.sendid,
			SRC.subscriberkey,
			SRC.emailaddress,
			SRC.subscriberid,
			SRC.listid,
			SRC.eventdate,
			SRC.eventtype,
			SRC.batchid,
			SRC.triggeredsendexternalkey,
			SRC.domain,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM
			edw_cip.cip_sfmc_send_log_complaints SRC
			LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			and ADT.batch_name = 'SFMC_Send_Log_to_CIP' 
			WHERE SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_lists */
Truncate table edw_staging.cip_sfmc_send_log_lists;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_lists from Source Table ext_edap_staging.Lists */			
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_lists(
			clientid,
			listid,
			name,
			description,
			datecreated,
			status,
			listtype,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.clientid,
			SRC.listid,
			SRC.name,
			SRC.description,
			SRC.datecreated,
			SRC.status,
			SRC.listtype,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM
			(SELECT *,ROW_NUMBER() OVER(PARTITION BY listid ORDER BY datecreated) as row_num
			FROM EDW_CIP.cip_sfmc_send_log_lists) SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			and ADT.batch_name = 'SFMC_Send_Log_to_CIP' 
		WHERE row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/*Truncate Stage table*/
TRUNCATE TABLE EDW_STAGING.CIP_SFMC_SEND_LOG_MMFA_PRINTMAIL;

/*Insert into Stage table*/
INSERT INTO EDW_STAGING.CIP_SFMC_SEND_LOG_MMFA_PRINTMAIL
(
      FIRST_NM
    , MIDDLE_NM
    , LAST_NM
    , ADDRESS_STREET_1_TXT
    , ADDRESS_STREET_2_TXT
    , ADDRESS_STREET_3_TXT
    , CITY_NM
    , STATE_CDE
    , ZIP_CDE
    , ZIP_6_9_CDE
    , COUNTRY_CDE
    , RESPONSE_GUID
    , CAMPAIGN_ID
    , CREATIVE_CODE
    , MAIL_DT
    , DATA_PROCESS_DATE
    , MARKETING_AGENT_FULL_NM
    , AR_STATE_LICENSE_NR
    , CA_STATE_LICENSE_NR
    , MARKETING_ADDRESS_GREETING
    , MARKETING_GREETING
    , RESP_DT
    , SFMC_SUBSCRIBER_ID
    , MAIL_BY_DAYS
    , RESPOND_BY_DAYS
    , EXTRACTED_IND
    , BUSINESS_UNIT
    , DIM_PARTY_NATURAL_KEY_HASH_UUID
    , AGENT_BP_ID
    , AGENT_HOME_AGENCY_CDE
    , MASTER_CONTRACT_GROUP_ID
    , MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
    , CONTRACT_NAME_COLLOQUIAL
    , CUSTOM_URL
    , CUSTOM_PHONE
    , MINDSET_SEGMENT_DESC
    , AGE
    , PLAN_NM_1_TXT
    , PRODUCT_TYPE_CDE
    , MATCH_INDICATOR
    , COMPANY_NAME_TXT
    , NEW_IND
    , ENROLLMENT_START_DT
    , ENROLLMENT_END_DT
    , ENROLLMENT_TYPE_TXT
    , ENROLLMENT_LOGIN_TXT
    , BROKER1_NM
    , BROKER1_ELECTRONIC_ADDRESS_ID
    , BROKER1_PHONE_NR
    , LETTER_SEND_FROM_NM
    , LETTER_SEND_FROM_TITLE_TXT
    , GROUP_NR
    , SFMC_PARTY_ID
    , PLAN_ID
    , PARTICIPANT_ID
    , SUBSCRIBER_NR
    , ATTRIBUTE1
    , ATTRIBUTE2
    , ATTRIBUTE3
    , ATTRIBUTE4
    , ATTRIBUTE5
    , ATTRIBUTE6
    , ATTRIBUTE7
    , ATTRIBUTE8
    , ATTRIBUTE9
    , ATTRIBUTE10
    , ATTRIBUTE11
    , ATTRIBUTE12
    , COMBINED_KEY
    , LEAD_SOURCE
    , ECDM_MEMBER_ID
    , DECILE
    , SCORE
    , OFFER_NR
    , RESPONSE_TYPE
    , ROW_PROCESS_DTM
    , AUDIT_ID
    , SOURCE_SYSTEM_ID
    , SRC_SOURCE_SYSTEM_ID
	) 
	 SELECT
			  SRC.FIRST_NM
			, SRC.MIDDLE_NM
			, SRC.LAST_NM
			, SRC.ADDRESS_STREET_1_TXT
			, SRC.ADDRESS_STREET_2_TXT
			, SRC.ADDRESS_STREET_3_TXT
			, SRC.CITY_NM
			, SRC.STATE_CDE
			, SRC.ZIP_CDE
			, SRC.ZIP_6_9_CDE
			, SRC.COUNTRY_CDE
			, SRC.RESPONSE_GUID
			, SRC.CAMPAIGN_ID
			, SRC.CREATIVE_CODE
			, SRC.MAIL_DT
			, SRC.DATA_PROCESS_DATE
			, SRC.MARKETING_AGENT_FULL_NM
			, SRC.AR_STATE_LICENSE_NR
			, SRC.CA_STATE_LICENSE_NR
			, SRC.MARKETING_ADDRESS_GREETING
			, SRC.MARKETING_GREETING
			, SRC.RESP_DT
			, SRC.SFMC_SUBSCRIBER_ID
			, SRC.MAIL_BY_DAYS
			, SRC.RESPOND_BY_DAYS
			, SRC.EXTRACTED_IND
			, SRC.BUSINESS_UNIT
			, SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
			, SRC.AGENT_BP_ID
			, SRC.AGENT_HOME_AGENCY_CDE
			, SRC.MASTER_CONTRACT_GROUP_ID
			, SRC.MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
			, SRC.CONTRACT_NAME_COLLOQUIAL
			, SRC.CUSTOM_URL
			, SRC.CUSTOM_PHONE
			, SRC.MINDSET_SEGMENT_DESC
			, SRC.AGE
			, SRC.PLAN_NM_1_TXT
			, SRC.PRODUCT_TYPE_CDE
			, SRC.MATCH_INDICATOR
			, SRC.COMPANY_NAME_TXT
			, SRC.NEW_IND
			, SRC.ENROLLMENT_START_DT
			, SRC.ENROLLMENT_END_DT
			, SRC.ENROLLMENT_TYPE_TXT
			, SRC.ENROLLMENT_LOGIN_TXT
			, SRC.BROKER1_NM
			, SRC.BROKER1_ELECTRONIC_ADDRESS_ID
			, SRC.BROKER1_PHONE_NR
			, SRC.LETTER_SEND_FROM_NM
			, SRC.LETTER_SEND_FROM_TITLE_TXT
			, SRC.GROUP_NR
			, SRC.SFMC_PARTY_ID
			, SRC.PLAN_ID
			, SRC.PARTICIPANT_ID
			, SRC.SUBSCRIBER_NR
			, SRC.ATTRIBUTE1
			, SRC.ATTRIBUTE2
			, SRC.ATTRIBUTE3
			, SRC.ATTRIBUTE4
			, SRC.ATTRIBUTE5
			, SRC.ATTRIBUTE6
			, SRC.ATTRIBUTE7
			, SRC.ATTRIBUTE8
			, SRC.ATTRIBUTE9
			, SRC.ATTRIBUTE10
			, SRC.ATTRIBUTE11
			, SRC.ATTRIBUTE12
			, SRC.COMBINED_KEY
			, SRC.LEAD_SOURCE
			, SRC.ECDM_MEMBER_ID
			, SRC.DECILE
			, SRC.SCORE
			, SRC.OFFER_NR
			, SRC.RESPONSE_TYPE
			, SRC.ROW_PROCESS_DTM
			, ADT.AUDIT_ID AS AUDIT_ID
			, SRC.SOURCE_SYSTEM_ID
			, SRC.SRC_SOURCE_SYSTEM_ID
			FROM
			EDW_CIP.CIP_SFMC_SEND_LOG_MMFA_PRINTMAIL SRC
			LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT ON
			ADT.RECORD_ALIGNER = 1
			AND ADT.BATCH_NAME = 'SFMC_Send_Log_to_CIP' 
			
			WHERE ( CLEAN_STRING(SRC.dim_party_natural_key_hash_uuid::varchar) is null
			 OR REGEXP_ILIKE(DIM_PARTY_NATURAL_KEY_HASH_UUID::VARCHAR, '[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}'))
			AND CLEAN_STRING(SRC.SFMC_SUBSCRIBER_ID::VARCHAR) is not null 
			AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_opens */
Truncate table edw_staging.cip_sfmc_send_log_opens;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_opens from Source Table ext_edap_staging.Opens */	
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_opens(
			clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			batchid,
			triggeredsendexternalkey,
			isunique,
			ipaddress,
			country,
			region,
			city,
			latitude,
			longitude,
			metrocode,
			areacode,
			browser,
			emailclient,
			operatingsystem,
			device,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			batchid,
			triggeredsendexternalkey,
			isunique,
			ipaddress,
			country,
			region,
			city,
			latitude,
			longitude,
			metrocode,
			areacode,
			browser,
			emailclient,
			operatingsystem,
			device,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			source_system_id
		FROM
			edw_cip.cip_sfmc_send_log_opens SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			and ADT.batch_name = 'SFMC_Send_Log_to_CIP' 
		WHERE SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* TRUNCATE STAGE TABLE EDW_STAGING.CIP_SFMC_SEND_LOG_SEND_LOG */
TRUNCATE TABLE EDW_STAGING.CIP_SFMC_SEND_LOG_SEND_LOG;

/*INSERT DATA INTO EDW_STAGING.CIP_SFMC_SEND_LOG_SEND_LOG  */
  INSERT INTO EDW_STAGING.CIP_SFMC_SEND_LOG_SEND_LOG
	(
			  JOBID
			, LISTID
			, BATCHID
			, SUBID
			, TRIGGEREDSENDID
			, ERRORCODE
			, SENDDATE
			, ARCHIVEDDATE
			, SFMC_SUBSCRIBER_ID
			, DIM_PARTY_NATURAL_KEY_HASH_UUID
			, MINDSET_SEGMENT_DESC
			, MASTER_CONTRACT_GROUP_ID
			, PLAN_ID
			, SUBSCRIBER_NR
			, PARTICIPANT_ID
			, ADDITIONALEMAILATTRIBUTE1
			, ADDITIONALEMAILATTRIBUTE2
			, ADDITIONALEMAILATTRIBUTE3
			, ADDITIONALEMAILATTRIBUTE4
			, ADDITIONALEMAILATTRIBUTE5
			, ADDITIONALEMAILATTRIBUTE6
			, ADDITIONALEMAILATTRIBUTE7
			, ADDITIONALEMAILATTRIBUTE8
			, ADDITIONALEMAILATTRIBUTE9
			, ADDITIONALEMAILATTRIBUTE10
			, ADDITIONALEMAILATTRIBUTE11
			, ADDITIONALEMAILATTRIBUTE12
			, AGENT_BP_ID
			, AGENT_HOME_AGENCY_CDE
			, CAMPAIGN_ID
			, RESPONSE_TYPE
			, LEAD_SOURCE
			, DECILE
			, GROUP_NR
			, OFFER_NR
			, SCORE
			, COMPANY_NAME_TXT
			, ENROLLMENT_START_DT
			, ECDM_MEMBER_ID
			, RESPONSE_GUID
			, ROW_PROCESS_DTM
			, AUDIT_ID
			, SOURCE_SYSTEM_ID
    ) SELECT
			  SRC.JOBID
			, SRC.LISTID
			, SRC.BATCHID
			, SRC.SUBID
			, SRC.TRIGGEREDSENDID
			, SRC.ERRORCODE
			, SRC.SENDDATE
			, SRC.ARCHIVEDDATE
			, SRC.SFMC_SUBSCRIBER_ID
			, SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
			, SRC.MINDSET_SEGMENT_DESC
			, SRC.MASTER_CONTRACT_GROUP_ID
			, SRC.PLAN_ID
			, SRC.SUBSCRIBER_NR
			, SRC.PARTICIPANT_ID
			, SRC.ADDITIONALEMAILATTRIBUTE1
			, SRC.ADDITIONALEMAILATTRIBUTE2
			, SRC.ADDITIONALEMAILATTRIBUTE3
			, SRC.ADDITIONALEMAILATTRIBUTE4
			, SRC.ADDITIONALEMAILATTRIBUTE5
			, SRC.ADDITIONALEMAILATTRIBUTE6
			, SRC.ADDITIONALEMAILATTRIBUTE7
			, SRC.ADDITIONALEMAILATTRIBUTE8
			, SRC.ADDITIONALEMAILATTRIBUTE9
			, SRC.ADDITIONALEMAILATTRIBUTE10
			, SRC.ADDITIONALEMAILATTRIBUTE11
			, SRC.ADDITIONALEMAILATTRIBUTE12
			, SRC.AGENT_BP_ID
			, SRC.AGENT_HOME_AGENCY_CDE
			, SRC.CAMPAIGN_ID
			, SRC.RESPONSE_TYPE
			, SRC.LEAD_SOURCE
			, SRC.DECILE
			, SRC.GROUP_NR
			, SRC.OFFER_NR
			, SRC.SCORE
			, SRC.COMPANY_NAME_TXT
			, SRC.ENROLLMENT_START_DT
			, SRC.ECDM_MEMBER_ID
			, SRC.RESPONSE_GUID
			, SRC.ROW_PROCESS_DTM
			, ADT.AUDIT_ID AS AUDIT_ID
			, SRC.SOURCE_SYSTEM_ID
		FROM
			( SELECT *,ROW_NUMBER() OVER(PARTITION BY JOBID,LISTID,BATCHID,SUBID,SFMC_SUBSCRIBER_ID,SENDDATE ORDER BY SENDDATE) AS ROW_NUM
			  FROM EDW_CIP.CIP_SFMC_SEND_LOG_SEND_LOG
			) SRC
		      LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT 
			  ON ADT.RECORD_ALIGNER = 1
			  AND ADT.BATCH_NAME = 'SFMC_Send_Log_to_CIP'
			  WHERE SRC.ROW_NUM=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_sendjobs */
Truncate table edw_staging.cip_sfmc_send_log_sendjobs;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_sendjobs from Source Table ext_edap_staging.SendJobs */	
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_sendjobs(
			clientid,
			sendid,
			fromname,
			fromemail,
			schedtime,
			senttime,
			subject,
			emailname,
			triggeredsendexternalkey,
			senddefinitionexternalkey,
			jobstatus,
			previewurl,
			ismultipart,
			additional,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.clientid,
			SRC.sendid,
			SRC.fromname,
			SRC.fromemail,
			SRC.schedtime,
			SRC.senttime,
			SRC.subject,
			SRC.emailname,
			SRC.triggeredsendexternalkey,
			SRC.senddefinitionexternalkey,
			SRC.jobstatus,
			SRC.previewurl,
			SRC.ismultipart,
			SRC.additional,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM
			(SELECT *, ROW_NUMBER() OVER (PARTITION BY SENDID ORDER BY SENTTIME) AS row_num 
			FROM EDW_CIP.cip_sfmc_send_log_sendjobs) SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			and ADT.batch_name = 'SFMC_Send_Log_to_CIP' 
		WHERE SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_sent */
Truncate table edw_staging.cip_sfmc_send_log_sent;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_sent from Source Table ext_edap_staging.Sent */				
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_sent(
			clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			batchid,
			triggeredsendexternalkey,
			campaignid,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			batchid,
			triggeredsendexternalkey,
			campaignid,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			source_system_id
		FROM
			(SELECT *,ROW_NUMBER() OVER(PARTITION BY sendid,listid,batchid,subscriberid,eventdate ORDER BY eventdate) as row_num 
			FROM edw_cip.cip_sfmc_send_log_sent) SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			and ADT.batch_name = 'SFMC_Send_Log_to_CIP'
		WHERE SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/* Truncate Stage Table edw_staging.cip_sfmc_send_log_unsubs */
Truncate table edw_staging.cip_sfmc_send_log_unsubs;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_unsubs from Source Table ext_edap_staging.Unsubs */
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_unsubs(
			clientid,
			sendid,
			subscriberkey,
			emailaddress,
			subscriberid,
			listid,
			eventdate,
			eventtype,
			batchid,
			triggeredsendexternalkey,
			unsubreason,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.clientid,
			SRC.sendid,
			SRC.subscriberkey,
			SRC.emailaddress,
			SRC.subscriberid,
			SRC.listid,
			SRC.eventdate,
			SRC.eventtype,
			SRC.batchid,
			SRC.triggeredsendexternalkey,
			SRC.unsubreason,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			SRC.source_system_id
		FROM
			EDW_CIP.CIP_SFMC_SEND_LOG_UNSUBS SRC
		LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			and ADT.batch_name = 'SFMC_Send_Log_to_CIP' 
		WHERE SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/*Truncate Stage table*/
TRUNCATE TABLE EDW_STAGING.CIP_SFMC_SEND_LOG_WP_CUSTOM_PRINTMAIL;

/*Insert into Stage table*/
INSERT INTO EDW_STAGING.CIP_SFMC_SEND_LOG_WP_CUSTOM_PRINTMAIL
(
	FIRST_NM
	,MIDDLE_NM
	,LAST_NM
	,ADDRESS_STREET_1_TXT
	,ADDRESS_STREET_2_TXT
	,ADDRESS_STREET_3_TXT
	,CITY_NM
	,STATE_CDE
	,ZIP_CDE
	,ZIP_6_9_CDE
	,COUNTRY_CDE
	,PIN_NUMBER
	,CAMPAIGN_ID
	,CREATIVE_CODE
	,MAIL_DT
	,DATA_PROCESS_DATE
	,MARKETING_AGENT_FULL_NM
	,AR_STATE_LICENSE_NR
	,CA_STATE_LICENSE_NR
	,MARKETING_ADDRESS_GREETING
	,MARKETING_GREETING
	,RESP_DT
	,SFMC_SUBSCRIBER_ID
	,MAIL_BY_DAYS
	,RESPOND_BY_DAYS
	,EXTRACTED_IND
	,BUSINESS_UNIT
	,DIM_PARTY_NATURAL_KEY_HASH_UUID
	,AGENT_BP_ID
	,AGENT_HOME_AGENCY_CDE
	,MASTER_CONTRACT_GROUP_ID
	,MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
	,CONTRACT_NAME_COLLOQUIAL
	,CUSTOM_URL
	,CUSTOM_PHONE
	,MINDSET_SEGMENT_DESC
	,AGE
	,PLAN_NM_1_TXT
	,PRODUCT_TYPE_CDE
	,MATCH_INDICATOR
	,COMPANY_NAME_TXT
	,NEW_IND
	,ENROLLMENT_START_DT
	,ENROLLMENT_END_DT
	,ENROLLMENT_TYPE_TXT
	,ENROLLMENT_LOGIN_TXT
	,BROKER1_NM
	,BROKER1_ELECTRONIC_ADDRESS_ID
	,BROKER1_PHONE_NR
	,LETTER_SEND_FROM_NM
	,LETTER_SEND_FROM_TITLE_TXT
	,GROUP_NR
	,SFMC_PARTY_ID
	,PLAN_ID
	,PARTICIPANT_ID
	,SUBSCRIBER_NR
	,ATTRIBUTE1
	,ATTRIBUTE2
	,ATTRIBUTE3
	,ATTRIBUTE4
	,ATTRIBUTE5
	,ATTRIBUTE6
	,ATTRIBUTE7
	,ATTRIBUTE8
	,ATTRIBUTE9
	,ATTRIBUTE10
	,ATTRIBUTE11
	,ATTRIBUTE12
	,COMBINED_KEY
	,ROW_PROCESS_DTM
	,AUDIT_ID
	,SOURCE_SYSTEM_ID
	,SRC_SOURCE_SYSTEM_ID
) 
SELECT
	SRC.FIRST_NM
	,SRC.MIDDLE_NM
	,SRC.LAST_NM
	,SRC.ADDRESS_STREET_1_TXT
	,SRC.ADDRESS_STREET_2_TXT
	,SRC.ADDRESS_STREET_3_TXT
	,SRC.CITY_NM
	,SRC.STATE_CDE
	,SRC.ZIP_CDE
	,SRC.ZIP_6_9_CDE
	,SRC.COUNTRY_CDE
	,SRC.PIN_NUMBER
	,SRC.CAMPAIGN_ID
	,SRC.CREATIVE_CODE
	,SRC.MAIL_DT
	,SRC.DATA_PROCESS_DATE
	,SRC.MARKETING_AGENT_FULL_NM
	,SRC.AR_STATE_LICENSE_NR
	,SRC.CA_STATE_LICENSE_NR
	,SRC.MARKETING_ADDRESS_GREETING
	,SRC.MARKETING_GREETING
	,SRC.RESP_DT :: TIMESTAMPTZ
	,SRC.SFMC_SUBSCRIBER_ID
	,SRC.MAIL_BY_DAYS
	,SRC.RESPOND_BY_DAYS
	,SRC.EXTRACTED_IND
	,SRC.BUSINESS_UNIT
	,SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
	,SRC.AGENT_BP_ID
	,SRC.AGENT_HOME_AGENCY_CDE
	,SRC.MASTER_CONTRACT_GROUP_ID
	,SRC.MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
	,SRC.CONTRACT_NAME_COLLOQUIAL
	,SRC.CUSTOM_URL
	,SRC.CUSTOM_PHONE
	,SRC.MINDSET_SEGMENT_DESC
	,SRC.AGE
	,SRC.PLAN_NM_1_TXT
	,SRC.PRODUCT_TYPE_CDE
	,SRC.MATCH_INDICATOR
	,SRC.COMPANY_NAME_TXT
	,SRC.NEW_IND
	,SRC.ENROLLMENT_START_DT
	,SRC.ENROLLMENT_END_DT
	,SRC.ENROLLMENT_TYPE_TXT
	,SRC.ENROLLMENT_LOGIN_TXT
	,SRC.BROKER1_NM
	,SRC.BROKER1_ELECTRONIC_ADDRESS_ID
	,SRC.BROKER1_PHONE_NR
	,SRC.LETTER_SEND_FROM_NM
	,SRC.LETTER_SEND_FROM_TITLE_TXT
	,SRC.GROUP_NR
	,SRC.SFMC_PARTY_ID
	,SRC.PLAN_ID
	,SRC.PARTICIPANT_ID
	,SRC.SUBSCRIBER_NR
	,SRC.ATTRIBUTE1
	,SRC.ATTRIBUTE2
	,SRC.ATTRIBUTE3
	,SRC.ATTRIBUTE4
	,SRC.ATTRIBUTE5
	,SRC.ATTRIBUTE6
	,SRC.ATTRIBUTE7
	,SRC.ATTRIBUTE8
	,SRC.ATTRIBUTE9
	,SRC.ATTRIBUTE10
	,SRC.ATTRIBUTE11
	,SRC.ATTRIBUTE12
	,SRC.COMBINED_KEY
	,SRC.ROW_PROCESS_DTM
	,ADT.AUDIT_ID AS AUDIT_ID
	,SRC.SOURCE_SYSTEM_ID
	,SRC.SRC_SOURCE_SYSTEM_ID
	FROM EDW_CIP.CIP_SFMC_SEND_LOG_WP_CUSTOM_PRINTMAIL SRC
	
	LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT 
	ON ADT.RECORD_ALIGNER = 1
	AND ADT.BATCH_NAME = 'SFMC_Send_Log_to_CIP'
	
	WHERE (CLEAN_STRING(SRC.dim_party_natural_key_hash_uuid::varchar) is null
	OR REGEXP_ILIKE(DIM_PARTY_NATURAL_KEY_HASH_UUID::VARCHAR,'[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}'))
	AND CLEAN_STRING(SRC.SFMC_SUBSCRIBER_ID::VARCHAR) is not null 
	AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/*Truncate Stage table*/
TRUNCATE TABLE EDW_STAGING.CIP_SFMC_SEND_LOG_WP_PRINTMAIL;

/*Insert into Stage table*/
INSERT INTO EDW_STAGING.CIP_SFMC_SEND_LOG_WP_PRINTMAIL(
FIRST_NM
,MIDDLE_NM
,LAST_NM
,ADDRESS_STREET_1_TXT
,ADDRESS_STREET_2_TXT
,ADDRESS_STREET_3_TXT
,CITY_NM
,STATE_CDE
,ZIP_CDE
,ZIP_6_9_CDE
,COUNTRY_CDE
,PIN_NUMBER
,CAMPAIGN_ID
,CREATIVE_CODE
,MAIL_DT
,DATA_PROCESS_DATE
,MARKETING_AGENT_FULL_NM
,AR_STATE_LICENSE_NR
,CA_STATE_LICENSE_NR
,MARKETING_ADDRESS_GREETING
,MARKETING_GREETING
,RESP_DT
,SFMC_SUBSCRIBER_ID
,MAIL_BY_DAYS
,RESPOND_BY_DAYS
,EXTRACTED_IND
,BUSINESS_UNIT
,DIM_PARTY_NATURAL_KEY_HASH_UUID
,AGENT_BP_ID
,AGENT_HOME_AGENCY_CDE
,MASTER_CONTRACT_GROUP_ID
,MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
,CONTRACT_NAME_COLLOQUIAL
,CUSTOM_URL
,CUSTOM_PHONE
,MINDSET_SEGMENT_DESC
,AGE
,PLAN_NM_1_TXT
,PRODUCT_TYPE_CDE
,MATCH_INDICATOR
,COMPANY_NAME_TXT
,NEW_IND
,ENROLLMENT_START_DT
,ENROLLMENT_END_DT
,ENROLLMENT_TYPE_TXT
,ENROLLMENT_LOGIN_TXT
,BROKER1_NM
,BROKER1_ELECTRONIC_ADDRESS_ID
,BROKER1_PHONE_NR
,LETTER_SEND_FROM_NM
,LETTER_SEND_FROM_TITLE_TXT
,GROUP_NR
,SFMC_PARTY_ID
,PLAN_ID
,PARTICIPANT_ID
,SUBSCRIBER_NR
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,COMBINED_KEY
,ROW_PROCESS_DTM
,AUDIT_ID
,SOURCE_SYSTEM_ID
,SRC_SOURCE_SYSTEM_ID
) SELECT
SRC.FIRST_NM
,SRC.MIDDLE_NM
,SRC.LAST_NM
,SRC.ADDRESS_STREET_1_TXT
,SRC.ADDRESS_STREET_2_TXT
,SRC.ADDRESS_STREET_3_TXT
,SRC.CITY_NM
,SRC.STATE_CDE
,SRC.ZIP_CDE
,SRC.ZIP_6_9_CDE
,SRC.COUNTRY_CDE
,SRC.PIN_NUMBER
,SRC.CAMPAIGN_ID
,SRC.CREATIVE_CODE
,SRC.MAIL_DT
,SRC.DATA_PROCESS_DATE
,SRC.MARKETING_AGENT_FULL_NM
,SRC.AR_STATE_LICENSE_NR
,SRC.CA_STATE_LICENSE_NR
,SRC.MARKETING_ADDRESS_GREETING
,SRC.MARKETING_GREETING
,SRC.RESP_DT :: TIMESTAMPTZ
,SRC.SFMC_SUBSCRIBER_ID
,SRC.MAIL_BY_DAYS
,SRC.RESPOND_BY_DAYS
,SRC.EXTRACTED_IND
,SRC.BUSINESS_UNIT
,SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
,SRC.AGENT_BP_ID
,SRC.AGENT_HOME_AGENCY_CDE
,SRC.MASTER_CONTRACT_GROUP_ID
,SRC.MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
,SRC.CONTRACT_NAME_COLLOQUIAL
,SRC.CUSTOM_URL
,SRC.CUSTOM_PHONE
,SRC.MINDSET_SEGMENT_DESC
,SRC.AGE
,SRC.PLAN_NM_1_TXT
,SRC.PRODUCT_TYPE_CDE
,SRC.MATCH_INDICATOR
,SRC.COMPANY_NAME_TXT
,SRC.NEW_IND
,SRC.ENROLLMENT_START_DT
,SRC.ENROLLMENT_END_DT
,SRC.ENROLLMENT_TYPE_TXT
,SRC.ENROLLMENT_LOGIN_TXT
,SRC.BROKER1_NM
,SRC.BROKER1_ELECTRONIC_ADDRESS_ID
,SRC.BROKER1_PHONE_NR
,SRC.LETTER_SEND_FROM_NM
,SRC.LETTER_SEND_FROM_TITLE_TXT
,SRC.GROUP_NR
,SRC.SFMC_PARTY_ID
,SRC.PLAN_ID
,SRC.PARTICIPANT_ID
,SRC.SUBSCRIBER_NR
,SRC.ATTRIBUTE1
,SRC.ATTRIBUTE2
,SRC.ATTRIBUTE3
,SRC.ATTRIBUTE4
,SRC.ATTRIBUTE5
,SRC.ATTRIBUTE6
,SRC.ATTRIBUTE7
,SRC.ATTRIBUTE8
,SRC.ATTRIBUTE9
,SRC.ATTRIBUTE10
,SRC.ATTRIBUTE11
,SRC.ATTRIBUTE12
,SRC.COMBINED_KEY
,SRC.ROW_PROCESS_DTM
,ADT.AUDIT_ID AS AUDIT_ID
,SRC.SOURCE_SYSTEM_ID
,SRC.SRC_SOURCE_SYSTEM_ID
FROM
EDW_CIP.CIP_SFMC_SEND_LOG_WP_PRINTMAIL SRC
LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT ON
ADT.RECORD_ALIGNER = 1
AND ADT.BATCH_NAME = 'SFMC_Send_Log_to_CIP' 

WHERE ( CLEAN_STRING(SRC.dim_party_natural_key_hash_uuid::varchar) is null
			 OR REGEXP_ILIKE(DIM_PARTY_NATURAL_KEY_HASH_UUID::VARCHAR, '[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}'))
			AND CLEAN_STRING(SRC.SFMC_SUBSCRIBER_ID::VARCHAR) is not null 
	AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);

/*Truncate Stage table*/
TRUNCATE TABLE EDW_STAGING.CIP_SFMC_SEND_LOG_WS_PRINTMAIL;

/*Insert into Stage table*/
INSERT INTO EDW_STAGING.CIP_SFMC_SEND_LOG_WS_PRINTMAIL
(
	FIRST_NM
	,MIDDLE_NM
	,LAST_NM
	,ADDRESS_STREET_1_TXT
	,ADDRESS_STREET_2_TXT
	,ADDRESS_STREET_3_TXT
	,CITY_NM
	,STATE_CDE
	,ZIP_CDE
	,ZIP_6_9_CDE
	,COUNTRY_CDE
	,PIN_NUMBER
	,CAMPAIGN_ID
	,CREATIVE_CODE
	,MAIL_DT
	,DATA_PROCESS_DATE
	,MARKETING_AGENT_FULL_NM
	,AR_STATE_LICENSE_NR
	,CA_STATE_LICENSE_NR
	,MARKETING_ADDRESS_GREETING
	,MARKETING_GREETING
	,RESP_DT
	,SFMC_SUBSCRIBER_ID
	,MAIL_BY_DAYS
	,RESPOND_BY_DAYS
	,EXTRACTED_IND
	,BUSINESS_UNIT
	,DIM_PARTY_NATURAL_KEY_HASH_UUID
	,AGENT_BP_ID
	,AGENT_HOME_AGENCY_CDE
	,MASTER_CONTRACT_GROUP_ID
	,MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
	,CONTRACT_NAME_COLLOQUIAL
	,CUSTOM_URL
	,CUSTOM_PHONE
	,MINDSET_SEGMENT_DESC
	,AGE
	,PLAN_NM_1_TXT
	,PRODUCT_TYPE_CDE
	,MATCH_INDICATOR
	,COMPANY_NAME_TXT
	,NEW_IND
	,ENROLLMENT_START_DT
	,ENROLLMENT_END_DT
	,ENROLLMENT_TYPE_TXT
	,ENROLLMENT_LOGIN_TXT
	,BROKER1_NM
	,BROKER1_ELECTRONIC_ADDRESS_ID
	,BROKER1_PHONE_NR
	,LETTER_SEND_FROM_NM
	,LETTER_SEND_FROM_TITLE_TXT
	,GROUP_NR
	,SFMC_PARTY_ID
	,PLAN_ID
	,PARTICIPANT_ID
	,SUBSCRIBER_NR
	,ATTRIBUTE1
	,ATTRIBUTE2
	,ATTRIBUTE3
	,ATTRIBUTE4
	,ATTRIBUTE5
	,ATTRIBUTE6
	,ATTRIBUTE7
	,ATTRIBUTE8
	,ATTRIBUTE9
	,ATTRIBUTE10
	,ATTRIBUTE11
	,ATTRIBUTE12
	,COMBINED_KEY
	,ROW_PROCESS_DTM
	,AUDIT_ID
	,SOURCE_SYSTEM_ID
	,SRC_SOURCE_SYSTEM_ID
) 
SELECT
	SRC.FIRST_NM
	,SRC.MIDDLE_NM
	,SRC.LAST_NM
	,SRC.ADDRESS_STREET_1_TXT
	,SRC.ADDRESS_STREET_2_TXT
	,SRC.ADDRESS_STREET_3_TXT
	,SRC.CITY_NM
	,SRC.STATE_CDE
	,SRC.ZIP_CDE
	,SRC.ZIP_6_9_CDE
	,SRC.COUNTRY_CDE
	,SRC.PIN_NUMBER
	,SRC.CAMPAIGN_ID
	,SRC.CREATIVE_CODE
	,SRC.MAIL_DT
	,SRC.DATA_PROCESS_DATE
	,SRC.MARKETING_AGENT_FULL_NM
	,SRC.AR_STATE_LICENSE_NR
	,SRC.CA_STATE_LICENSE_NR
	,SRC.MARKETING_ADDRESS_GREETING
	,SRC.MARKETING_GREETING
	,SRC.RESP_DT
	,SRC.SFMC_SUBSCRIBER_ID
	,SRC.MAIL_BY_DAYS
	,SRC.RESPOND_BY_DAYS
	,SRC.EXTRACTED_IND
	,SRC.BUSINESS_UNIT
	,SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID
	,SRC.AGENT_BP_ID
	,SRC.AGENT_HOME_AGENCY_CDE
	,SRC.MASTER_CONTRACT_GROUP_ID
	,SRC.MASTER_CONTRACT_GROUP_NM_NATIONAL_DESC
	,SRC.CONTRACT_NAME_COLLOQUIAL
	,SRC.CUSTOM_URL
	,SRC.CUSTOM_PHONE
	,SRC.MINDSET_SEGMENT_DESC
	,SRC.AGE
	,SRC.PLAN_NM_1_TXT
	,SRC.PRODUCT_TYPE_CDE
	,SRC.MATCH_INDICATOR
	,SRC.COMPANY_NAME_TXT
	,SRC.NEW_IND
	,SRC.ENROLLMENT_START_DT
	,SRC.ENROLLMENT_END_DT
	,SRC.ENROLLMENT_TYPE_TXT
	,SRC.ENROLLMENT_LOGIN_TXT
	,SRC.BROKER1_NM
	,SRC.BROKER1_ELECTRONIC_ADDRESS_ID
	,SRC.BROKER1_PHONE_NR
	,SRC.LETTER_SEND_FROM_NM
	,SRC.LETTER_SEND_FROM_TITLE_TXT
	,SRC.GROUP_NR
	,SRC.SFMC_PARTY_ID
	,SRC.PLAN_ID
	,SRC.PARTICIPANT_ID
	,SRC.SUBSCRIBER_NR
	,SRC.ATTRIBUTE1
	,SRC.ATTRIBUTE2
	,SRC.ATTRIBUTE3
	,SRC.ATTRIBUTE4
	,SRC.ATTRIBUTE5
	,SRC.ATTRIBUTE6
	,SRC.ATTRIBUTE7
	,SRC.ATTRIBUTE8
	,SRC.ATTRIBUTE9
	,SRC.ATTRIBUTE10
	,SRC.ATTRIBUTE11
	,SRC.ATTRIBUTE12
	,SRC.COMBINED_KEY
	,CURRENT_TIMESTAMP AS ROW_PROCESS_DTM
	,ADT.AUDIT_ID AS AUDIT_ID
	,SRC.SOURCE_SYSTEM_ID
	,SRC.SRC_SOURCE_SYSTEM_ID
	FROM EDW_CIP.CIP_SFMC_SEND_LOG_WS_PRINTMAIL SRC
	
	LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT ON
	ADT.RECORD_ALIGNER = 1
	AND ADT.BATCH_NAME = 'SFMC_Send_Log_to_CIP'
	
	WHERE (CLEAN_STRING(SRC.dim_party_natural_key_hash_uuid::varchar) is null
		OR REGEXP_ILIKE(DIM_PARTY_NATURAL_KEY_HASH_UUID::VARCHAR,'[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}'))
	AND CLEAN_STRING(SRC.SFMC_SUBSCRIBER_ID::varchar) is not null 
	AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6); 
	
/* Truncate Stage Table edw_staging.cip_sfmc_send_log_customer_response */
Truncate table edw_staging.cip_sfmc_send_log_customer_response;

/*Insert Values in to Stage Table edw_staging.cip_sfmc_send_log_customer_response from Source Table ext_edap_staging.Customer_Response_Details_Data_Extract */
INSERT
	INTO
		edw_staging.cip_sfmc_send_log_customer_response(
			client_first_nm,
			client_last_nm,
			electronic_address_id,
			preferredcontactmethod,
			callbackpreferrence,
			emailname,
			sfmc_subscriber_id,
			campaign_id,
			sf_crm_contact_id,
			campaignmemberid,
			campaignmemberstatus,
			phone_nr,
			dim_party_natural_key_hash_uuid,
			agent_dim_party_natural_key_hash_uuid,
			agent_home_agency_cde,
			jobid,
			respondeddate,
			topicsofconversation,
			row_process_dtm,
			audit_id,
			source_system_id
		) SELECT
			SRC.client_first_nm,
			SRC.client_last_nm,
			SRC.electronic_address_id,
			SRC.preferredcontactmethod,
			SRC.callbackpreferrence,
			SRC.emailname,
			SRC.sfmc_subscriber_id,
			SRC.campaign_id,
			SRC.sf_crm_contact_id,
			SRC.campaignmemberid,
			SRC.campaignmemberstatus,
			SRC.phone_nr::int as phone_nr,
			SRC.dim_party_natural_key_hash_uuid,
			SRC.agent_dim_party_natural_key_hash_uuid,
			SRC.agent_home_agency_cde::int as agent_home_agency_cde,
			SRC.jobid::int as jobid,
			SRC.respondeddate::timestamp as respondeddate,
			SRC.topicsofconversation,
			SRC.row_process_dtm,
			ADT.AUDIT_ID as audit_id,
			222 as source_system_id
			FROM
			(
				SELECT *,ROW_NUMBER() OVER(PARTITION BY JOBID,SFMC_SUBSCRIBER_ID,CAMPAIGN_ID,RESPONDEDDATE ORDER BY RESPONDEDDATE DESC) AS row_num 
				from edw_cip.cip_sfmc_send_log_customer_response
			) SRC
			LEFT JOIN edw_audit.etl_batch_audit_vw ADT ON
			ADT.record_aligner = 1
			AND ADT.batch_name = 'SFMC_Send_Log_to_CIP'
			WHERE CLEAN_STRING(SRC.sfmc_subscriber_id::varchar) is not null and SRC.row_num=1 AND SRC.row_process_dtm BETWEEN :date1::timestamp(6) AND :date2::timestamp(6);
