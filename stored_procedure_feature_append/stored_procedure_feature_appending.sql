create or replace procedure risk.test.feature_appending(driver_table varchar, staging_prefix varchar, output_table varchar)

/*
driver_table - with dev population(must have fields: user_id, auth_event_merchant_name_raw, mcc_cd, auth_event_id, auth_event_created_ts, req_amt, entry_type)
staging_prefix - used as staging table name prefix(e.g.: risk.test.'staging_prefix'_user_profile)
output_table - final output table name
*/

returns varchar
language sql
as

$$
begin

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user profile

key: user_id

with feature store features simulated: 
    user_id__profile

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__profile/v2.sql
*/
	let tbl_user_profile varchar := staging_prefix||'_user_profile';

    create or replace table identifier(:tbl_user_profile) as (
    with t1 as(
    SELECT a.user_id,
           first_name,
           last_name,
           address,
           last_four_ssn,
           enrollment_initiated_ts AS enrollment_time,
           user_status,
           year_of_birth,
           stated_income,
           city,
           county,
           email,
           current_sign_in_ip,
           last_sign_in_ip,
           referred_by,
           datediff(day,enrollment_initiated_ts,last_sign_in_ts) as daydiff_enroll_lastsignin,
           datediff(day,enrollment_initiated_ts,current_sign_in_ts) as daydiff_enroll_cursignin,    
           zip_code::string as zip_code,
           LEFT(phone,3) as phone_code,
           state_cd as state_code,
           first_name || last_name                                                                           AS full_name,
           SPLIT(LOWER(first_name), ' ')                                                                     AS first_name_parts,
           SPLIT(LOWER(last_name), ' ')                                                                      AS last_name_parts,
           CASE
               WHEN LENGTH(SPLIT_PART(LOWER(last_name), '-', 1)) > 2 THEN SPLIT_PART(LOWER(last_name), '-', 1)
               ELSE '***' END                                                                                AS last_name_part1_1,
           CASE
               WHEN LENGTH(SPLIT_PART(LOWER(last_name), ' ', 1)) > 2 THEN SPLIT_PART(LOWER(last_name), ' ', 1)
               ELSE '***' END                                                                                AS last_name_part1_2,
           CASE
               WHEN LENGTH(SPLIT_PART(LOWER(last_name), '-', 2)) > 2 THEN SPLIT_PART(LOWER(last_name), '-', 2)
               ELSE '***' END                                                                                AS last_name_part2_1,
           CASE
               WHEN LENGTH(SPLIT_PART(LOWER(last_name), ' ', 2)) > 2 THEN SPLIT_PART(LOWER(last_name), ' ', 2)
               ELSE '***' END                                                                                AS last_name_part2_2,
           CASE WHEN nickname IS NULL THEN 0 ELSE 1 END                                                      AS has_nickname,

           -- Email features
           REGEXP_SUBSTR(email, '.+@(.+?)\\.', 1, 1, 'e')                                                    AS email_domain,
           REPLACE(SPLIT_PART(SPLIT_PART(LOWER(email), '@', 1), '+', 1), '.',
                   '')                                                                                       AS email_name,
           REGEXP_REPLACE(email_name, '\\d', '')                                                             AS email_letters_all,
           REGEXP_SUBSTR(email_name, '[a-z]+', 1)                                                            AS email_letters_first,
           EDITDISTANCE(first_name, REGEXP_SUBSTR(email, '(.+?)@', 1, 1, 'e'))                               AS ld_bw_first_name_and_email,
           EDITDISTANCE(last_name, REGEXP_SUBSTR(email, '(.+?)@', 1, 1, 'e'))                                AS ld_bw_last_name_and_email,
           EDITDISTANCE(full_name, email_letters_all)                                                        AS ld_bw_full_name_and_email,
           EDITDISTANCE(full_name, email_letters_first)                                                      AS ld_bw_full_name_and_email_first_letter,
           DIV0(ld_bw_full_name_and_email,
           GREATEST(LENGTH(full_name), LENGTH(email_letters_all)))                                           AS email_ld_norm1,
           DIV0(ld_bw_full_name_and_email_first_letter,
           GREATEST(LENGTH(full_name), LENGTH(email_letters_first)))                                         AS email_ld_norm2,
           CASE
               WHEN CONTAINS(email_name, LOWER(first_name))
                   AND (CONTAINS(email_name, LOWER(last_name))
                       OR CONTAINS(email_name, last_name_part1_1)
                       OR CONTAINS(email_name, last_name_part1_2)
                       OR CONTAINS(email_name, last_name_part2_1)
                       OR CONTAINS(email_name, last_name_part2_2)) THEN 'full_name'
               WHEN CONTAINS(email_name, LOWER(last_name))
                   OR CONTAINS(email_name, last_name_part1_1)
                   OR CONTAINS(email_name, last_name_part1_2)
                   OR CONTAINS(email_name, last_name_part2_1)
                   OR CONTAINS(email_name, last_name_part2_2) THEN 'last_name'
               WHEN CONTAINS(email_name, LOWER(first_name)) THEN 'first_name'
               WHEN CONTAINS(email_name, LOWER(LEFT(last_name, 5))) THEN 'last_name_5letters'
               WHEN CONTAINS(email_name, LOWER(LEFT(first_name, 5))) THEN 'first_name_5letters'
               WHEN CONTAINS(email_name, LOWER(LEFT(last_name, 3))) THEN 'last_name_3letters'
               WHEN CONTAINS(email_name, LOWER(LEFT(first_name, 3))) THEN 'first_name_3letters'
               ELSE 'none'
               END                                                                                           AS email_contains_name,
           DATEDIFF(YEAR, date_of_birth, current_timestamp())                                                 AS age,
           RIGHT(CONCAT('000', last_four_ssn), 4)                                                            AS last4_ssn,
           CASE WHEN CONTAINS(email_name, last4_ssn) THEN 1 ELSE 0 END                                       AS is_email_contains_last4_ssn,
           REGEXP_SUBSTR(nickname, '-(\\d+)$', 1, 1, 'e', 1)                                                 AS nickname_increment
    FROM edw_pii_db.core.dim_user_pii a
    inner join (select distinct user_id from identifier(:driver_table)) b on (a.user_id=b.user_id)
    where 1=1
    )
    select 
       1 as test,
       user_id,
       first_name,
       last_name,
       city,
       county,
       address,
       email,
       email_name,
       last_four_ssn,
       current_sign_in_ip,
       last_sign_in_ip,
       referred_by,
       daydiff_enroll_lastsignin,
       daydiff_enroll_cursignin,  
       datediff(month,enrollment_time, current_timestamp()) as mob_enroll,
       enrollment_time                        AS na__enrollment_time,
       stated_income                          AS na__stated_income,
       state_code                             AS na__state_code,
       zip_code                               AS na__zip_code,
       phone_code                             AS na__phone_code,
       email_domain                           AS na__email_domain,
       ld_bw_first_name_and_email             AS na__ld_bw_first_name_and_email,
       ld_bw_last_name_and_email              AS na__ld_bw_last_name_and_email,
       ld_bw_full_name_and_email              AS na__ld_bw_full_name_and_email,
       ld_bw_full_name_and_email_first_letter AS na__ld_bw_full_name_and_email_first_letter,
       email_ld_norm1                         AS na__email_ld_norm1,
       email_ld_norm2                         AS na__email_ld_norm2,
       email_contains_name                    AS na__email_contains_name,
       age                                    AS na__age,
       is_email_contains_last4_ssn            AS na__is_email_contains_last4_ssn
       from t1
    );
    
 

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
merchant user_id tran vel(beta)

key: auth_event_id
offset: 7d
lookback: 390/90
*/  

	let tbl_mrch_uid_vel_b varchar := staging_prefix||'_mrch_uid_vel_b';

	create or replace table identifier(:tbl_mrch_uid_vel_b) as(

		with t1 as( 
	        select 
			a.auth_event_id
			,-1*rae.req_amt as amt  
			,a.auth_event_created_ts as curr_auth_ts
			,rae.auth_event_created_ts as past_auth_ts
			,case when rae.response_cd in ('00', '10') then 1 else 0 end as approved
			,case when d.dispute_created_at is not null then 1 else 0 end as dispute_fraud

			FROM identifier(:driver_table) a
			left join edw_db.core.fct_realtime_auth_event rae on (a.user_id=rae.user_id and a.auth_event_merchant_name_raw=rae.auth_event_merchant_name_raw and rae.auth_event_created_ts between dateadd(day,-390,a.auth_event_created_ts) and dateadd(day,-7,a.auth_event_created_ts))
		  	LEFT JOIN edw_db.core.fct_realtime_auth_event dual_auth ON rae.auth_id = dual_auth.original_auth_id AND rae.user_id = dual_auth.user_id
		  	LEFT JOIN risk.prod.disputed_transactions d on (rae.auth_id = d.authorization_code or dual_auth.auth_id = d.authorization_code) and rae.user_id = d.user_id and d.dispute_created_at<a.auth_event_created_ts
		           

		  	WHERE 1=1
		    and rae.original_auth_id = 0
		    AND rae.req_amt < 0
    )
    	SELECT auth_event_id,
        
            min(datediff(hour,past_auth_ts,curr_auth_ts)) as min__mrch_uid_hourgap_p390,
            max(datediff(hour,past_auth_ts,curr_auth_ts)) as max__mrch_uid_hourgap_p390,
            
            sum(amt) as sum__mrch_uid_txn_p390,
            count(auth_event_id) as count__mrch_uid_txn_p390,
            sum(case when approved=1 then amt else 0 end) as sum__mrch_uid_appv_txn_p390,
            count(case when approved=1 then auth_event_id end) as count__mrch_uid_appv_txn_p390,
            sum(case when dispute_fraud=1 then amt else 0 end) as sum__mrch_uid_disp_txn_p390,
            count(case when dispute_fraud=1 then auth_event_id end) as count__mrch_uid_disp_txn_p390,
            sum__mrch_uid_appv_txn_p390/nullifzero(sum__mrch_uid_txn_p390) as ratio__mrch_uid_dlc_txn_sum_p390,
            count__mrch_uid_appv_txn_p390/nullifzero(count__mrch_uid_txn_p390) as ratio__mrch_uid_dlc_txn_cnt_p390,
            sum__mrch_uid_disp_txn_p390/nullifzero(sum__mrch_uid_appv_txn_p390) as ratio__mrch_uid_disp_txn_sum_p390,
            count__mrch_uid_disp_txn_p390/nullifzero(count__mrch_uid_appv_txn_p390) as ratio__mrch_uid_disp_txn_cnt_p390,
            
            sum(case when past_auth_ts>=dateadd(day,-90,curr_auth_ts) then amt else 0 end) as sum__mrch_uid_txn_p90,
            count(case when past_auth_ts>=dateadd(day,-90,curr_auth_ts) then auth_event_id end) as count__mrch_uid_txn_p90,
            sum(case when past_auth_ts>=dateadd(day,-90,curr_auth_ts) and approved=1 then amt else 0 end) as sum__mrch_uid_appv_txn_p90,
            count(case when past_auth_ts>=dateadd(day,-90,curr_auth_ts) and approved=1 then auth_event_id end) as count__mrch_uid_appv_txn_p90,
            sum(case when past_auth_ts>=dateadd(day,-90,curr_auth_ts) and dispute_fraud=1 then amt else 0 end) as sum__mrch_uid_disp_txn_p90,
            count(case when past_auth_ts>=dateadd(day,-90,curr_auth_ts) and dispute_fraud=1 then auth_event_id end) as count__mrch_uid_disp_txn_p90,
            sum__mrch_uid_appv_txn_p90/nullifzero(sum__mrch_uid_txn_p90) as ratio__mrch_uid_dlc_txn_sum_p90,
            count__mrch_uid_appv_txn_p90/nullifzero(count__mrch_uid_txn_p90) as ratio__mrch_uid_dlc_txn_cnt_p90,
            sum__mrch_uid_disp_txn_p90/nullifzero(sum__mrch_uid_appv_txn_p90) as ratio__mrch_uid_disp_txn_sum_p90,
            count__mrch_uid_disp_txn_p90/nullifzero(count__mrch_uid_appv_txn_p90) as ratio__mrch_uid_disp_txn_cnt_p90
            
        FROM t1
	  	GROUP BY 1
	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
merchant user_id tran vel2(beta)

key: auth_event_id
offset: 7d
lookback: 390/90
*/  

	let tbl_mrch_uid_vel2_b varchar := staging_prefix||'_mrch_uid_vel2_b';

	create or replace table identifier(:tbl_mrch_uid_vel2_b) as(

		with t1 as( 
	        select 
			a.auth_event_id
			,-1*rae.req_amt as amt  
			,a.auth_event_created_ts as curr_auth_ts
			,rae.auth_event_created_ts as past_auth_ts
			,case when rae.response_cd in ('00', '10') then 1 else 0 end as approved
			,case when d.dispute_created_at is not null then 1 else 0 end as dispute_fraud

			FROM identifier(:driver_table) a
			left join edw_db.core.fct_realtime_auth_event rae on (a.user_id=rae.user_id and a.auth_event_merchant_name_raw=rae.auth_event_merchant_name_raw and rae.auth_event_created_ts between dateadd(day,-7,a.auth_event_created_ts) and dateadd(second,-1,a.auth_event_created_ts))
		  	LEFT JOIN edw_db.core.fct_realtime_auth_event dual_auth ON rae.auth_id = dual_auth.original_auth_id AND rae.user_id = dual_auth.user_id
		  	LEFT JOIN risk.prod.disputed_transactions d on (rae.auth_id = d.authorization_code or dual_auth.auth_id = d.authorization_code) and rae.user_id = d.user_id and d.dispute_created_at<a.auth_event_created_ts
		           

		  	WHERE 1=1
		    and rae.original_auth_id = 0
		    AND rae.req_amt < 0
    )
    	SELECT auth_event_id,
            min(datediff(hour,past_auth_ts,curr_auth_ts)) as min__mrch_uid_hourgap_p7d,
            sum(amt) as sum__mrch_uid_txn_p7d,
            count(auth_event_id) as count__mrch_uid_txn_p7d,
            sum(case when approved=1 then amt else 0 end) as sum__mrch_uid_appv_txn_p7d,
            count(case when approved=1 then auth_event_id end) as count__mrch_uid_appv_txn_p7d,
            sum(case when dispute_fraud=1 then amt else 0 end) as sum__mrch_uid_disp_txn_p7d,
            count(case when dispute_fraud=1 then auth_event_id end) as count__mrch_uid_disp_txn_p7d,
            sum__mrch_uid_appv_txn_p7d/nullifzero(sum__mrch_uid_txn_p7d) as ratio__mrch_uid_dlc_txn_sum_p7d,
            count__mrch_uid_appv_txn_p7d/nullifzero(count__mrch_uid_txn_p7d) as ratio__mrch_uid_dlc_txn_cnt_p7d,
            sum__mrch_uid_disp_txn_p7d/nullifzero(sum__mrch_uid_appv_txn_p7d) as ratio__mrch_uid_disp_txn_sum_p7d,
            count__mrch_uid_disp_txn_p7d/nullifzero(count__mrch_uid_appv_txn_p7d) as ratio__mrch_uid_disp_txn_cnt_p7d
            
        FROM t1
	  	GROUP BY 1
	);




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
mcc user_id tran vel

key: auth event key

https://github.com/1debit/ml-feature-library/blob/main/chime_ml/feature_library/features/mcc_user_id__processing_transact_vel.sql
*/  

	let tbl_mcc_uid_vel varchar := staging_prefix||'_mcc_uid_vel';

	create or replace table identifier(:tbl_mcc_uid_vel) as(
		with t1 as( 
	        select 
			a.auth_event_id
			,-1*rae.req_amt as amt  
			,a.auth_event_created_ts as curr_auth_ts
			,rae.auth_event_created_ts as past_auth_ts
			,case when rae.response_cd in ('00', '10') then 1 else 0 end as approved
			,case when d.dispute_created_at is not null then 1 else 0 end as dispute_fraud

			FROM identifier(:driver_table) a
			left join edw_db.core.fct_realtime_auth_event rae on (a.user_id=rae.user_id and a.mcc_cd=rae.mcc_cd and rae.auth_event_created_ts between dateadd(day,-3,a.auth_event_created_ts) and dateadd(second,-1,a.auth_event_created_ts))
		  	LEFT JOIN edw_db.core.fct_realtime_auth_event dual_auth ON rae.auth_id = dual_auth.original_auth_id AND rae.user_id = dual_auth.user_id
		  	LEFT JOIN risk.prod.disputed_transactions d on (rae.auth_id = d.authorization_code or dual_auth.auth_id = d.authorization_code) and rae.user_id = d.user_id and d.dispute_created_at<a.auth_event_created_ts
		            

		  	WHERE rae.original_auth_id = 0
		    AND rae.req_amt < 0
		    AND rae.response_cd <> '85'
	    )
	    	SELECT auth_event_id,
		        sum(amt) as sum__mccuid_transactions_p3d,
		        count(auth_event_id) as count__mccuid_transactions_p3d,
		        sum(case when dispute_fraud=0 and approved=1 then amt else 0 end) as sum__mccuid_good_txn_p3d,
		        count(case when dispute_fraud=0 and approved=1 then auth_event_id else null end) as count__mccuid_good_txn_p3d,
		        sum__mccuid_good_txn_p3d/nullifzero(sum__mccuid_transactions_p3d) as ratio_sum__mccuid_good_txn_p3d,
		        count__mccuid_good_txn_p3d/nullifzero(count__mccuid_transactions_p3d) as ratio_count__mccuid_good_txn_p3d,
		        sum(case when dispute_fraud = 1 and approved = 1 then amt else 0 end) as sum__mccuid_fraud_gross_p3d,
		        count(case when dispute_fraud= 1 and approved=1 then auth_event_id else null end) as count__mccuid_fraud_gross_p3d,

		        sum(case when past_auth_ts>=dateadd(day,-1,curr_auth_ts) then amt else 0 end) as sum__mccuid_transactions_p1d,
		        count(case when past_auth_ts>=dateadd(day,-1,curr_auth_ts) then auth_event_id end) as count__mccuid_transactions_p1d,
		        sum(case when past_auth_ts>=dateadd(day,-1,curr_auth_ts) and dispute_fraud=0 and approved=1 then amt else 0 end) as sum__mccuid_good_txn_p1d,
		        count(case when past_auth_ts>=dateadd(day,-1,curr_auth_ts) and dispute_fraud=0 and approved=1 then auth_event_id else null end) as count__mccuid_good_txn_p1d,
		        sum__mccuid_good_txn_p1d/nullifzero(sum__mccuid_transactions_p1d) as ratio_sum__mccuid_good_txn_p1d,
		        count__mccuid_good_txn_p1d/nullifzero(count__mccuid_transactions_p1d) as ratio_count__mccuid_good_txn_p1d,
		        sum(case when past_auth_ts>=dateadd(day,-1,curr_auth_ts) and dispute_fraud = 1 and approved = 1 then amt else 0 end) as sum__mccuid_fraud_gross_p1d,
		        count(case when past_auth_ts>=dateadd(day,-1,curr_auth_ts) and dispute_fraud= 1 and approved=1 then auth_event_id else null end) as count__mccuid_fraud_gross_p1d,
		  		
		  		sum(case when past_auth_ts>=dateadd(hour,-2,curr_auth_ts) then amt else 0 end) as sum__mccuid_transactions_p2h,
		        count(case when past_auth_ts>=dateadd(hour,-2,curr_auth_ts) then auth_event_id end) as count__mccuid_transactions_p2h,
		        sum(case when past_auth_ts>=dateadd(hour,-2,curr_auth_ts) and dispute_fraud=0 and approved=1 then amt else 0 end) as sum__mccuid_good_txn_p2h,
		        count(case when past_auth_ts>=dateadd(hour,-2,curr_auth_ts) and dispute_fraud=0 and approved=1 then auth_event_id else null end) as count__mccuid_good_txn_p2h,
		        sum__mccuid_good_txn_p2h/nullifzero(sum__mccuid_transactions_p2h) as ratio_sum__mccuid_good_txn_p2h,
		        count__mccuid_good_txn_p2h/nullifzero(count__mccuid_transactions_p2h) as ratio_count__mccuid_good_txn_p2h,
		        sum(case when past_auth_ts>=dateadd(hour,-1,curr_auth_ts) and dispute_fraud = 1 and approved = 1 then amt else 0 end) as sum__mccuid_fraud_gross_p2h,
		        count(case when past_auth_ts>=dateadd(hour,-2,curr_auth_ts) and dispute_fraud= 1 and approved=1 then auth_event_id else null end) as count__mccuid_fraud_gross_p2h

		  	FROM t1
		  	GROUP BY 1
	);


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
pii chg: phone email by user or agent/users

key: auth_event_id

with feature store features simulated:
    user_id__pii_update__0s__7d__v1___count__phone_change
    user_id__pii_update__0s__7d__v1___count__email_change

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__pii_update/v1.sql
*/

	let tbl_pii varchar := staging_prefix||'_pii_change';

	create or replace table identifier(:tbl_pii) as(
	    
	    select 
	    a.auth_event_id
	    
	    ,count(distinct case when event = 'in_app_email_address_update' then event_ts end) as count__email_change_by_user_p30
	    ,count(distinct case when event = 'in_app_phone_number_update' then event_ts end) as count__phone_change_by_user_p30
	    ,count(distinct case when event = 'email' then event_ts end) as count__email_change_p30
	    ,count(distinct case when event = 'phone' then event_ts end) as count__phone_change_p30
	    
	    ,count(distinct case when b.event_ts >= dateadd(day,-7,a.auth_event_created_ts) and event = 'in_app_email_address_update' then event_ts end) as count__email_change_by_user_p7d
	    ,count(distinct case when b.event_ts >= dateadd(day,-7,a.auth_event_created_ts) and event = 'in_app_phone_number_update' then event_ts end) as count__phone_change_by_user_p7d
	    ,count(distinct case when b.event_ts >= dateadd(day,-7,a.auth_event_created_ts) and event = 'email' then event_ts end) as count__email_change_p7d
	    ,count(distinct case when b.event_ts >= dateadd(day,-7,a.auth_event_created_ts) and event = 'phone' then event_ts end) as count__phone_change_p7d
	    
	    ,count(distinct case when b.event_ts >= dateadd(day,-3,a.auth_event_created_ts) and event = 'in_app_email_address_update' then event_ts end) as count__email_change_by_user_p3d
	    ,count(distinct case when b.event_ts >= dateadd(day,-3,a.auth_event_created_ts) and event = 'in_app_phone_number_update' then event_ts end) as count__phone_change_by_user_p3d
	    ,count(distinct case when b.event_ts >= dateadd(day,-3,a.auth_event_created_ts) and event = 'email' then event_ts end) as count__email_change_p3d
	    ,count(distinct case when b.event_ts >= dateadd(day,-3,a.auth_event_created_ts) and event = 'phone' then event_ts end) as count__phone_change_p3d
	    
	    ,count(distinct case when b.event_ts >= dateadd(day,-2,a.auth_event_created_ts) and event = 'in_app_email_address_update' then event_ts end) as count__email_change_by_user_p2d
	    ,count(distinct case when b.event_ts >= dateadd(day,-2,a.auth_event_created_ts) and event = 'in_app_phone_number_update' then event_ts end) as count__phone_change_by_user_p2d
	    ,count(distinct case when b.event_ts >= dateadd(day,-2,a.auth_event_created_ts) and event = 'email' then event_ts end) as count__email_change_p2d
	    ,count(distinct case when b.event_ts >= dateadd(day,-2,a.auth_event_created_ts) and event = 'phone' then event_ts end) as count__phone_change_p2d
	    
	    ,count(distinct case when b.event_ts >= dateadd(day,-1,a.auth_event_created_ts) and event = 'in_app_email_address_update' then event_ts end) as count__email_change_by_user_p1d
	    ,count(distinct case when b.event_ts >= dateadd(day,-1,a.auth_event_created_ts) and event = 'in_app_phone_number_update' then event_ts end) as count__phone_change_by_user_p1d
	    ,count(distinct case when b.event_ts >= dateadd(day,-1,a.auth_event_created_ts) and event = 'email' then event_ts end) as count__email_change_p1d
	    ,count(distinct case when b.event_ts >= dateadd(day,-1,a.auth_event_created_ts) and event = 'phone' then event_ts end) as count__phone_change_p1d
	    
	    ,count(distinct case when b.event_ts >= dateadd(hour,-1,a.auth_event_created_ts) and event = 'in_app_email_address_update' then event_ts end) as count__email_change_by_user_p1h
	    ,count(distinct case when b.event_ts >= dateadd(hour,-1,a.auth_event_created_ts) and event = 'in_app_phone_number_update' then event_ts end) as count__phone_change_by_user_p1h
	    ,count(distinct case when b.event_ts >= dateadd(hour,-1,a.auth_event_created_ts) and event = 'email' then event_ts end) as count__email_change_p1h
	    ,count(distinct case when b.event_ts >= dateadd(hour,-1,a.auth_event_created_ts) and event = 'phone' then event_ts end) as count__phone_change_p1h
	    
	    ,count(distinct case when b.event_ts >= dateadd(minute,-5,a.auth_event_created_ts) and event = 'in_app_email_address_update' then event_ts end) as count__email_change_by_user_p5m
	    ,count(distinct case when b.event_ts >= dateadd(minute,-5,a.auth_event_created_ts) and event = 'in_app_phone_number_update' then event_ts end) as count__phone_change_by_user_p5m
	    ,count(distinct case when b.event_ts >= dateadd(minute,-5,a.auth_event_created_ts) and event = 'email' then event_ts end) as count__email_change_p5m
	    ,count(distinct case when b.event_ts >= dateadd(minute,-5,a.auth_event_created_ts) and event = 'phone' then event_ts end) as count__phone_change_p5m
	    
	    from identifier(:driver_table) a
	    left join 
	    (
	        select a.*
	         from(
	             select user_id::number as user_id, convert_timezone('America/Los_Angeles', timestamp) as event_ts, 'in_app_email_address_update' as event from segment.chime_prod.email_address_updated where success = 1
	                union 
	             select user_id::number as user_id, convert_timezone('America/Los_Angeles', timestamp) as event_ts, 'in_app_phone_number_update' as event from segment.chime_prod.update_phone_number where success = 1
	                union 
	             select item_id::number as user_id, convert_timezone('America/Los_Angeles', created_at) as event_ts, object as event from analytics.looker.versions_pivot where item_type = 'User'
	             ) a
	        where 1=1
	        and a.user_id is not null and event_ts is not null and event is not null
	        
	    ) b on (a.user_id=b.user_id and b.event_ts between dateadd(day,-30,a.auth_event_created_ts) and a.auth_event_created_ts)
	    where 1=1
	    group by 1

	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 
atom score 

key: auth_event_id

with feature store features simulated: 
    user_id__atom_score__0s__2h__v1___nunique__device_ids 
    user_id__atom_score__0s__2h__v1___max__atom_score 

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__atom_score/v2.sql
*/

	let tbl_atom varchar := staging_prefix||'_atom';

	create or replace table identifier(:tbl_atom)  as(
		select a.auth_event_id
		,max(b.session_timestamp) as max_atom_score_ts
		,max(score) as max_atom_score_p30
		,max(case when b.session_timestamp>=dateadd(day,-3,a.auth_event_created_ts) then score end) as max_atom_score_p3d
		,max(case when b.session_timestamp>=dateadd(day,-1,a.auth_event_created_ts) then score end) as max_atom_score_p1d
		,max(case when b.session_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) then score end) as max_atom_score_p2h
		    
		,count(distinct b.device_id) as cnt_dist_dvc_p30
		,count(distinct case when b.session_timestamp>=dateadd(day,-3,a.auth_event_created_ts) then b.device_id end) as cnt_dist_dvc_p3d
		,count(distinct case when b.session_timestamp>=dateadd(day,-1,a.auth_event_created_ts) then b.device_id end) as cnt_dist_dvc_p1d
		,count(distinct case when b.session_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) then b.device_id end) as cnt_dist_dvc_p2h
		    
		    from identifier(:driver_table) a
		    left join ml.model_inference.ato_login_alerts b on (a.user_id=b.user_id and b.session_timestamp between dateadd(day,-30,a.auth_event_created_ts) and a.auth_event_created_ts)
		    where 1=1
		    and score<>0
		    group by 1
	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
app event usage info: application/cta dvc related login dvc/tz/carrier etc.

key: auth_event_id

with feature store features simulated: 
    user_id__usage__2d__7d__v1___nunique__device_ids
    user_id__usage__2d__7d__v1___nunique__ips 


https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__usage/v2.sql
*/
	
	let tbl_login_hist varchar := staging_prefix||'_login_hist';

	create or replace table identifier(:tbl_login_hist) as(
		select 
		a.auth_event_id
		/*look back 30 days*/ 
		,count(distinct device_id) as nunique__device_ids_p30
		,count(distinct ip) as nunique__ips_p30
		,count(distinct network_carrier) as nunique__network_carriers_p30
		,count(distinct timezone) as nunique__timezones_p30
		,count(distinct os_name) as nunique__os_versions_p30
		,count(distinct intnl_network_carrier) as nunique__intnl_network_carrier_p30
		,count(distinct africa_network_carriers) as nunique__africa_network_carriers_p30
		,count(distinct africa_timezones) as nunique__africa_timezones_p30
		    
		/*look back 7 days*/  
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then device_id end) as nunique__device_ids_p7d
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then ip end) as nunique__ips_p7d
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then network_carrier end) as nunique__network_carriers_p7d
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then timezone end) as nunique__timezones_p7d
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then os_name end) as nunique__os_versions_p7d
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then intnl_network_carrier end) as nunique__intnl_network_carrier_p7d
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then africa_network_carriers end) as nunique__africa_network_carriers_p7d
		,count(distinct case when b.session_timestamp >= dateadd(day,-7,a.auth_event_created_ts) then africa_timezones end) as nunique__africa_timezones_p7d
		    
		/*look back 2 days*/       
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then device_id end) as nunique__device_ids_p2d
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then ip end) as nunique__ips_p2d
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then network_carrier end) as nunique__network_carriers_p2d
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then timezone end) as nunique__timezones_p2d
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then os_name end) as nunique__os_versions_p2d
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then intnl_network_carrier end) as nunique__intnl_network_carrier_p2d
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then africa_network_carriers end) as nunique__africa_network_carriers_p2d
		,count(distinct case when b.session_timestamp >= dateadd(day,-2,a.auth_event_created_ts) then africa_timezones end) as nunique__africa_timezones_p2d
		/*look back 1 days*/       
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then device_id end) as nunique__device_ids_p1d
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then ip end) as nunique__ips_p1d
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then network_carrier end) as nunique__network_carriers_p1d
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then timezone end) as nunique__timezones_p1d
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then os_name end) as nunique__os_versions_p1d
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then intnl_network_carrier end) as nunique__intnl_network_carrier_p1d
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then africa_network_carriers end) as nunique__africa_network_carriers_p1d
		,count(distinct case when b.session_timestamp >= dateadd(day,-1,a.auth_event_created_ts) then africa_timezones end) as nunique__africa_timezones_p1d
		/*look back 2 hour*/       
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then device_id end) as nunique__device_ids_p2h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then ip end) as nunique__ips_p2h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then network_carrier end) as nunique__network_carriers_p2h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then timezone end) as nunique__timezones_p2h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then os_name end) as nunique__os_versions_p2h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then intnl_network_carrier end) as nunique__intnl_network_carrier_p2h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then africa_network_carriers end) as nunique__africa_network_carriers_p2h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-2,a.auth_event_created_ts) then africa_timezones end) as nunique__africa_timezones_p2h
		/*look back 1 days*/       
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then device_id end) as nunique__device_ids_p1h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then ip end) as nunique__ips_p1h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then network_carrier end) as nunique__network_carriers_p1h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then timezone end) as nunique__timezones_p1h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then os_name end) as nunique__os_versions_p1h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then intnl_network_carrier end) as nunique__intnl_network_carrier_p1h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then africa_network_carriers end) as nunique__africa_network_carriers_p1h
		,count(distinct case when b.session_timestamp >= dateadd(hour,-1,a.auth_event_created_ts) then africa_timezones end) as nunique__africa_timezones_p1h
		 /*look back 5 mins*/       
		,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then device_id end) as nunique__device_ids_p5m
		,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then ip end) as nunique__ips_p5m
		,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then network_carrier end) as nunique__network_carriers_p5m
		,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then timezone end) as nunique__timezones_p5m
		,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then os_name end) as nunique__os_versions_p5m
		,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then intnl_network_carrier end) as nunique__intnl_network_carrier_p5m
		,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then africa_network_carriers end) as nunique__africa_network_carriers_p5m

		    from identifier(:driver_table) a
		    left join (
		                select user_id, timestamp as session_timestamp, device_id, network_carrier, os_name, ip, timezone /*, platform, zip_code */
		                ,case when lower(network_carrier) not in
		                          ('t-mobile', 'at&t', 'metro by t-mobile', 'verizon', 'boost mobile', 'null', 'cricket',
		                           'tfw', 'sprint', '','metro', 'boost', 'home','spectrum', 'xfinity mobile', 'verizon wireless', 'assurance wireless',
		                           'u.s. cellular', 'metropcs','carrier', 'google fi') then network_carrier end as intnl_network_carrier
		                ,case when lower(network_carrier) like any ('%mtn%','%airtel%','%glo%','%9mobile%','%stay safe%','%besafe%', '% ng%','%nigeria%','%etisalat%') 
		                           and lower(network_carrier) not in ('roaming indicator off', 'searching for service') 
		                           then network_carrier end as africa_network_carriers
		                ,case when lower(timezone) like '%africa%' then timezone end as africa_timezones
		                from edw_db.feature_store.atom_app_events_v2
		        
		                
		              ) b on (a.user_id=b.user_id and b.session_timestamp between dateadd(day,-30,a.auth_event_created_ts) and a.auth_event_created_ts)
		    where 1=1
		    group by 1
	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
button tapped

key: auth_event_id

with feature store features simulated:
    user_id__app_actions_prod_events__0s__2h__v1___count__event_log_in_button_tapped 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_login_failed 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_sign_out_button_tapped 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_admin_button_clicked 

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__app_actions_prod_events/v2.sql
*/

	let tbl_app_action varchar := staging_prefix||'_app_action';

	create or replace table identifier(:tbl_app_action) as(
	    
	    select 
	    a.auth_event_id
	    
	    ,max(event_timestamp) as last__tapped_timestamp
	    ,max(case when event = 'log_in_button_tapped' then event_timestamp end) as last__event_log_in_button_tapped_timestamp
	    ,max(case when event = 'login_failed' then event_timestamp end) as last__event_login_failed_timestamp
	    ,max(case when event = 'sign_out_button_tapped' then event_timestamp end) as last__event_sign_out_button_tapped_timestamp
	    ,max(case when event = 'admin_button_clicked' then event_timestamp end) as last__event_admin_button_clicked_timestamp
	    
	    ,coalesce(count( distinct case when event = 'log_in_button_tapped' then id end),0) as count__event_log_in_button_tapped_p28
	    ,coalesce(count( distinct case when event = 'login_failed' then id end),0) as count__event_login_failed_p28
	    ,coalesce(count( distinct case when event = 'sign_out_button_tapped' then id end),0) as count__event_sign_out_button_tapped_p28
	    ,coalesce(count( distinct case when event = 'admin_button_clicked' then id end),0) as count__event_admin_button_clicked_p28
	    
	    ,coalesce(count( distinct case when b.event_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'log_in_button_tapped' then id end),0) as count__event_log_in_button_tapped_p2h
	    ,coalesce(count( distinct case when b.event_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'login_failed' then id end),0) as count__event_login_failed_p2h
	    ,coalesce(count( distinct case when b.event_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'sign_out_button_tapped' then id end),0) as count__event_sign_out_button_tapped_p2h
	    ,coalesce(count( distinct case when b.event_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'admin_button_clicked' then id end),0) as count__event_admin_button_clicked_p2h
	    from identifier(:driver_table) a
	    left join 
	    (
	        select a.*
	         from(
	             select user_id, id, received_at as event_timestamp, event  from segment.chime_prod.log_in_button_tapped
	             union all
	             select user_id, id, received_at as event_timestamp, event  from segment.chime_prod.login_failed
	             union all
	             select user_id, id, received_at as event_timestamp, event  from segment.chime_prod.sign_out_button_tapped
	             union all
	             select user_id, id, received_at as event_timestamp, event  from segment.chime_prod.admin_button_clicked
	         ) a
	        where 1=1
	        and a.event is not null and a.event_timestamp is not null and try_to_number(a.user_id) is not null
	        
	    ) b on (a.user_id=try_to_number(b.user_id) and b.event_timestamp between dateadd(day,-28,a.auth_event_created_ts) and a.auth_event_created_ts)
	    where 1=1
	    group by 1

	);


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
app screen views

key: auth_event_id

with feature store features simulated:
    user_id__app_screen_views

https://github.com/1debit/ml-feature-library/blob/main/chime_ml/feature_library/features/user_id__app_screen_views.sql
*/

	let tbl_app_screen_views varchar := staging_prefix||'_app_screen_views';

	create or replace table identifier(:tbl_app_screen_views) as(
    
    select 
    a.auth_event_id,
    /*past 7 days*/
    SUM(CASE WHEN name='Home' THEN 1 ELSE 0 END) as count__home_views_p7d,
    SUM(CASE WHEN name='Settings' THEN 1 ELSE 0 END) as count__setting_views_p7d,
    SUM(CASE WHEN name in ('Capture Check',
                           'Check Amount',
                           'Checkbook',
                           'Credit Paycheck Refill Activation Error',
                           'Enable Paycheck Refill',
                           'Mailed Check History') THEN 1 ELSE 0 END) as count__check_views_p7d,
    SUM(CASE WHEN name = 'Direct Deposit' THEN 1 ELSE 0 END) as count__dd_views_p7d,
    SUM(CASE WHEN name='ATM Map' THEN 1 ELSE 0 END) as count__atmmap_views_p7d,
    SUM(CASE WHEN name='Spending Account' THEN 1 ELSE 0 END) as count__spendingacct_views_p7d,
    SUM(CASE WHEN name='Edit Personal Information' THEN 1 ELSE 0 END) as count__editpersinfo_views_p7d,
    SUM(CASE WHEN name in ('Edit Address',
                            'Edit Email',
                            'Edit Password',
                            'Edit Personal Information',
                            'Edit Phone',
                            'Edit Phone Verification',
                            'Edit Preferred Name',
                            'Edit Recipient',
                            'Edit Username') THEN 1 ELSE 0 END) as count__editinfo_views_p7d,
    SUM(CASE WHEN name in ('Card Replacement Reason') THEN 1 ELSE 0 END) as count__cardrep_views_p7d,
    SUM(CASE WHEN name in ( 'New Support Request',
                           'Support Request') THEN 1 ELSE 0 END) as count__suppreq_views_p7d,
    SUM(CASE WHEN name in ('Transfer Funds',
                            'Transfer Funds Loading') THEN 1 ELSE 0 END) as count__trns_funds_views_p7d,
    SUM(CASE WHEN name = 'Move Money' THEN 1 ELSE 0 END) as count__movemoney_views_p7d,
    SUM(CASE WHEN name in  ('Pay Friends Activity Details',
                            'Pay Friends Activity Feed',
                            'Pay Friends Payment Details',
                            'Pay Friends Transfers') THEN 1 ELSE 0 END) as count__pf_views_p7d,
    SUM(CASE WHEN name in  ('Credit Builder ATM Intro',
                            'Credit Builder Account',
                            'Credit Builder Card Activated',
                            'Credit Builder Context',
                            'Credit Builder Digital Card',
                            'Credit Builder Verify Your Identity',
                            'Credit Card Replacement Confirmation',
                            'Credit Card Replacement Reason',
                            'Credit Enrollment Error',
                            'Credit Enrollment Info Page',
                            'Credit Enrollment Terms and Conditions',
                            'Unlock Credit Builder')  THEN 1 ELSE 0 END) as count__cb_views_p7d,
    SUM(CASE WHEN name in  ('Enroll in SpotMe',
                            'Enroll in SpotMe Education',
                            'SpotMe Boosts Education',
                            'SpotMe Boosts Select Friend',
                            'SpotMe FAQ',
                            'SpotMe Terms & Conditions') THEN 1 ELSE 0 END) as count__spotme_views_p7d,
    SUM(CASE WHEN name='Confirm Address' THEN 1 ELSE 0 END) as count__confadd_views_p7d,
    SUM(CASE WHEN name='My Temporary Card' THEN 1 ELSE 0 END) as count__tempcard_views_p7d,
    SUM(CASE WHEN name='Edit Email' THEN 1 ELSE 0 END) as count__editemail_views_p7d,
    SUM(CASE WHEN name='Edit Phone' THEN 1 ELSE 0 END) as count__editphone_views_p7d,
    SUM(CASE WHEN name='Edit Phone Verification' THEN 1 ELSE 0 END) AS count__editphonever_views_p7d,
    SUM(CASE WHEN name='Set Your Limit' THEN 1 ELSE 0 END) AS count__setlimit_views_p7d,
    SUM(CASE WHEN name='2FA Verification Code' THEN 1 ELSE 0 END) AS count__2fa_views_p7d,
    SUM(CASE WHEN name='Transfer Account Selection' THEN 1 ELSE 0 END) AS count__transfer_acct_views_p7d,
    SUM(CASE WHEN name='Declined Transaction Details' THEN 1 ELSE 0 END) AS count__declined_trx_views_p7d,
    SUM(CASE WHEN name in ( 'Dispute',
                            'Dispute Action Success',
                            'Dispute Filed Error',
                            'Dispute Filed New Card',
                            'Dispute Filed Success',
                            'Dispute Question',
                            'Dispute Rebuttal Explanation',
                            'Dispute Request Documents',
                            'Dispute Transaction Picker',
                            'Disputes')  THEN 1 ELSE 0 END) AS count__dispute_views_p7d,
    
    
    /*past 2 hours*/
    SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Home' THEN 1 ELSE 0 END) as count__home_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Settings' THEN 1 ELSE 0 END) as count__setting_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in ('Capture Check',
                               'Check Amount',
                               'Checkbook',
                               'Credit Paycheck Refill Activation Error',
                               'Enable Paycheck Refill',
                               'Mailed Check History') THEN 1 ELSE 0 END) as count__check_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name = 'Direct Deposit' THEN 1 ELSE 0 END) as count__dd_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='ATM Map' THEN 1 ELSE 0 END) as count__atmmap_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Spending Account' THEN 1 ELSE 0 END) as count__spendingacct_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Edit Personal Information' THEN 1 ELSE 0 END) as count__editpersinfo_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in ('Edit Address',
                                'Edit Email',
                                'Edit Password',
                                'Edit Personal Information',
                                'Edit Phone',
                                'Edit Phone Verification',
                                'Edit Preferred Name',
                                'Edit Recipient',
                                'Edit Username') THEN 1 ELSE 0 END) as count__editinfo_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in ('Card Replacement Reason') THEN 1 ELSE 0 END) as count__cardrep_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in ( 'New Support Request',
                               'Support Request') THEN 1 ELSE 0 END) as count__suppreq_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in ('Transfer Funds',
                                'Transfer Funds Loading') THEN 1 ELSE 0 END) as count__trns_funds_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name = 'Move Money' THEN 1 ELSE 0 END) as count__movemoney_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in  ('Pay Friends Activity Details',
                                'Pay Friends Activity Feed',
                                'Pay Friends Payment Details',
                                'Pay Friends Transfers') THEN 1 ELSE 0 END) as count__pf_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in  ('Credit Builder ATM Intro',
                                'Credit Builder Account',
                                'Credit Builder Card Activated',
                                'Credit Builder Context',
                                'Credit Builder Digital Card',
                                'Credit Builder Verify Your Identity',
                                'Credit Card Replacement Confirmation',
                                'Credit Card Replacement Reason',
                                'Credit Enrollment Error',
                                'Credit Enrollment Info Page',
                                'Credit Enrollment Terms and Conditions',
                                'Unlock Credit Builder')  THEN 1 ELSE 0 END) as count__cb_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in  ('Enroll in SpotMe',
                                'Enroll in SpotMe Education',
                                'SpotMe Boosts Education',
                                'SpotMe Boosts Select Friend',
                                'SpotMe FAQ',
                                'SpotMe Terms & Conditions') THEN 1 ELSE 0 END) as count__spotme_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Confirm Address' THEN 1 ELSE 0 END) as count__confadd_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='My Temporary Card' THEN 1 ELSE 0 END) as count__tempcard_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Edit Email' THEN 1 ELSE 0 END) as count__editemail_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Edit Phone' THEN 1 ELSE 0 END) as count__editphone_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Edit Phone Verification' THEN 1 ELSE 0 END) AS count__editphonever_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Set Your Limit' THEN 1 ELSE 0 END) AS count__setlimit_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='2FA Verification Code' THEN 1 ELSE 0 END) AS count__2fa_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Transfer Account Selection' THEN 1 ELSE 0 END) AS count__transfer_acct_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name='Declined Transaction Details' THEN 1 ELSE 0 END) AS count__declined_trx_views_p2h,
        SUM(CASE WHEN b.timestamp>=dateadd(hour,-2,a.auth_event_created_ts) and name in ( 'Dispute',
                                'Dispute Action Success',
                                'Dispute Filed Error',
                                'Dispute Filed New Card',
                                'Dispute Filed Success',
                                'Dispute Question',
                                'Dispute Rebuttal Explanation',
                                'Dispute Request Documents',
                                'Dispute Transaction Picker',
                                'Disputes')  THEN 1 ELSE 0 END) AS count__dispute_views_p2h
    
    from identifier(:driver_table) a
    left join segment.chime_prod.screens b on (a.user_id=try_to_number(b.user_id) and b.timestamp between dateadd(day,-7,a.auth_event_created_ts) and a.auth_event_created_ts)
    where 1=1
    and b.name is not null 
    group by 1

	);


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
blocked device id match

key: auth_event_id

with feature store features simulated:
    user_id__create_session_event

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__create_session_event/v1.sql

*/
	
	let tbl_block_dvc_hist varchar := staging_prefix||'_block_dvc_hist';

	create or replace table identifier(:tbl_block_dvc_hist) as(
	    with t1 as(
	        SELECT  
	            b.auth_event_id,
	            case when bd.device_id is not null then 1 else 0 end as blocked_dvc_used_ind,
	            s.session_timestamp as last_blocked_dvc_ts,
	            datediff(day,s.session_timestamp,b.auth_event_created_ts) as daydiff_blkdvc_auth

	        FROM identifier(:driver_table) b
	        left join edw_db.feature_store.atom_user_sessions_v2 s on (s.user_id=b.user_id and s.session_timestamp<dateadd(hour,-2,b.auth_event_created_ts))
	        left join mysql_db.chime_prod.blocked_device_ids bd on s.device_id = bd.device_id and bd.blocked=1
	        qualify row_number() over (partition by b.auth_event_id, s.device_id order by (select null))=1
	    )
	        select auth_event_id
	        ,max(blocked_dvc_used_ind) as blocked_dvc_used_ind
	        ,max(last_blocked_dvc_ts) as last_blocked_dvc_ts
	        ,min(daydiff_blkdvc_auth) as last_blocked_dvc_auth_daydiff
	            from t1
	            group by 1
	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
debit card link failure count

key: auth_event_id

with feature store features simulated:
    user_id__debit_card_link_failure

https://github.com/1debit/ml_workflows/tree/main/feature_library_v2/src/families/user_id__debit_card_link_failure
*/
	
	let tbl_debit_lnk_failure varchar := staging_prefix||'_debit_lnk_failure';

	create or replace table identifier(:tbl_debit_lnk_failure) as(
	    SELECT  
	        a.auth_event_id
	        ,max(original_timestamp) as last__lnkfail_timestamp_p180
	        ,coalesce(count(distinct(b.id)), 0) as nunique__linked_card_failures_p180       
	        ,coalesce(count(distinct(case when b.original_timestamp>=dateadd(day,-30,a.auth_event_created_ts) then b.id end)), 0) as nunique__linked_card_failures_p30
	        ,coalesce(count(distinct(case when b.original_timestamp>=dateadd(day,-7,a.auth_event_created_ts) then b.id end)), 0) as nunique__linked_card_failures_p7d
	        ,coalesce(count(distinct(case when b.original_timestamp>=dateadd(hour,-72,a.auth_event_created_ts) then b.id end)), 0) as nunique__linked_card_failures_p72h
	        ,coalesce(count(distinct(case when b.original_timestamp>=dateadd(hour,-24,a.auth_event_created_ts) then b.id end)), 0) as nunique__linked_card_failures_p24h
	        ,coalesce(count(distinct(case when b.original_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) then b.id end)), 0) as nunique__linked_card_failures_p2h
	           
	    FROM identifier(:driver_table) a
	    left join segment.move_money_service.debit_card_linking_failed b on (a.user_id=b.user_id and b.original_timestamp between dateadd(day,-180,a.auth_event_created_ts) and a.auth_event_created_ts) 
	    
	    group by 1
	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
check deposit success and return summary

key: auth_event_id

with feature store features simulated:
    user_id__user_check_deposit

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__user_check_deposit/v2.sql
*/
	
	let tbl_check_deposit_hist varchar := staging_prefix||'_check_deposit_hist';

	create or replace table identifier(:tbl_check_deposit_hist) as(
	    SELECT  
	        a.auth_event_id,        
	        MAX(created_at) as last__check_deposit_timestamp,
	        COUNT(CASE WHEN status in ('accepted', 'posted', 'returned') THEN 1 ELSE NULL END) AS count__appr_checks_deposited,
	        SUM(CASE WHEN status in ('accepted', 'posted', 'returned') THEN approved_amount ELSE 0 END) AS sum__appr_check_amt_deposited,
	        COUNT(CASE WHEN status = 'returned' THEN 1 ELSE NULL END) AS count__returned_checks,
	        SUM(CASE WHEN status = 'returned' THEN approved_amount ELSE 0 END) AS sum__returned_check_amt,
	        COUNT(CASE WHEN status = 'rejected' THEN 1 ELSE NULL END) AS count__rejected_checks,
	        SUM(CASE WHEN status = 'rejected' THEN approved_amount ELSE 0 END) AS sum__rejected_check_amt,
	        COUNT(CASE WHEN status = 'posted' THEN 1 ELSE NULL END) as count__posted_check,
	    
	        /*past 15 days*/
	        COUNT(CASE WHEN b.created_at>=dateadd(day,-15,a.auth_event_created_ts) and status in ('accepted', 'posted', 'returned') THEN 1 ELSE NULL END) AS count__appr_checks_deposited_p15,
	        SUM(CASE WHEN b.created_at>=dateadd(day,-15,a.auth_event_created_ts) and status in ('accepted', 'posted', 'returned') THEN approved_amount ELSE 0 END) AS sum__appr_check_amt_deposited_p15,
	        COUNT(CASE WHEN b.created_at>=dateadd(day,-15,a.auth_event_created_ts) and status = 'returned' THEN 1 ELSE NULL END) AS count__returned_checks_p15,
	        SUM(CASE WHEN b.created_at>=dateadd(day,-15,a.auth_event_created_ts) and status = 'returned' THEN approved_amount ELSE 0 END) AS sum__returned_check_amt_p15,
	        COUNT(CASE WHEN b.created_at>=dateadd(day,-15,a.auth_event_created_ts) and status = 'rejected' THEN 1 ELSE NULL END) AS count__rejected_checks_p15,
	        SUM(CASE WHEN b.created_at>=dateadd(day,-15,a.auth_event_created_ts) and status = 'rejected' THEN approved_amount ELSE 0 END) AS sum__rejected_check_amt_p15,
	        COUNT(CASE WHEN b.created_at>=dateadd(day,-15,a.auth_event_created_ts) and status = 'posted' THEN 1 ELSE NULL END) as count__posted_check_p15
	        
	    FROM identifier(:driver_table) a
	    left join MYSQL_DB.CHIME_PROD.user_check_deposits b on (a.user_id=b.user_id and b.created_at<a.auth_event_created_ts)
	    
	    group by 1
	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
realtime auth vel

key: auth_event_id

with feature store features simulated:
    user_id__realtime_auth_vel (as of 2022.10.28, v3 only has 0-2h)

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__realtime_auth_vel/v3.sql
*/

	let tbl_auth_vel_p2d varchar := staging_prefix||'_auth_vel_p2d';

	create or replace table identifier(:tbl_auth_vel_p2d) as(
	select 
	    a.auth_event_id
	    /*past 2 days*/
	    ,SUM(abs(amount)) AS sum__dollar_approved_p2d
	    ,SUM(case when lower(merchant_name) like ('%cash%app%') or lower(merchant_name) like ('%apple%cash%') or mcc in  
	                                      (7801,7995,7995,7800, --GAMBLING MCC's
	                                       4829,5399,6051,7299) --MAY 2022 DISPUTE ATTACK MCCs   
	                                               then abs(amount) else 0 end) AS sum__risky_merchant_spend_p2d
	    ,AVG(ABS(amount)) AS avg__dollar_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' THEN ABS(amount) END) AS avg__debit_dollar_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Chip' THEN id END) AS count__debit_emv_approved_p2d
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS sum__debit_emv_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS avg__debit_emv_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Contactless' THEN id END) AS count__debit_emv_contactless_approved_p2d
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS sum__debit_emv_contactless_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS avg__debit_emv_contactless_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Fallback' THEN id END) AS count__debit_emv_fallback_approved_p2d
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS sum__debit_emv_fallback_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS avg__debit_emv_fallback_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Magnetic Stripe' THEN id END) AS count__debit_magstripe_approved_p2d
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS sum__debit_magstripe_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS avg__debit_magstripe_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Contactless' THEN id END) AS count__debit_contactless_approved_p2d
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS sum__debit_contactless_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS avg__debit_contactless_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN id END) AS count__debit_cnp_approved_p2d
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS sum__debit_cnp_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS avg__debit_cnp_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Manual' THEN id END) AS count__debit_manual_approved_p2d
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS sum__debit_manual_approved_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS avg__debit_manual_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' THEN ABS(amount) END) AS avg__credit_dollar_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Chip' THEN id END) AS count__credit_emv_approved_p2d
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS sum__credit_emv_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS avg__credit_emv_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Contactless' THEN id END) AS count__credit_emv_contactless_approved_p2d
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS sum__credit_emv_contactless_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS avg__credit_emv_contactless_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Fallback' THEN id END) AS count__credit_emv_fallback_approved_p2d
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS sum__credit_emv_fallback_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS avg__credit_emv_fallback_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Magnetic Stripe' THEN id END) AS count__credit_magstripe_approved_p2d
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS sum__credit_magstripe_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS avg__credit_magstripe_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Contactless' THEN id END) AS count__credit_contactless_approved_p2d
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS sum__credit_contactless_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS avg__credit_contactless_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN id END) AS count__credit_cnp_approved_p2d
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS sum__credit_cnp_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS avg__credit_cnp_approved_p2d
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Manual' THEN id END) AS count__credit_manual_approved_p2d
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS sum__credit_manual_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS avg__credit_manual_approved_p2d
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN visa_risk_score END) AS avg__credit_cnp_approved_vrs_p2d
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN visa_risk_score END) AS avg__debit_cnp_approved_vrs_p2d
	     
	    from identifier(:driver_table) a
	    left join (
	                select user_id,timestamp,account_type,entry_type,amount,merchant_name,mcc,id,visa_risk_score               
	                from segment.chime_prod.realtime_auth
	                where 1=1
	                and amount < 0.00
	                and response_code='approved'
	                and mti in ('0100','0200','0400')
	                and visa_risk_score != 'None'
	    ) b on (a.user_id=b.user_id and b.timestamp between dateadd(day,-2,a.auth_event_created_ts) and a.auth_event_created_ts)
	    where 1=1
	    group by 1
	);

	

	let tbl_auth_vel_p2h varchar := staging_prefix||'_auth_vel_p2h';

	create or replace table identifier(:tbl_auth_vel_p2h) as(
	select 
	    a.auth_event_id
	    /*past 30 days*/
	    ,SUM(abs(amount)) AS sum__dollar_approved_p2h
	    ,SUM(case when lower(merchant_name) like ('%cash%app%') or lower(merchant_name) like ('%apple%cash%') or mcc in  
	                                      (7801,7995,7995,7800, --GAMBLING MCC's
	                                       4829,5399,6051,7299) --MAY 2022 DISPUTE ATTACK MCCs   
	                                               then abs(amount) else 0 end) AS sum__risky_merchant_spend_p2h
	    ,AVG(ABS(amount)) AS avg__dollar_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' THEN ABS(amount) END) AS avg__debit_dollar_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Chip' THEN id END) AS count__debit_emv_approved_p2h
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS sum__debit_emv_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS avg__debit_emv_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Contactless' THEN id END) AS count__debit_emv_contactless_approved_p2h
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS sum__debit_emv_contactless_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS avg__debit_emv_contactless_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Fallback' THEN id END) AS count__debit_emv_fallback_approved_p2h
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS sum__debit_emv_fallback_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS avg__debit_emv_fallback_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Magnetic Stripe' THEN id END) AS count__debit_magstripe_approved_p2h
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS sum__debit_magstripe_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS avg__debit_magstripe_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Contactless' THEN id END) AS count__debit_contactless_approved_p2h
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS sum__debit_contactless_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS avg__debit_contactless_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN id END) AS count__debit_cnp_approved_p2h
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS sum__debit_cnp_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS avg__debit_cnp_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'checking' AND b.entry_type = 'Manual' THEN id END) AS count__debit_manual_approved_p2h
	    ,SUM(CASE WHEN account_type = 'checking' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS sum__debit_manual_approved_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS avg__debit_manual_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' THEN ABS(amount) END) AS avg__credit_dollar_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Chip' THEN id END) AS count__credit_emv_approved_p2h
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS sum__credit_emv_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Chip' THEN ABS(amount) END) AS avg__credit_emv_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Contactless' THEN id END) AS count__credit_emv_contactless_approved_p2h
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS sum__credit_emv_contactless_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Contactless' THEN ABS(amount) END) AS avg__credit_emv_contactless_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Fallback' THEN id END) AS count__credit_emv_fallback_approved_p2h
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS sum__credit_emv_fallback_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'EMV Fallback' THEN ABS(amount) END) AS avg__credit_emv_fallback_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Magnetic Stripe' THEN id END) AS count__credit_magstripe_approved_p2h
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS sum__credit_magstripe_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Magnetic Stripe' THEN ABS(amount) END) AS avg__credit_magstripe_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Contactless' THEN id END) AS count__credit_contactless_approved_p2h
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS sum__credit_contactless_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Contactless' THEN ABS(amount) END) AS avg__credit_contactless_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN id END) AS count__credit_cnp_approved_p2h
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS sum__credit_cnp_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN ABS(amount) END) AS avg__credit_cnp_approved_p2h
	    ,COUNT(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Manual' THEN id END) AS count__credit_manual_approved_p2h
	    ,SUM(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS sum__credit_manual_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Manual' THEN ABS(amount) END) AS avg__credit_manual_approved_p2h
	    ,AVG(CASE WHEN account_type = 'secured_credit' AND b.entry_type = 'Card Not Present' THEN visa_risk_score END) AS avg__credit_cnp_approved_vrs_p2h
	    ,AVG(CASE WHEN account_type = 'checking' AND b.entry_type = 'Card Not Present' THEN visa_risk_score END) AS avg__debit_cnp_approved_vrs_p2h
	     
	    from identifier(:driver_table) a
	    left join (
	                select user_id,timestamp,account_type,entry_type,amount,merchant_name,mcc,id,visa_risk_score               
	                from segment.chime_prod.realtime_auth
	                where 1=1
	                and amount < 0.00
	                and response_code='approved'
	                and mti in ('0100','0200','0400')
	                and visa_risk_score != 'None'
	    ) b on (a.user_id=b.user_id and b.timestamp between dateadd(hour,-2,a.auth_event_created_ts) and a.auth_event_created_ts)
	    where 1=1
	    group by 1
	);


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user dispute history

key: auth_event_id

with feature store features simulated:
    user_id__dispute_to_spend

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__dispute_to_spend/v1.sql
*/

	let tbl_user_disputes varchar := staging_prefix||'_user_disputes';

	create or replace table identifier(:tbl_user_disputes) as(

      SELECT 
      a.auth_event_id,
      max(b.dispute_created_at) as last__userdisp_timestamp,
        
      COUNT(DISTINCT b.user_dispute_claim_id) AS count__claims_p180,
      COUNT(DISTINCT CASE WHEN b.resolution_decision = 'Denied' THEN b.user_dispute_claim_id END) AS count__denied_claims_p180,
      COUNT(DISTINCT b.user_dispute_claim_txn_id) AS count__txns_p180,
      COUNT(DISTINCT CASE WHEN b.resolution_decision = 'Denied' THEN b.user_dispute_claim_txn_id END) AS count__denied_txns_p180,
      SUM(b.transaction_amount) AS sum__txn_amt_p180,
      SUM(CASE WHEN b.resolution_decision = 'Denied' THEN b.transaction_amount else 0 END) AS sum__denied_txn_amt_p180,
        
      COUNT(DISTINCT case when b.dispute_created_at>=dateadd(day,-30,a.auth_event_created_ts) then b.user_dispute_claim_id end) AS count__claims_p30,
      COUNT(DISTINCT CASE WHEN b.dispute_created_at>=dateadd(day,-30,a.auth_event_created_ts) and b.resolution_decision = 'Denied' THEN b.user_dispute_claim_id END) AS count__denied_claims_p30,
      COUNT(DISTINCT case when b.dispute_created_at>=dateadd(day,-30,a.auth_event_created_ts) then b.user_dispute_claim_txn_id end) AS count__txns_p30,
      COUNT(DISTINCT case when b.dispute_created_at>=dateadd(day,-30,a.auth_event_created_ts) and b.resolution_decision = 'Denied' THEN b.user_dispute_claim_txn_id END) AS count__denied_txns_p30,
      SUM(case when b.dispute_created_at>=dateadd(day,-30,a.auth_event_created_ts) then b.transaction_amount else 0 end) AS sum__txn_amt_p30,
      SUM(CASE WHEN b.dispute_created_at>=dateadd(day,-30,a.auth_event_created_ts) and b.resolution_decision = 'Denied' THEN b.transaction_amount else 0 END) AS sum__denied_txn_amt_p30
      
        
	  FROM identifier(:driver_table) a
      left join risk.prod.disputed_transactions b on (a.user_id=b.user_id and b.dispute_created_at between dateadd(day,-180,a.auth_event_created_ts) and a.auth_event_created_ts)
     
      group by 1

	);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user fund loading behavior(dd, cash dep, transfer in etc.)

key: auth_event_id

with feature store features simulated:
    user_id__dispute_to_spend

https://github.com/1debit/ml_workflows/blob/main/feature_library_v2/src/families/user_id__dispute_to_spend/v1.sql
*/

    let tbl_user_funding varchar := staging_prefix||'_user_funding';

	create or replace table identifier(:tbl_user_funding) as(

      SELECT 
      a.auth_event_id,
      min(datediff(day,b.transaction_timestamp,a.auth_event_created_ts)) as min__uid_funding_auth_daydiff,
      sum(case when b.transaction_timestamp>dateadd(day,-7,a.auth_event_created_ts) then 1 else 0 end) as count__uid_funding_p7d,
      
      sum(case when b.transaction_timestamp>dateadd(day,-7,a.auth_event_created_ts) then settled_amt else 0 end) as sum__uid_funding_p7d,    
      sum(case when b.transaction_timestamp>dateadd(day,-7,a.auth_event_created_ts) and b.transaction_cd in ('PMDK','PMDD','PMCN') then settled_amt else 0 end)/nullifzero(sum__uid_funding_p7d) as ratio__uid_ddfund_p7d,
      sum(case when b.transaction_timestamp>dateadd(day,-7,a.auth_event_created_ts) and b.transaction_cd in ('PMGT','PMIQ','PMGO','PMVL','PMRL') then settled_amt else 0 end)/nullifzero(sum__uid_funding_p7d) as ratio__uid_cashfund_p7d,
      sum__uid_funding_p7d/nullifzero(max(b.settled_amt)) as ratio__uid_sumfund_p7d_maxhist
      
	  FROM identifier(:driver_table) a
      left join edw_db.core.fct_settled_transaction b on (a.user_id=b.user_id and b.transaction_timestamp between dateadd(day,-365,a.auth_event_created_ts) and a.auth_event_created_ts and b.settled_amt>0)
     
      group by 1

	);


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

Final EP

*/

	let tbl_final varchar :=output_table;

	create or replace table identifier(:tbl_final) as(
	    select 
	    a.*

	    /*profile*/
	    ,b.na__stated_income
	    ,b.na__email_domain
	    ,b.na__ld_bw_first_name_and_email
	    ,b.na__ld_bw_last_name_and_email
	    ,b.na__ld_bw_full_name_and_email
	    ,b.na__ld_bw_full_name_and_email_first_letter
	    ,b.na__email_ld_norm1
	    ,b.na__email_ld_norm2
	    ,b.na__email_contains_name
	    ,b.na__age
	    ,b.na__is_email_contains_last4_ssn

	    /*pii change*/
	    ,c.count__email_change_by_user_p30
	    ,c.count__phone_change_by_user_p30
	    ,c.count__email_change_p30
	    ,c.count__phone_change_p30
	    ,c.count__email_change_by_user_p7d
	    ,c.count__phone_change_by_user_p7d
	    ,c.count__email_change_p7d
	    ,c.count__phone_change_p7d
	    ,c.count__email_change_by_user_p3d
	    ,c.count__phone_change_by_user_p3d
	    ,c.count__email_change_p3d
	    ,c.count__phone_change_p3d
	    ,c.count__email_change_by_user_p2d
	    ,c.count__phone_change_by_user_p2d
	    ,c.count__email_change_p2d
	    ,c.count__phone_change_p2d
	    ,c.count__email_change_by_user_p1d
	    ,c.count__phone_change_by_user_p1d
	    ,c.count__email_change_p1d
	    ,c.count__phone_change_p1d
	    ,c.count__email_change_by_user_p1h
	    ,c.count__phone_change_by_user_p1h
	    ,c.count__email_change_p1h
	    ,c.count__phone_change_p1h
	    ,c.count__email_change_by_user_p5m
	    ,c.count__phone_change_by_user_p5m
	    ,c.count__email_change_p5m
	    ,c.count__phone_change_p5m

	    /*login hist*/
	    ,d.nunique__device_ids_p30
	    ,d.nunique__ips_p30
	    ,d.nunique__network_carriers_p30
	    ,d.nunique__timezones_p30
	    ,d.nunique__os_versions_p30
	    ,d.nunique__intnl_network_carrier_p30
	    ,d.nunique__africa_network_carriers_p30
	    ,d.nunique__africa_timezones_p30
	    ,d.nunique__device_ids_p7d
	    ,d.nunique__ips_p7d
	    ,d.nunique__network_carriers_p7d
	    ,d.nunique__timezones_p7d
	    ,d.nunique__os_versions_p7d
	    ,d.nunique__intnl_network_carrier_p7d
	    ,d.nunique__africa_network_carriers_p7d
	    ,d.nunique__africa_timezones_p7d
	    ,d.nunique__device_ids_p2d
	    ,d.nunique__ips_p2d
	    ,d.nunique__network_carriers_p2d
	    ,d.nunique__timezones_p2d
	    ,d.nunique__os_versions_p2d
	    ,d.nunique__intnl_network_carrier_p2d
	    ,d.nunique__africa_network_carriers_p2d
	    ,d.nunique__africa_timezones_p2d
	    ,d.nunique__device_ids_p1d
	    ,d.nunique__ips_p1d
	    ,d.nunique__network_carriers_p1d
	    ,d.nunique__timezones_p1d
	    ,d.nunique__os_versions_p1d
	    ,d.nunique__intnl_network_carrier_p1d
	    ,d.nunique__africa_network_carriers_p1d
	    ,d.nunique__africa_timezones_p1d
	    ,d.nunique__device_ids_p2h
	    ,d.nunique__ips_p2h
	    ,d.nunique__network_carriers_p2h
	    ,d.nunique__timezones_p2h
	    ,d.nunique__os_versions_p2h
	    ,d.nunique__intnl_network_carrier_p2h
	    ,d.nunique__africa_network_carriers_p2h
	    ,d.nunique__africa_timezones_p2h
	    ,d.nunique__device_ids_p1h
	    ,d.nunique__ips_p1h
	    ,d.nunique__network_carriers_p1h
	    ,d.nunique__timezones_p1h
	    ,d.nunique__os_versions_p1h
	    ,d.nunique__intnl_network_carrier_p1h
	    ,d.nunique__africa_network_carriers_p1h
	    ,d.nunique__africa_timezones_p1h
	    ,d.nunique__device_ids_p5m
	    ,d.nunique__ips_p5m
	    ,d.nunique__network_carriers_p5m
	    ,d.nunique__timezones_p5m
	    ,d.nunique__os_versions_p5m
	    ,d.nunique__intnl_network_carrier_p5m
	    ,d.nunique__africa_network_carriers_p5m

	    /*atom*/
	    ,e.max_atom_score_ts
	    ,e.max_atom_score_p30
	    ,e.max_atom_score_p3d
	    ,e.max_atom_score_p1d
	    ,e.max_atom_score_p2h
	    ,e.cnt_dist_dvc_p30
	    ,e.cnt_dist_dvc_p3d
	    ,e.cnt_dist_dvc_p1d
	    ,e.cnt_dist_dvc_p2h

	    /*login hist*/
	    ,f.last__tapped_timestamp
	    ,f.last__event_log_in_button_tapped_timestamp
	    ,f.last__event_login_failed_timestamp
	    ,f.last__event_sign_out_button_tapped_timestamp
	    ,f.last__event_admin_button_clicked_timestamp
	    ,f.count__event_log_in_button_tapped_p28
	    ,f.count__event_login_failed_p28
	    ,f.count__event_sign_out_button_tapped_p28
	    ,f.count__event_admin_button_clicked_p28
	    ,f.count__event_log_in_button_tapped_p2h
	    ,f.count__event_login_failed_p2h
	    ,f.count__event_sign_out_button_tapped_p2h
	    ,f.count__event_admin_button_clicked_p2h

	    /*screen view*/
	    ,g.count__home_views_p7d
	    ,g.count__setting_views_p7d
	    ,g.count__check_views_p7d
	    ,g.count__dd_views_p7d
	    ,g.count__atmmap_views_p7d
	    ,g.count__spendingacct_views_p7d
	    ,g.count__editpersinfo_views_p7d
	    ,g.count__editinfo_views_p7d
	    ,g.count__cardrep_views_p7d
	    ,g.count__suppreq_views_p7d
	    ,g.count__trns_funds_views_p7d
	    ,g.count__movemoney_views_p7d
	    ,g.count__pf_views_p7d
	    ,g.count__cb_views_p7d
	    ,g.count__spotme_views_p7d
	    ,g.count__confadd_views_p7d
	    ,g.count__tempcard_views_p7d
	    ,g.count__editemail_views_p7d
	    ,g.count__editphone_views_p7d
	    ,g.count__editphonever_views_p7d
	    ,g.count__setlimit_views_p7d
	    ,g.count__2fa_views_p7d
	    ,g.count__transfer_acct_views_p7d
	    ,g.count__declined_trx_views_p7d
	    ,g.count__dispute_views_p7d
	    ,g.count__home_views_p2h
	    ,g.count__setting_views_p2h
	    ,g.count__check_views_p2h
	    ,g.count__dd_views_p2h
	    ,g.count__atmmap_views_p2h
	    ,g.count__spendingacct_views_p2h
	    ,g.count__editpersinfo_views_p2h
	    ,g.count__editinfo_views_p2h
	    ,g.count__cardrep_views_p2h
	    ,g.count__suppreq_views_p2h
	    ,g.count__trns_funds_views_p2h
	    ,g.count__movemoney_views_p2h
	    ,g.count__pf_views_p2h
	    ,g.count__cb_views_p2h
	    ,g.count__spotme_views_p2h
	    ,g.count__confadd_views_p2h
	    ,g.count__tempcard_views_p2h
	    ,g.count__editemail_views_p2h
	    ,g.count__editphone_views_p2h
	    ,g.count__editphonever_views_p2h
	    ,g.count__setlimit_views_p2h
	    ,g.count__2fa_views_p2h
	    ,g.count__transfer_acct_views_p2h
	    ,g.count__declined_trx_views_p2h
	    ,g.count__dispute_views_p2h

	    /*blocked dvc indicator*/
	    ,h.blocked_dvc_used_ind
	    ,h.last_blocked_dvc_ts
	    ,h.last_blocked_dvc_auth_daydiff

	    /*debit lnk fail*/
	    ,i.last__lnkfail_timestamp_p180
	    ,i.nunique__linked_card_failures_p180
	    ,i.nunique__linked_card_failures_p30
	    ,i.nunique__linked_card_failures_p7d
	    ,i.nunique__linked_card_failures_p72h
	    ,i.nunique__linked_card_failures_p24h
	    ,i.nunique__linked_card_failures_p2h

	    /*return check dept*/
	    ,j.last__check_deposit_timestamp
	    ,j.count__appr_checks_deposited
	    ,j.sum__appr_check_amt_deposited
	    ,j.count__returned_checks
	    ,j.sum__returned_check_amt
	    ,j.count__rejected_checks
	    ,j.sum__rejected_check_amt
	    ,j.count__posted_check
	    ,j.count__appr_checks_deposited_p15
	    ,j.sum__appr_check_amt_deposited_p15
	    ,j.count__returned_checks_p15
	    ,j.sum__returned_check_amt_p15
	    ,j.count__rejected_checks_p15
	    ,j.sum__rejected_check_amt_p15
	    ,j.count__posted_check_p15

	    /*auth vel p2d*/
	    ,k.sum__dollar_approved_p2d
	    ,k.sum__risky_merchant_spend_p2d
	    ,k.avg__dollar_approved_p2d
	    ,k.avg__debit_dollar_approved_p2d
	    ,k.count__debit_emv_approved_p2d
	    ,k.sum__debit_emv_approved_p2d
	    ,k.avg__debit_emv_approved_p2d
	    ,k.count__debit_emv_contactless_approved_p2d
	    ,k.sum__debit_emv_contactless_approved_p2d
	    ,k.avg__debit_emv_contactless_approved_p2d
	    ,k.count__debit_emv_fallback_approved_p2d
	    ,k.sum__debit_emv_fallback_approved_p2d
	    ,k.avg__debit_emv_fallback_approved_p2d
	    ,k.count__debit_magstripe_approved_p2d
	    ,k.sum__debit_magstripe_approved_p2d
	    ,k.avg__debit_magstripe_approved_p2d
	    ,k.count__debit_contactless_approved_p2d
	    ,k.sum__debit_contactless_approved_p2d
	    ,k.avg__debit_contactless_approved_p2d
	    ,k.count__debit_cnp_approved_p2d
	    ,k.sum__debit_cnp_approved_p2d
	    ,k.avg__debit_cnp_approved_p2d
	    ,k.count__debit_manual_approved_p2d
	    ,k.sum__debit_manual_approved_p2d
	    ,k.avg__debit_manual_approved_p2d
	    ,k.avg__credit_dollar_approved_p2d
	    ,k.count__credit_emv_approved_p2d
	    ,k.sum__credit_emv_approved_p2d
	    ,k.avg__credit_emv_approved_p2d
	    ,k.count__credit_emv_contactless_approved_p2d
	    ,k.sum__credit_emv_contactless_approved_p2d
	    ,k.avg__credit_emv_contactless_approved_p2d
	    ,k.count__credit_emv_fallback_approved_p2d
	    ,k.sum__credit_emv_fallback_approved_p2d
	    ,k.avg__credit_emv_fallback_approved_p2d
	    ,k.count__credit_magstripe_approved_p2d
	    ,k.sum__credit_magstripe_approved_p2d
	    ,k.avg__credit_magstripe_approved_p2d
	    ,k.count__credit_contactless_approved_p2d
	    ,k.sum__credit_contactless_approved_p2d
	    ,k.avg__credit_contactless_approved_p2d
	    ,k.count__credit_cnp_approved_p2d
	    ,k.sum__credit_cnp_approved_p2d
	    ,k.avg__credit_cnp_approved_p2d
	    ,k.count__credit_manual_approved_p2d
	    ,k.sum__credit_manual_approved_p2d
	    ,k.avg__credit_manual_approved_p2d
	    ,k.avg__credit_cnp_approved_vrs_p2d
	    ,k.avg__debit_cnp_approved_vrs_p2d

	    /*auth vel p2h*/
	    ,l.sum__dollar_approved_p2h
	    ,l.sum__risky_merchant_spend_p2h
	    ,l.avg__dollar_approved_p2h
	    ,l.avg__debit_dollar_approved_p2h
	    ,l.count__debit_emv_approved_p2h
	    ,l.sum__debit_emv_approved_p2h
	    ,l.avg__debit_emv_approved_p2h
	    ,l.count__debit_emv_contactless_approved_p2h
	    ,l.sum__debit_emv_contactless_approved_p2h
	    ,l.avg__debit_emv_contactless_approved_p2h
	    ,l.count__debit_emv_fallback_approved_p2h
	    ,l.sum__debit_emv_fallback_approved_p2h
	    ,l.avg__debit_emv_fallback_approved_p2h
	    ,l.count__debit_magstripe_approved_p2h
	    ,l.sum__debit_magstripe_approved_p2h
	    ,l.avg__debit_magstripe_approved_p2h
	    ,l.count__debit_contactless_approved_p2h
	    ,l.sum__debit_contactless_approved_p2h
	    ,l.avg__debit_contactless_approved_p2h
	    ,l.count__debit_cnp_approved_p2h
	    ,l.sum__debit_cnp_approved_p2h
	    ,l.avg__debit_cnp_approved_p2h
	    ,l.count__debit_manual_approved_p2h
	    ,l.sum__debit_manual_approved_p2h
	    ,l.avg__debit_manual_approved_p2h
	    ,l.avg__credit_dollar_approved_p2h
	    ,l.count__credit_emv_approved_p2h
	    ,l.sum__credit_emv_approved_p2h
	    ,l.avg__credit_emv_approved_p2h
	    ,l.count__credit_emv_contactless_approved_p2h
	    ,l.sum__credit_emv_contactless_approved_p2h
	    ,l.avg__credit_emv_contactless_approved_p2h
	    ,l.count__credit_emv_fallback_approved_p2h
	    ,l.sum__credit_emv_fallback_approved_p2h
	    ,l.avg__credit_emv_fallback_approved_p2h
	    ,l.count__credit_magstripe_approved_p2h
	    ,l.sum__credit_magstripe_approved_p2h
	    ,l.avg__credit_magstripe_approved_p2h
	    ,l.count__credit_contactless_approved_p2h
	    ,l.sum__credit_contactless_approved_p2h
	    ,l.avg__credit_contactless_approved_p2h
	    ,l.count__credit_cnp_approved_p2h
	    ,l.sum__credit_cnp_approved_p2h
	    ,l.avg__credit_cnp_approved_p2h
	    ,l.count__credit_manual_approved_p2h
	    ,l.sum__credit_manual_approved_p2h
	    ,l.avg__credit_manual_approved_p2h
	    ,l.avg__credit_cnp_approved_vrs_p2h
	    ,l.avg__debit_cnp_approved_vrs_p2h

	    /*user dispute hist*/
	    ,m.last__userdisp_timestamp
	    ,m.count__claims_p180
	    ,m.count__denied_claims_p180
	    ,m.count__txns_p180
	    ,m.count__denied_txns_p180
	    ,m.sum__txn_amt_p180
	    ,m.sum__denied_txn_amt_p180
	    ,m.count__claims_p30
	    ,m.count__denied_claims_p30
	    ,m.count__txns_p30
	    ,m.count__denied_txns_p30
	    ,m.sum__txn_amt_p30
	    ,m.sum__denied_txn_amt_p30

		/*mrch uid vel(beta)*/
        ,n2.min__mrch_uid_hourgap_p390
        ,n2.max__mrch_uid_hourgap_p390
	    ,n2.sum__mrch_uid_txn_p390
		,n2.count__mrch_uid_txn_p390
		,n2.sum__mrch_uid_appv_txn_p390
		,n2.count__mrch_uid_appv_txn_p390
		,n2.sum__mrch_uid_disp_txn_p390
		,n2.count__mrch_uid_disp_txn_p390
		,n2.ratio__mrch_uid_dlc_txn_sum_p390
		,n2.ratio__mrch_uid_dlc_txn_cnt_p390
		,n2.ratio__mrch_uid_disp_txn_sum_p390
		,n2.ratio__mrch_uid_disp_txn_cnt_p390
        
		,n2.sum__mrch_uid_txn_p90
		,n2.count__mrch_uid_txn_p90
		,n2.sum__mrch_uid_appv_txn_p90
		,n2.count__mrch_uid_appv_txn_p90
		,n2.sum__mrch_uid_disp_txn_p90
		,n2.count__mrch_uid_disp_txn_p90
		,n2.ratio__mrch_uid_dlc_txn_sum_p90
		,n2.ratio__mrch_uid_dlc_txn_cnt_p90
		,n2.ratio__mrch_uid_disp_txn_sum_p90
		,n2.ratio__mrch_uid_disp_txn_cnt_p90

        /*mrch uid vel2(beta;0s-7d)*/
        ,n1.min__mrch_uid_hourgap_p7d
	    ,n1.sum__mrch_uid_txn_p7d
		,n1.count__mrch_uid_txn_p7d
		,n1.sum__mrch_uid_appv_txn_p7d
		,n1.count__mrch_uid_appv_txn_p7d
		,n1.sum__mrch_uid_disp_txn_p7d
		,n1.count__mrch_uid_disp_txn_p7d
		,n1.ratio__mrch_uid_dlc_txn_sum_p7d
		,n1.ratio__mrch_uid_dlc_txn_cnt_p7d
		,n1.ratio__mrch_uid_disp_txn_sum_p7d
		,n1.ratio__mrch_uid_disp_txn_cnt_p7d
        
		/*mcc uid vel(beta)*/
		,o.sum__mccuid_transactions_p3d
		,o.count__mccuid_transactions_p3d
		,o.sum__mccuid_good_txn_p3d
		,o.count__mccuid_good_txn_p3d
		,o.ratio_sum__mccuid_good_txn_p3d
		,o.ratio_count__mccuid_good_txn_p3d
		,o.sum__mccuid_fraud_gross_p3d
		,o.count__mccuid_fraud_gross_p3d
		,o.sum__mccuid_transactions_p1d
		,o.count__mccuid_transactions_p1d
		,o.sum__mccuid_good_txn_p1d
		,o.count__mccuid_good_txn_p1d
		,o.ratio_sum__mccuid_good_txn_p1d
		,o.ratio_count__mccuid_good_txn_p1d
		,o.sum__mccuid_fraud_gross_p1d
		,o.count__mccuid_fraud_gross_p1d
		,o.sum__mccuid_transactions_p2h
		,o.count__mccuid_transactions_p2h
		,o.sum__mccuid_good_txn_p2h
		,o.count__mccuid_good_txn_p2h
		,o.ratio_sum__mccuid_good_txn_p2h
		,o.ratio_count__mccuid_good_txn_p2h
		,o.sum__mccuid_fraud_gross_p2h
		,o.count__mccuid_fraud_gross_p2h
        
        /*user fund loading behavior*/
        ,p.min__uid_funding_auth_daydiff
        ,p.count__uid_funding_p7d
        ,p.sum__uid_funding_p7d
        ,p.ratio__uid_ddfund_p7d
        ,p.ratio__uid_cashfund_p7d
        ,p.ratio__uid_sumfund_p7d_maxhist

	        from identifier(:driver_table) a
	        left join identifier(:tbl_user_profile) b on (a.user_id=b.user_id)
	        left join identifier(:tbl_pii) c on (a.auth_event_id=c.auth_event_id)
	        left join identifier(:tbl_login_hist) d on (a.auth_event_id=d.auth_event_id)
	        left join identifier(:tbl_atom) e on (a.auth_event_id=e.auth_event_id)
	        left join identifier(:tbl_app_action) f on (a.auth_event_id=f.auth_event_id)
	        left join identifier(:tbl_app_screen_views) g on (a.auth_event_id=g.auth_event_id)
	        left join identifier(:tbl_block_dvc_hist) h on (a.auth_event_id=h.auth_event_id)
	        left join identifier(:tbl_debit_lnk_failure) i on (a.auth_event_id=i.auth_event_id)
	        left join identifier(:tbl_check_deposit_hist) j on (a.auth_event_id=j.auth_event_id)
	        left join identifier(:tbl_auth_vel_p2d) k on (a.auth_event_id=k.auth_event_id)
	        left join identifier(:tbl_auth_vel_p2h) l on (a.auth_event_id=l.auth_event_id)
	        left join identifier(:tbl_user_disputes) m on (a.auth_event_id=m.auth_event_id)
            
            left join identifier(:tbl_mrch_uid_vel2_b) n1 on (a.auth_event_id=n1.auth_event_id)
	        left join identifier(:tbl_mrch_uid_vel_b) n2 on (a.auth_event_id=n2.auth_event_id)
	        left join identifier(:tbl_mcc_uid_vel) o on (a.auth_event_id=o.auth_event_id)
            left join identifier(:tbl_user_funding) p on (a.auth_event_id=p.auth_event_id)

	);
 


end;
$$
;



call risk.test.feature_appending('risk.test.risky_mrch_eval_adhoc_top100','sp_feature','risk.test.risky_mrch_eval_adhoc_sp_final');
 
