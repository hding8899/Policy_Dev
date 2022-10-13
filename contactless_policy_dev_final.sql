/*>>>>>>>> 
ep pulling

1) contantless
2) approved txn
3) debit amt(req amt<0)
4) may-aug, 2022

key: user_id, auth_event_created_ts|auth_id
*/


create or replace table risk.test.risk_score_simu as(

    select  
    rae.auth_id
    ,rae.auth_event_id
    ,rae.auth_event_created_ts
    
    ,rae.user_id
    ,datediff(month, m.created_at::date, rae.auth_event_created_ts::date) as mob_member
    ,datediff(month, dc.card_created_ts::date, rae.auth_event_created_ts::date) as mob_card
    ,m.state_code as user_state
    
    ,rae.account_status_cd
    ,rae.card_status_cd as card_status_at_txn
    ,rae.available_funds

    ,rae.card_network_cd
    ,rae.card_sub_network_cd
    ,rae.auth_event_merchant_name_raw
    ,rae.req_amt
    ,rae.final_amt
    ,rae.trans_ts
    ,rae.mti_cd

    ,rae.merch_id
    ,rae.mcc_cd
    
    ,rae.risk_score
    ,rae.pin_result_cd
    ,rae.is_international
    ,rae.acq_id

    ,rae.entry_type
    ,rae.is_card_present
    ,rae.is_cardholder_present
    ,case when rae.mcc_cd in ('6010','6011') then 1 or 0 end as atm_withdraw_ind
    --,rae.pan,right(rae.pan,4) as pan_l4d
    --,dual.auth_id as dual_auth_id
    ,o2.type as dfe_rule_disable_status
    ,o2.timestamp as dfe_rule_disable_time
    ,case when rae.card_network_cd='Mastercard' then trim(substr(rae.auth_event_merchant_name_raw,38))
          when rae.card_network_cd='Visa' then trim(substr(rae.auth_event_merchant_name_raw,37,2))
     else null end as merchant_state
    
    ,case when merchant_state<>user_state and user_state is not null and merchant_state is not null then 1 else 0 end as user_mrch_state_diff_ind

    ,dt.dispute_created_at
    ,dt.resolution_decision
    ,case when dt.unique_transaction_id is not null then 1 else 0 end as dispute_ind
    ,case when dt.unique_transaction_id is not null then datediff(day,rae.auth_event_created_ts,dt.dispute_created_at) else null end as dispute_txn_daydiff
    ,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_ind
    ,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') and (dt.resolution = 'Pending Resolution' or dt.resolution is null) then 1 else 0 end as dispute_unauth_pending_ind
    ,case when dt.resolution_decision in ('approve','Approved') then 1 else 0 end as dispute_aprv_ind
    ,case when dt.resolution_decision in ('approve','Approved') and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_aprv_ind

    from edw_db.core.fct_realtime_auth_event as rae
    left join edw_db.core.fct_realtime_auth_event as dual on rae.user_id=dual.user_id and (rae.auth_id=dual.original_auth_id)
    left join segment.chime_prod.member_overrides as o2 on (o2.user_id=rae.user_id and o2.type='disable_fraud_rules' and rae.auth_event_created_ts<=dateadd('hour',1,o2.timestamp) and rae.auth_event_created_ts>o2.timestamp)
    left join risk.prod.disputed_transactions as dt on (dt.user_id=rae.user_id and (dt.authorization_code=rae.auth_id or dt.authorization_code=dual.auth_id))
    left join chime.finance.members m on (rae.user_id=m.id)
    left join edw_db.core.dim_card dc on (rae.user_id=dc.user_id and right(rae.pan,4)=right(dc.card_number,4))
    
    
    where 1=1
    and (rae.auth_event_created_ts::date between '2022-07-01' and '2022-08-31' /*for dev*/ or rae.auth_event_created_ts::date >= '2022-10-10') /*for vol estimation and justification*/
    and rae.original_auth_id=0
    and rae.entry_type like '%Contactless%'/*relaxed to test strategies for other txn type*/
    and rae.response_cd in ('00','10') /*approved txn*/
    and rae.req_amt<0 /*debit spending only*/
    --and rae.card_network_cd='Visa' /*0-90 risk score applies to visa; contactless txn, mastercard and star is very minimum*/
    qualify row_number() over (partition by rae.auth_event_id order by o2.timestamp,dt.dispute_created_at)=1

);




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user_id - dispute history


*/
create or replace table risk.test.risk_score_dispute_hist as (
select ep.auth_event_id

,count(distinct d.unique_transaction_id) as cnt_disp_life
,count(distinct case when d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.unique_transaction_id end) as cnt_disp_unauth_life

,max(ep.auth_event_created_ts) as max_disp_ts
,min(ep.auth_event_created_ts) as min_disp_ts
    
,max(case when d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then ep.auth_event_created_ts end) as max_disp_unauth_ts
,min(case when d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then ep.auth_event_created_ts end) as min_disp_unauth_ts
    
,avg(distinct d.transaction_amount) as avg_disp_life
,avg(distinct case when d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as avg_disp_unauth_life

,sum(distinct d.transaction_amount) as sum_disp_life
,sum(distinct case when d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as sum_disp_unauth_life

,count(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-30 then d.transaction_amount end) as cnt_disp_p30
,count(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-30  and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as cnt_disp_unauth_p30
    
,avg(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-30 then d.transaction_amount end) as avg_disp_p30
,avg(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-30 and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as avg_disp_unauth_p30

,sum(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-30 then d.transaction_amount end) as sum_disp_p30
,sum(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-30 and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as sum_disp_unauth_p30

,count(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-7 then d.transaction_amount end) as cnt_disp_p7
,count(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-7  and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as cnt_disp_unauth_p7
    
,avg(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-7 then d.transaction_amount end) as avg_disp_p7
,avg(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-7 and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as avg_disp_unauth_p7

,sum(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-7 then d.transaction_amount end) as sum_disp_p7
,sum(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-7 and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as sum_disp_unauth_p7

,count(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-3 then d.transaction_amount end) as cnt_disp_p3
,count(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-3  and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as cnt_disp_unauth_p3
    
,avg(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-3 then d.transaction_amount end) as avg_disp_p3
,avg(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-3 and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as avg_disp_unauth_p3

,sum(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-3 then d.transaction_amount end) as sum_disp_p3
,sum(distinct case when d.dispute_created_at>=ep.auth_event_created_ts::date-3 and d.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then d.transaction_amount end) as sum_disp_unauth_p3
  
    
    
    from risk.prod.disputed_transactions d
    inner join (select distinct user_id, auth_event_created_ts, auth_event_id from risk.test.risk_score_simu) ep on (d.user_id=ep.user_id and d.dispute_created_at<ep.auth_event_created_ts)
    group by 1 
);
    
   


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user_id - provisioning history

*/

create or replace table risk.test.risk_score_provision_hist as (
    with t1 as(
    select a.auth_event_id, a.user_id, a.auth_event_created_ts as auth_ts, b.auth_event_created_ts as provision_ts, a.auth_event_created_ts::date-b.auth_event_created_ts::date as auth_provi_daydiff, b.response_cd
        from risk.test.risk_score_simu a
        inner join edw_db.core.fct_realtime_auth_event b on (a.user_id=b.user_id and b.auth_event_created_ts<a.auth_event_created_ts and b.auth_event_merchant_name_raw ilike '%Visa Provisioning%')
    )
    select 
    auth_event_id
    , count(*) as cnt_provision
    , sum(case when response_cd in ('00','10') then 0 else 1 end) as cnt_fail_provision
    , sum(case when response_cd in ('00','10') then 0 else 1 end)/count(*) as rate_fail_provision

    , max(provision_ts) as last_provision_ts
    , min(provision_ts) as first_provision_ts
    , min(auth_provi_daydiff) as min_auth_provi_gap
    , max(auth_provi_daydiff) as max_auth_provi_gap

    , max(case when response_cd in ('00','10') then provision_ts::date end) as last_suc_provision_dt
    , min(case when response_cd in ('00','10') then provision_ts::date end) as first_suc_provision_dt
    , min(case when response_cd in ('00','10') then auth_provi_daydiff end) as min_suc_auth_provi_gap
    , max(case when response_cd in ('00','10') then auth_provi_daydiff end) as max_suc_auth_provi_gap


        from t1
        group by 1
);



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user_id - usage info: application/cta dvc related login dvc/tz/carrier etc.

with feature store features simulated: 
    user_id__usage__2d__7d__v1___nunique__device_ids
    user_id__usage__2d__7d__v1___nunique__ips 
    etc.

*/

create or replace table risk.test.risk_score_login_hist as(
select 
a.auth_event_id
/*look back 30 days*/ 
,count(distinct device_id) as nunique__device_ids_p7d
,count(distinct ip) as nunique__ips_p7d
,count(distinct network_carrier) as nunique__network_carriers_p7d
,count(distinct timezone) as nunique__timezones_p7d
,count(distinct os_name) as nunique__os_versions_p7d
,count(distinct intnl_network_carrier) as nunique__intnl_network_carrier_p7d
,count(distinct africa_network_carriers) as nunique__africa_network_carriers_p7d
,count(distinct africa_timezones) as nunique__africa_timezones_p7d


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
 /*look back 1 days*/       
,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then device_id end) as nunique__device_ids_p5m
,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then ip end) as nunique__ips_p5m
,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then network_carrier end) as nunique__network_carriers_p5m
,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then timezone end) as nunique__timezones_p5m
,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then os_name end) as nunique__os_versions_p5m
,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then intnl_network_carrier end) as nunique__intnl_network_carrier_p5m
,count(distinct case when b.session_timestamp >= dateadd(minute,-5,a.auth_event_created_ts) then africa_network_carriers end) as nunique__africa_network_carriers_p5m

    from risk.test.risk_score_simu a
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
        
                
              ) b on (a.user_id=b.user_id and b.session_timestamp between dateadd(day,-7,a.auth_event_created_ts) and a.auth_event_created_ts)
    group by 1
);





/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 
user_id - atom score summary

with feature store features simulated: 
    user_id__atom_score__0s__2h__v1___nunique__device_ids 
    user_id__atom_score__0s__2h__v1___max__atom_score 

*/

create or replace table risk.test.risk_score_atom as(
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
    
    from risk.test.risk_score_simu a
    left join ml.model_inference.ato_login_alerts b on (a.user_id=b.user_id and b.session_timestamp between dateadd(day,-30,a.auth_event_created_ts) and a.auth_event_created_ts)
    where 1=1
    and score<>0
    group by 1
);




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user_id - same mrch txn history

*/
create or replace table risk.test.risk_score_same_mrch_txn as(
    select 
    a.auth_event_id
    , sum(case when rae.auth_event_id is not null then 1 else 0 end) as cnt_pre_txn /*num of time txn in the same merchant previously*/
    , sum(case when rae.response_cd in ('00','10') then 1 else 0 end) as cnt_pre_appv_txn
    , max(case when rae.response_cd in ('00','10') then rae.final_amt end) as max_pre_txn
    , min(case when rae.response_cd in ('00','10') then rae.final_amt end) as min_pre_txn
    
    , sum(case when rae.entry_type like '%Contactless%' then 1 else 0 end) as cnt_pre_cntls_txn
    , sum(case when rae.entry_type like '%Contactless%' and rae.response_cd in ('00','10') then 1 else 0 end) as cnt_pre_cntls_appv_txn
    
    , max(a.auth_event_created_ts)::date-min(rae.auth_event_created_ts)::date as frst_to_auth_gap
    , max(a.auth_event_created_ts)::date-max(rae.auth_event_created_ts)::date as last_to_auth_gap
    
    , max(a.final_amt)/nullifzero(max_pre_txn) as ratio_cur_txn_pre_max
    , max(a.final_amt)/nullifzero(min_pre_txn) as ratio_cur_txn_pre_min

    , sum(case when dt.unique_transaction_id is not null then 1 else 0 end) as cnt_pre_disp_txn
    , sum(case when dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end) as cnt_pre_unauthdisp_txn
    , cnt_pre_disp_txn/nullifzero(cnt_pre_appv_txn) as ratio_pre_disp_txn
    , cnt_pre_unauthdisp_txn/nullifzero(cnt_pre_appv_txn) as ratio_pre_unauthdisp_txn

        from risk.test.risk_score_simu a
        left join edw_db.core.fct_realtime_auth_event rae on (a.user_id=rae.user_id and a.auth_event_merchant_name_raw=rae.auth_event_merchant_name_raw and rae.auth_event_created_ts<a.auth_event_created_ts and rae.req_amt<0)
        left join edw_db.core.fct_realtime_auth_event as dual on (rae.user_id=dual.user_id and rae.auth_id=dual.original_auth_id)
        left join risk.prod.disputed_transactions as dt on (dt.user_id=rae.user_id and (dt.authorization_code=rae.auth_id or dt.authorization_code=dual.auth_id) and dt.dispute_created_at<a.auth_event_created_ts)

        where 1=1
        and (rae.original_auth_id=0 or rae.original_auth_id is null)
        group by 1
);
    


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user_id - txn vel

with feature store features simulated: 
    user_id__realtime_auth_vel
*/

create or replace table risk.test.risk_score_realauth_vel as(
    
    select 
    b.auth_event_id
    /*debit card contactles txn for diff timeframes*/
    ,count(case when account_type = 'checking' then id end) as count__debit_contactless_approved_p90
    ,sum(case when account_type = 'checking' then abs(amount) end) as sum__debit_contactless_approved_p90
    ,avg(case when account_type = 'checking' then abs(amount) end) as avg__debit_contactless_approved_p90

    ,count(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) and account_type = 'checking' then id end) as count__debit_contactless_approved_p30
    ,sum(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) and  account_type = 'checking' then abs(amount) end) as sum__debit_contactless_approved_p30
    ,avg(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) and  account_type = 'checking' then abs(amount) end) as avg__debit_contactless_approved_p30

    ,count(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) and  account_type = 'checking' then id end) as count__debit_contactless_approved_p2d
    ,sum(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) and  account_type = 'checking' then abs(amount) end) as sum__debit_contactless_approved_p2d
    ,avg(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) and  account_type = 'checking' then abs(amount) end) as avg__debit_contactless_approved_p2d

    ,count(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) and  account_type = 'checking' then id end) as count__debit_contactless_approved_p2h
    ,sum(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) and  account_type = 'checking' then abs(amount) end) as sum__debit_contactless_approved_p2h
    ,avg(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) and  account_type = 'checking' then abs(amount) end) as avg__debit_contactless_approved_p2h

    /*credit card contactles txn for diff timeframes*/
    ,count(case when account_type = 'secured_credit' then id end) as count__credit_contactless_approved_p90
    ,sum(case when account_type = 'secured_credit' then abs(amount) end) as sum__credit_contactless_approved_p90
    ,avg(case when account_type = 'secured_credit' then abs(amount) end) as avg__credit_contactless_approved_p90

    ,count(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) and account_type = 'secured_credit' then id end) as count__credit_contactless_approved_p30
    ,sum(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) and  account_type = 'secured_credit' then abs(amount) end) as sum__credit_contactless_approved_p30
    ,avg(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) and  account_type = 'secured_credit' then abs(amount) end) as avg__credit_contactless_approved_p30

    ,count(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) and  account_type = 'secured_credit' then id end) as count__credit_contactless_approved_p2d
    ,sum(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) and  account_type = 'secured_credit' then abs(amount) end) as sum__credit_contactless_approved_p2d
    ,avg(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) and  account_type = 'secured_credit' then abs(amount) end) as avg__credit_contactless_approved_p2d

    ,count(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) and  account_type = 'secured_credit' then id end) as count__credit_contactless_approved_p2h
    ,sum(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) and  account_type = 'secured_credit' then abs(amount) end) as sum__credit_contactless_approved_p2h
    ,avg(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) and  account_type = 'secured_credit' then abs(amount) end) as avg__credit_contactless_approved_p2h

    /*all cards contactles txn for diff timeframes*/
    ,count(id) as count__contactless_approved_p90
    ,sum(abs(amount)) as sum__contactless_approved_p90
    ,avg(abs(amount)) as avg__contactless_approved_p90

    ,count(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) then id end) as count__ccontactless_approved_p30
    ,sum(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) then abs(amount) end) as sum__contactless_approved_p30
    ,avg(case when a.timestamp>=dateadd(day,-30,b.auth_event_created_ts) then abs(amount) end) as avg__contactless_approved_p30

    ,count(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) then id end) as count__contactless_approved_p2d
    ,sum(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) then abs(amount) end) as sum__contactless_approved_p2d
    ,avg(case when a.timestamp>=dateadd(day,-2,b.auth_event_created_ts) then abs(amount) end) as avg__contactless_approved_p2d

    ,count(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) then id end) as count__contactless_approved_p2h
    ,sum(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) then abs(amount) end) as sum__contactless_approved_p2h
    ,avg(case when a.timestamp>=dateadd(hour,-2,b.auth_event_created_ts) then abs(amount) end) as avg__contactless_approved_p2h

      from segment.chime_prod.realtime_auth a
      inner join risk.test.risk_score_simu b on (a.user_id=b.user_id and a.timestamp between dateadd(day,-90,b.auth_event_created_ts) and b.auth_event_created_ts)
      
      where 1=1 
      and a.amount < 0.00
      and a.response_code = 'approved'
      and a.mti in ('0100','0200','0400')
      and a.entry_type = 'Contactless'
    group by 1
);




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
transfer activities: ach, pf etc

with feature store features simulated:
    user_id__transfer_prod_events__0s__2h__v1___last__event_ach_transfer_initiated_timestamp 
    user_id__transfer_prod_events__0s__2h__v1___last__event_ach_transfer_error_timestamp 
    user_id__transfer_prod_events__0s__2h__v1___last__event_pay_friends_transfer_succeeded_timestamp 
    user_id__transfer_prod_events__0s__2h__v1___last__event_pay_friends_error_timestamp 
    user_id__transfer_prod_events__0s__2h__v1___last__event_transfer_from_savings_timestamp 
    user_id__transaction_prod_events__0s__2h__v1___last__event_realtime_auth_decline_timestamp 

*/
create or replace table risk.test.risk_score_transfer_activity as(
    select 
    a.auth_event_id
    ,max(case when event = 'ach_transfer_initiated' then event_ts end) as last__event_ach_transfer_initiated_timestamp
    ,max(case when event = 'ach_transfer_error' then event_ts end) as last__event_ach_transfer_error_timestamp
    ,max(case when event = 'pay_friends_transfer_succeeded' then event_ts end) as last__event_pay_friends_transfer_succeeded_timestamp
    ,max(case when event = 'pay_friends_error' then event_ts end) as last__event_pay_friends_error_timestamp
    ,max(case when event = 'transfer_from_savings' then event_ts end) as last__event_transfer_from_savings_timestamp

    ,coalesce(count( distinct case when event = 'ach_transfer_initiated' then id end),0) as count__event_ach_transfer_initiated_p28
    ,coalesce(count( distinct case when event = 'ach_transfer_error' then id end),0) as count__event_ach_transfer_error_p28
    ,coalesce(count( distinct case when event = 'pay_friends_transfer_succeeded' then id end),0) as count__event_pay_friends_transfer_succeeded_p28
    ,coalesce(count( distinct case when event = 'pay_friends_error' then id end),0) as count__event_pay_friends_error_p28
    ,coalesce(count( distinct case when event = 'transfer_from_savings' then id end),0) as count__event_transfer_from_savings_p28

    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'ach_transfer_initiated' then id end),0) as count__event_ach_transfer_initiated_p2h
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and  event = 'ach_transfer_error' then id end),0) as count__event_ach_transfer_error_p2h
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and  event = 'pay_friends_transfer_succeeded' then id end),0) as count__event_pay_friends_transfer_succeeded_p2h
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and  event = 'pay_friends_error' then id end),0) as count__event_pay_friends_error_p2h
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and  event = 'transfer_from_savings' then id end),0) as count__event_transfer_from_savings_p2h
    from risk.test.risk_score_simu a
    left join 
    (select *
        from
        (
            select user_id, id, received_at as event_ts, event  from segment.chime_prod.ach_transfer_initiated
                union all
            select user_id, id, received_at as event_ts, event  from segment.chime_prod.ach_transfer_error
                union all
            select user_id, id, received_at as event_ts, event  from segment.chime_prod.pay_friends_transfer_succeeded
                union all
            select user_id, id, received_at as event_ts, event  from segment.chime_prod.pay_friends_error
                union all
            select user_id, id, received_at as event_ts, event  from segment.chime_prod.transfer_from_savings
        ) a
         where try_to_number(a.user_id) is not null and event is not null and event_ts is not null
    ) b on (a.user_id=b.user_id and b.event_ts between dateadd(day,-28,a.auth_event_created_ts) and a.auth_event_created_ts)
    where 1=1
    group by 1
);




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
app action: button tapped

with feature store features simulated:
    user_id__app_actions_prod_events__0s__2h__v1___count__event_log_in_button_tapped 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_login_failed 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_sign_out_button_tapped 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_admin_button_clicked 

*/

create or replace table risk.test.risk_score_app_action as(
    
    select 
    a.auth_event_id
    
    ,max(event_timestamp) as last__event_timestamp
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
    from risk.test.risk_score_simu a
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
atm prod events: atm finder, pin tapped etc

with feature store features simulated:
    user_id__atm_prod_events__0s__2h__v1___count__event_atm_balance_inquiry 
    user_id__atm_prod_events__0s__2h__v1___count__event_atm_finder 
    user_id__atm_prod_events__0s__2h__v1___count__event_map_pin_tapped 

*/

create or replace table risk.test.risk_score_atm_events as(
    
    select 
    a.auth_event_id
    
    ,max(case when event = 'atm_balance_inquiry' then event_ts end) as last__event_atm_balance_inquiry_timestamp
    ,max(case when event = 'atm_finder' then event_ts end) as last__event_member_atm_finder_timestamp
    ,max(case when event = 'map_pin_tapped' then event_ts end) as last__event_map_pin_tapped_timestamp
    ,max(case when event = 'cash_locations' then event_ts end) as last__event_cash_locations_timestamp
    
    ,coalesce(count( distinct case when event = 'atm_balance_inquiry' then id end),0) as count__event_atm_balance_inquiry_p28
    ,coalesce(count( distinct case when event = 'atm_finder' then id end),0) as count__event_atm_finder_p28
    ,coalesce(count( distinct case when event = 'map_pin_tapped' then id end),0) as count__event_map_pin_tapped_p28
    ,coalesce(count( distinct case when event = 'cash_locations' then id end),0) as count__event_cash_locations_p28
    
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'atm_balance_inquiry' then id end),0) as count__event_atm_balance_inquiry_p2h
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'atm_finder' then id end),0) as count__event_atm_finder_p2h
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'map_pin_tapped' then id end),0) as count__event_map_pin_tapped_p2h
    ,coalesce(count( distinct case when b.event_ts>=dateadd(hour,-2,a.auth_event_created_ts) and event = 'cash_locations' then id end),0) as count__event_cash_locations_p2h
    from risk.test.risk_score_simu a
    left join 
    (
        select a.*
         from(
             select user_id, id, received_at as event_ts, event  from segment.chime_prod.atm_balance_inquiry
                union all
             select user_id, id, received_at as event_ts, event  from segment.chime_prod.atm_finder
                union all
             select user_id, id, received_at as event_ts, event  from segment.chime_prod.map_pin_tapped
                union all
             select user_id, id, received_at as event_ts, event  from segment.chime_prod.cash_locations
         ) a
        where 1=1
        and a.event is not null and a.event_ts is not null and try_to_number(a.user_id) is not null
        
    ) b on (a.user_id=try_to_number(b.user_id) and b.event_ts between dateadd(day,-28,a.auth_event_created_ts) and a.auth_event_created_ts)
    where 1=1
    group by 1

);





/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
user_id - pii chg: phone email by user or total

with feature store features simulated:
    user_id__pii_update__0s__7d__v1___count__phone_change
    user_id__pii_update__0s__7d__v1___count__email_change


*/

create or replace table risk.test.risk_score_pii_update as(
    
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
    
    from risk.test.risk_score_simu a
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
        --and a.user_id is not null and event_ts is not null and event is not null
        
    ) b on (a.user_id=b.user_id and b.event_ts between dateadd(day,-30,a.auth_event_created_ts) and a.auth_event_created_ts)
    where 1=1
    group by 1

);




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
mrch level - dispute rate and dispute deny rate


*/

create or replace table risk.test.risk_score_mrch_risk as(

select 
a.auth_event_id
,count(*) as cnt_txn
,coalesce(avg(case when dt.user_dispute_claim_id is not null then 1 else 0 end),0) as avg_disp_rate
,coalesce(avg(case when dt.resolution_decision='Denied' then 1 when dt.resolution_decision='Approved' then 0 else NULL end),0) as avg_disp_decline_rate
    from risk.test.risk_score_simu a
    left join edw_db.core.fct_realtime_auth_event rae on (a.user_id=rae.user_id and a.auth_event_merchant_name_raw=rae.auth_event_merchant_name_raw and rae.auth_event_created_ts between dateadd(day, -2*365, a.auth_event_created_ts) and a.auth_event_created_ts and rae.req_amt<0 and rae.response_cd in ('00','10'))
    left join edw_db.core.fct_realtime_auth_event as dual on (rae.user_id=dual.user_id and rae.auth_id=dual.original_auth_id)
    left join risk.prod.disputed_transactions as dt on (dt.user_id=rae.user_id and (dt.authorization_code=rae.auth_id or dt.authorization_code=dual.auth_id) and dt.dispute_created_at<a.auth_event_created_ts)
    group by 1
    having count(*)>100
);





/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

final dev ep(after global exclu)

*/

create or replace table risk.test.risk_score_final_ep as (
    select a.*
    /*
    , case when a.dispute_ind=1 then a.final_amt else 0 end as disp_amt
    , case when a.dispute_unauth_ind=1 then a.final_amt else 0 end as unauth_disp_amt
    , b.cnt_disp_life
    , b.cnt_disp_unauth_life
    , b.max_disp_ts
    , b.min_disp_ts
    , zeroifnull(-1*b.avg_disp_life) as avg_disp_life
    , zeroifnull(-1*b.avg_disp_unauth_life) as avg_disp_unauth_life
    , zeroifnull(-1*b.sum_disp_life) as sum_disp_life
    , zeroifnull(-1*b.sum_disp_unauth_life) as sum_disp_unauth_life
    , b.cnt_disp_p30
    , b.cnt_disp_unauth_p30
    , zeroifnull(-1*b.avg_disp_p30) as avg_disp_p30
    , zeroifnull(-1*b.avg_disp_unauth_p30) as avg_disp_unauth_p30
    , zeroifnull(-1*b.sum_disp_p30) as sum_disp_p30
    , zeroifnull(-1*b.sum_disp_unauth_p30) as sum_disp_unauth_p30
    
    , zeroifnull(-1*b.avg_disp_p7) as avg_disp_p7
    , zeroifnull(-1*b.avg_disp_unauth_p7) as avg_disp_unauth_p7
    , zeroifnull(-1*b.sum_disp_p7) as sum_disp_p7
    , zeroifnull(-1*b.sum_disp_unauth_p7) as sum_disp_unauth_p7
    
    , zeroifnull(-1*b.avg_disp_p3) as avg_disp_p3
    , zeroifnull(-1*b.avg_disp_unauth_p3) as avg_disp_unauth_p3
    , zeroifnull(-1*b.sum_disp_p3) as sum_disp_p3
    , zeroifnull(-1*b.sum_disp_unauth_p3) as sum_disp_unauth_p3
    */
    , c.count__email_change_by_user_p30
    , c.count__phone_change_by_user_p30
    , c.count__email_change_p30
    , c.count__phone_change_p30
    , c.count__email_change_by_user_p7d
    , c.count__phone_change_by_user_p7d
    , c.count__email_change_p7d
    , c.count__phone_change_p7d
    , c.count__email_change_by_user_p3d
    , c.count__phone_change_by_user_p3d
    , c.count__email_change_p3d
    , c.count__phone_change_p3d
    , c.count__email_change_by_user_p2d
    , c.count__phone_change_by_user_p2d
    , c.count__email_change_p2d
    , c.count__phone_change_p2d
    , c.count__email_change_by_user_p1d
    , c.count__phone_change_by_user_p1d
    , c.count__email_change_p1d
    , c.count__phone_change_p1d
    , c.count__email_change_by_user_p1h
    , c.count__phone_change_by_user_p1h
    , c.count__email_change_p1h
    , c.count__phone_change_p1h
    , c.count__email_change_by_user_p5m
    , c.count__phone_change_by_user_p5m
    , c.count__email_change_p5m
    , c.count__phone_change_p5m
    /*
    , e.cnt_pre_txn
    , e.cnt_pre_appv_txn
    , e.max_pre_txn
    , e.min_pre_txn
    , e.cnt_pre_cntls_txn
    , e.cnt_pre_cntls_appv_txn
    , e.frst_to_auth_gap
    , e.last_to_auth_gap
    , e.ratio_cur_txn_pre_max
    , e.ratio_cur_txn_pre_min
    , e.cnt_pre_disp_txn
    , e.cnt_pre_unauthdisp_txn
    , e.ratio_pre_disp_txn
    , e.ratio_pre_unauthdisp_txn
    */
    /*
    , f.nunique__device_ids_p90
    , f.nunique__ips_p90
    , f.nunique__network_carriers_p90
    , f.nunique__timezones_p90
    , f.nunique__os_versions_p90
    , f.nunique__intnl_network_carrier_p90
    , f.nunique__africa_network_carriers_p90
    , f.nunique__africa_timezones_p90
    , f.nunique__device_ids_p30
    , f.nunique__ips_p30
    , f.nunique__network_carriers_p30
    , f.nunique__timezones_p30
    , f.nunique__os_versions_p30
    , f.nunique__intnl_network_carrier_p30
    , f.nunique__africa_network_carriers_p30
    , f.nunique__africa_timezones_p30
    */
    , f.nunique__device_ids_p7d
    , f.nunique__ips_p7d
    , f.nunique__network_carriers_p7d
    , f.nunique__timezones_p7d
    , f.nunique__os_versions_p7d
    , f.nunique__intnl_network_carrier_p7d
    , f.nunique__africa_network_carriers_p7d
    , f.nunique__africa_timezones_p7d
    , f.nunique__device_ids_p2d
    , f.nunique__ips_p2d
    , f.nunique__network_carriers_p2d
    , f.nunique__timezones_p2d
    , f.nunique__os_versions_p2d
    , f.nunique__intnl_network_carrier_p2d
    , f.nunique__africa_network_carriers_p2d
    , f.nunique__africa_timezones_p2d
    , f.nunique__device_ids_p1d
    , f.nunique__ips_p1d
    , f.nunique__network_carriers_p1d
    , f.nunique__timezones_p1d
    , f.nunique__os_versions_p1d
    , f.nunique__intnl_network_carrier_p1d
    , f.nunique__africa_network_carriers_p1d
    , f.nunique__africa_timezones_p1d
    , f.nunique__device_ids_p2h
    , f.nunique__ips_p2h
    , f.nunique__network_carriers_p2h
    , f.nunique__timezones_p2h
    , f.nunique__os_versions_p2h
    , f.nunique__intnl_network_carrier_p2h
    , f.nunique__africa_network_carriers_p2h
    , f.nunique__africa_timezones_p2h
    , f.nunique__device_ids_p1h
    , f.nunique__ips_p1h
    , f.nunique__network_carriers_p1h
    , f.nunique__timezones_p1h
    , f.nunique__os_versions_p1h
    , f.nunique__intnl_network_carrier_p1h
    , f.nunique__africa_network_carriers_p1h
    , f.nunique__africa_timezones_p1h
    , f.nunique__device_ids_p5m
    , f.nunique__ips_p5m
    , f.nunique__network_carriers_p5m
    , f.nunique__timezones_p5m
    , f.nunique__os_versions_p5m
    , f.nunique__intnl_network_carrier_p5m
    , f.nunique__africa_network_carriers_p5m
    /*
    , h.cnt_provision
    , h.cnt_fail_provision
    , h.rate_fail_provision
    , h.last_provision_ts
    , h.first_provision_ts
    , h.min_auth_provi_gap
    , h.max_auth_provi_gap
    , h.last_suc_provision_dt
    , h.first_suc_provision_dt
    , h.min_suc_auth_provi_gap
    , h.max_suc_auth_provi_gap
    */
    , g.max_atom_score_ts
    , g.max_atom_score_p30
    , g.max_atom_score_p3d
    , g.max_atom_score_p1d
    , g.max_atom_score_p2h
    , g.cnt_dist_dvc_p30
    , g.cnt_dist_dvc_p3d
    , g.cnt_dist_dvc_p1d
    , g.cnt_dist_dvc_p2h
    /*
    , i.cnt_txn as cnt_mrch_txn_p2y
    , i.avg_disp_rate as avg_mrch_disp_rate
    , i.avg_disp_decline_rate as avg_mrch_disp_deny_rate
    
    , j.count__debit_contactless_approved_p90
    , j.sum__debit_contactless_approved_p90
    , j.avg__debit_contactless_approved_p90
    , j.count__debit_contactless_approved_p30
    , j.sum__debit_contactless_approved_p30
    , j.avg__debit_contactless_approved_p30
    , j.count__debit_contactless_approved_p2d
    , j.sum__debit_contactless_approved_p2d
    , j.avg__debit_contactless_approved_p2d
    , j.count__debit_contactless_approved_p2h
    , j.sum__debit_contactless_approved_p2h
    , j.avg__debit_contactless_approved_p2h
    , j.count__credit_contactless_approved_p90
    , j.sum__credit_contactless_approved_p90
    , j.avg__credit_contactless_approved_p90
    , j.count__credit_contactless_approved_p30
    , j.sum__credit_contactless_approved_p30
    , j.avg__credit_contactless_approved_p30
    , j.count__credit_contactless_approved_p2d
    , j.sum__credit_contactless_approved_p2d
    , j.avg__credit_contactless_approved_p2d
    , j.count__credit_contactless_approved_p2h
    , j.sum__credit_contactless_approved_p2h
    , j.avg__credit_contactless_approved_p2h
    , j.count__contactless_approved_p90
    , j.sum__contactless_approved_p90
    , j.avg__contactless_approved_p90
    , j.count__ccontactless_approved_p30
    , j.sum__contactless_approved_p30
    , j.avg__contactless_approved_p30
    , j.count__contactless_approved_p2d
    , j.sum__contactless_approved_p2d
    , j.avg__contactless_approved_p2d
    , j.count__contactless_approved_p2h
    , j.sum__contactless_approved_p2h
    , j.avg__contactless_approved_p2h
    
    , k.last__event_ach_transfer_initiated_timestamp
    , k.last__event_ach_transfer_error_timestamp
    , k.last__event_pay_friends_transfer_succeeded_timestamp
    , k.last__event_pay_friends_error_timestamp
    , k.last__event_transfer_from_savings_timestamp
    , k.count__event_ach_transfer_initiated_p28
    , k.count__event_ach_transfer_error_p28
    , k.count__event_pay_friends_transfer_succeeded_p28
    , k.count__event_pay_friends_error_p28
    , k.count__event_transfer_from_savings_p28
    , k.count__event_ach_transfer_initiated_p2h
    , k.count__event_ach_transfer_error_p2h
    , k.count__event_pay_friends_transfer_succeeded_p2h
    , k.count__event_pay_friends_error_p2h
    , k.count__event_transfer_from_savings_p2h

    , l.last__event_timestamp
    , l.last__event_log_in_button_tapped_timestamp
    , l.last__event_login_failed_timestamp
    , l.last__event_sign_out_button_tapped_timestamp
    , l.last__event_admin_button_clicked_timestamp
    , l.count__event_log_in_button_tapped_p28
    , l.count__event_login_failed_p28
    , l.count__event_sign_out_button_tapped_p28
    , l.count__event_admin_button_clicked_p28
    , l.count__event_log_in_button_tapped_p2h
    , l.count__event_login_failed_p2h
    , l.count__event_sign_out_button_tapped_p2h
    , l.count__event_admin_button_clicked_p2h
    */
    /*
    , m.last__event_atm_balance_inquiry_timestamp
    , m.last__event_member_atm_finder_timestamp
    , m.last__event_map_pin_tapped_timestamp
    , m.last__event_cash_locations_timestamp
    , m.count__event_atm_balance_inquiry_p28
    , m.count__event_atm_finder_p28
    , m.count__event_map_pin_tapped_p28
    , m.count__event_cash_locations_p28
    , m.count__event_atm_balance_inquiry_p2h
    , m.count__event_atm_finder_p2h
    , m.count__event_map_pin_tapped_p2h
    , m.count__event_cash_locations_p2h
    */
        from risk.test.risk_score_simu a
        --left join risk.test.risk_score_dispute_hist b on (a.auth_event_id=b.auth_event_id)
        left join risk.test.risk_score_pii_update c on (a.auth_event_id=c.auth_event_id)
        --left join risk.test.risk_score_piichg_hist c2 on (a.auth_event_id=c2.auth_event_id)
        --left join risk.test.risk_score_provi_dvc_hist d on (a.auth_event_id=d.auth_event_id)
        --left join risk.test.risk_score_same_mrch_txn e on (a.auth_event_id=e.auth_event_id)
        left join risk.test.risk_score_login_hist f on (a.auth_event_id=f.auth_event_id)
        left join risk.test.risk_score_atom g on (a.auth_event_id=g.auth_event_id)
        --left join risk.test.risk_score_provision_hist h on (a.auth_event_id=h.auth_event_id)
        --left join risk.test.risk_score_mrch_risk i on (a.auth_event_id=i.auth_event_id)
        --left join risk.test.risk_score_realauth_vel j on (a.auth_event_id=j.auth_event_id)
        --left join risk.test.risk_score_transfer_activity k on (a.auth_event_id=k.auth_event_id)
        --left join risk.test.risk_score_app_action l on (a.auth_event_id=l.auth_event_id)
        --left join risk.test.risk_score_atm_events m on (a.auth_event_id=m.auth_event_id)

        where 1=1
        --and a.final_amt>=10 and mob_card<24
);






/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
policy dev exploration query

*/


select 

width_bucket(MAX_ATOM_SCORE_P3D, 0, 1, 10) as bin
, min(MAX_ATOM_SCORE_P3D) as min_val, max(MAX_ATOM_SCORE_P3D) as max_val
, count(*) as cnt_txn

, avg(dispute_ind) as dispute_rate
, avg(dispute_unauth_ind) as dispute_unauth_rate
, avg(dispute_aprv_ind) as dispute_appv_rate
, avg(dispute_unauth_aprv_ind) as dispute_unauth_appv_rate

, sum(dispute_ind) as dispute_cnt
, sum(dispute_unauth_ind) as dispute_unauth_cnt
, sum(dispute_aprv_ind) as dispute_appv_cnt
, sum(dispute_unauth_aprv_ind) as dispute_unauth_appv_cnt

, sum(final_amt) as sum_txn
, sum(case when dispute_ind=1 then final_amt else 0 end) as sum_dispute_sum
, sum(case when dispute_unauth_ind=1 then final_amt else 0 end) as sum_dispute_unauth_sum
, sum(case when dispute_unauth_aprv_ind=1 then final_amt else 0 end) as sum_dispute_unauth_appv_sum
   from risk.test.risk_score_final_ep
    where 1=1
    and trunc(to_date(auth_event_created_ts),'month')< '2022-09-01'
    and COUNT__PHONE_CHANGE_P7D=0
    --and MAX_ATOM_SCORE_P1D>=0.01
    and nunique__timezones_p7d>=2
    --and NUNIQUE__DEVICE_IDS_P1D<2
    --and MAX_ATOM_SCORE_P3D>=0.5
    --and datediff(day,last_provision_ts,auth_event_created_ts)=0
    --and COUNT__EMAIL_CHANGE_P2D>0
    --and COUNT__EVENT_LOG_IN_BUTTON_TAPPED_P2H=0
    --and COUNT__EVENT_LOGIN_FAILED_P2H>=1
    --and MAX_ATOM_SCORE_P3D>=0.4
    --and COUNT__PHONE_CHANGE_P1D>0
    --and COUNT__CONTACTLESS_APPROVED_P2D>=10
    --AND MAX_ATOM_SCORE_P3D>=0.2
    --AND CNT_DIST_DVC_P3D>=2
    --and COUNT__EMAIL_CHANGE_P2D>=1 
    --and NUNIQUE__DEVICE_IDS_P2H>=2
    --and NUNIQUE__TIMEZONES_P2H>=2
    --and datediff(day,last_provision_ts,auth_event_created_ts)=0
    --and COUNT__EMAIL_CHANGE_P3D>0
    --and COUNT__EVENT_LOGIN_FAILED_P2H>1
    --and NUNIQUE__DEVICE_IDS_P2H>1
    --and MAX_ATOM_SCORE_P1D>0.1
    --and NUNIQUE__NETWORK_CARRIERS_P1D>1
    --and NUNIQUE__TIMEZONES_P1D>1
    --and NUNIQUE__DEVICE_IDS_P1D>=3
    --and COUNT__EMAIL_CHANGE_P7D>0
    --and datediff(hour,MAX_LOGIN_TS, auth_event_created_ts)<24
    --and datediff(hour,last_provision_ts,auth_event_created_ts)<2
    --and cnt_eml_chg_p3>0
    --and min_auth_provi_gap=0
    --and cnt_eml_chg_p2>0
    --and cnt_eml_chg_p3>0
    --and min_auth_provi_gap=1
    --and CNT_PHO_CHG_LIFE=0
    --and risk_score>=35
    --and CNT_LOGIN_DVC_P2D>3
    --and cnt_login_dvc_p2d>2
    --and cnt_eml_chg_p3>0
    --and CNT_LOGIN_INTLNC_P3D>0
    and final_amt>=50
    --and cnt_login_carrier_p3d>=2
    --and cnt_login_tz_p3d>=2
    --and cnt_login_carrier_p7d>=2
    --and cnt_login_dvc_life>=4
    --and mob_card<6
    --and cnt_disp_p30>=2
    --and min_suc_auth_provi_gap<2
    --and cnt_pre_txn<5
    --and cnt_pre_appv_txn<4
    --and MIN_AUTH_PROVI_GAP>=7
    --and MIN_AUTH_PROVI_GAP=0
    --and CNT_LOGIN_DVC_P7D>=2
    --and available_funds<0
    --and trunc(to_date(b.auth_event_created_ts),'month')< '2022-08-01'
    --and least(cnt_pho_chg_life,cnt_eml_chg_p2)=0

    group by 1 
    order by 1 
;


--create or replace table risk.test.risk_score_final_ep_volesti as select * from risk.test.risk_score_final_ep;

/*policy dev: no phone chg p7d*/
select 
case 
     when final_amt>=50 and max_atom_score_p30>=0.35 and  NUNIQUE__DEVICE_IDS_P2H>=2 then 'dec 1.1'
    
     when final_amt>=50 and risk_score>=35 and MAX_ATOM_SCORE_P1D>=0.1 and nunique__timezones_p7d>=2 /*or CNT_DIST_DVC_P2H>=3*/ then 'dec 1.2'
     
     --when final_amt>=50 and count__email_change_p7d>0 and risk_score>=35 /*or CNT_DIST_DVC_P2H>=3*/ then 'dec 1.3'
     --when final_amt>=100 and risk_score>=45 and datediff(day,last_provision_ts,auth_event_created_ts)=0 and COUNT__EVENT_LOGIN_FAILED_P2H>0 then 'dec 999'     
      
     else 'no dec' end as cat
, count(*) as cnt_txnr

, avg(dispute_ind) as dispute_rate
, avg(dispute_unauth_ind) as dispute_unauth_rate
, avg(dispute_aprv_ind) as dispute_appv_rate
, avg(dispute_unauth_aprv_ind) as dispute_unauth_appv_rate

, sum(dispute_ind) as dispute_cnt
, sum(dispute_unauth_ind) as dispute_unauth_cnt
, sum(dispute_aprv_ind) as dispute_appv_cnt
, sum(dispute_unauth_aprv_ind) as dispute_unauth_appv_cnt

, sum(final_amt) as sum_txn
, sum(case when dispute_ind=1 then final_amt else 0 end) as sum_dispute_sum
, sum(case when dispute_unauth_ind=1 then final_amt else 0 end) as sum_dispute_unauth_sum
, sum(case when dispute_unauth_aprv_ind=1 then final_amt else 0 end) as sum_dispute_unauth_appv_sum
   from risk.test.risk_score_final_ep
    where 1=1
    and COUNT__PHONE_CHANGE_P7D=0
    and trunc(to_date(auth_event_created_ts),'month')< '2022-09-01'
    --and CNT_PHO_CHG_LIFE=0   
    --and cnt_eml_chg_p3>0
    --and final_amt>=50
    --and MIN_AUTH_PROVI_GAP>=7
    --and risk_score>=35
    --and final_amt>=25
    --and cnt_login_dvc_p7d>=3
    --and cnt_login_carrier_p7d>1
    --and max_atom_score_p30>=0.2
    --and max_atom_score_p30>=0.2
    --and CNT_LOGIN_DVC_P7D>=2
    --and available_funds<0
    --and trunc(to_date(auth_event_created_ts),'month')< '2022-07-01'
    --and to_date(auth_event_created_ts) >='2022-10-05'
group by 1
order by 1
;



/*policy dev: phone chg p7d*/
select 
case when final_amt>=200 and max_atom_score_p1d>=0.1 and  (count__email_change_by_user_p7d-count__email_change_by_user_p5m>=1)   then 'dec 1.1'
     --when final_amt>=200 and (NUNIQUE__TIMEZONES_P2H-NUNIQUE__TIMEZONES_P5M>=2)  then 'dec 1.1'
     when final_amt>=200 and MAX_ATOM_SCORE_P1D>=0.4 and (NUNIQUE__TIMEZONES_P2H>=2   
                                                          or nunique__network_carriers_p2h>=2
                                                         or NUNIQUE__AFRICA_NETWORK_CARRIERS_P7d>=1 
                                                           or NUNIQUE__INTNL_NETWORK_CARRIER_P7D>=1
                                                         or  NUNIQUE__IPS_P1H>=3
                                                        or NUNIQUE__DEVICE_IDS_P1H>=2
                                                         or  NUNIQUE__OS_VERSIONS_P1H>=2
                                                         ) then 'dec 1.2'
          
     else 'no dec' end as cat
, count(*) as cnt_txn

, avg(dispute_ind) as dispute_rate
, avg(dispute_unauth_ind) as dispute_unauth_rate
, avg(dispute_aprv_ind) as dispute_appv_rate
, avg(dispute_unauth_aprv_ind) as dispute_unauth_appv_rate

, sum(dispute_ind) as dispute_cnt
, sum(dispute_unauth_ind) as dispute_unauth_cnt
, sum(dispute_aprv_ind) as dispute_appv_cnt
, sum(dispute_unauth_aprv_ind) as dispute_unauth_appv_cnt

, sum(final_amt) as sum_txn
, sum(case when dispute_ind=1 then final_amt else 0 end) as sum_dispute_sum
, sum(case when dispute_unauth_ind=1 then final_amt else 0 end) as sum_dispute_unauth_sum
, sum(case when dispute_unauth_aprv_ind=1 then final_amt else 0 end) as sum_dispute_unauth_appv_sum
   from risk.test.risk_score_final_ep
    where 1=1
    and count__phone_change_p7d>0
    and trunc(to_date(auth_event_created_ts),'month')< '2022-09-01'
    --and NUNIQUE__DEVICE_IDS_P1D>=3
    --and CNT_PHO_CHG_LIFE=0   
    --and cnt_eml_chg_p3>0
    --and final_amt>=50
    --and MIN_AUTH_PROVI_GAP>=7
    --and risk_score>=40
    --and final_amt>=25
    --and cnt_login_dvc_p7d>=3
    --and cnt_login_carrier_p7d>1
    --and max_atom_score_p30>=0.2
    --and max_atom_score_p30>=0.2
    --and CNT_LOGIN_DVC_P7D>=2
    --and available_funds<0
    --and trunc(to_date(auth_event_created_ts),'month')= '2022-09-01'
group by 1
order by 1
;
                                                             




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
post dev table drops
*/

drop table risk.test.risk_score_simu;
drop table risk.test.risk_score_dispute_hist;
drop table risk.test.risk_score_pii_update;
drop table risk.test.risk_score_same_mrch_txn;
drop table risk.test.risk_score_login_hist;
drop table risk.test.risk_score_atom;
drop table risk.test.risk_score_provision_hist;
drop table risk.test.risk_score_mrch_risk;
drop table risk.test.risk_score_realauth_vel;
drop table risk.test.risk_score_transfer_activity;
drop table risk.test.risk_score_app_action;
drop table risk.test.risk_score_atm_events;


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
For simu and production simulation
*/                                                             
    
with simu as (

    select 
    case when COUNT__PHONE_CHANGE_P7D=0 and final_amt>=50 and risk_score>=30 and max_atom_score_p30>=0.3 and  (NUNIQUE__DEVICE_IDS_P5M>=2 or NUNIQUE__DEVICE_IDS_P2H-NUNIQUE__DEVICE_IDS_P5M>=2 or NUNIQUE__DEVICE_IDS_P1D-NUNIQUE__DEVICE_IDS_P2H>=2) then 'dec 1.1' 
         when COUNT__PHONE_CHANGE_P7D=0 and final_amt>=50 and MAX_ATOM_SCORE_P2H>=0.5 /*or CNT_DIST_DVC_P2H>=3*/ then 'dec 1.2'
         when COUNT__PHONE_CHANGE_P7D>0 and final_amt>=200 and max_atom_score_p1d>=0.1 and  (count__email_change_by_user_p7d-count__email_change_by_user_p5m>=1)   then 'dec 2.1' 
         when COUNT__PHONE_CHANGE_P7D>0 and final_amt>=200 and MAX_ATOM_SCORE_P1D>=0.4 and (NUNIQUE__TIMEZONES_P2H-NUNIQUE__TIMEZONES_P5M>=2   
                                                          or nunique__network_carriers_p2h>=2
                                                         or NUNIQUE__AFRICA_NETWORK_CARRIERS_P7d>=1 
                                                           or NUNIQUE__INTNL_NETWORK_CARRIER_P7D>=1
                                                         or  NUNIQUE__IPS_P1H>=3
                                                        or NUNIQUE__DEVICE_IDS_P1H>=2
                                                         or  NUNIQUE__OS_VERSIONS_P1H>=2
                                                         ) then 'dec 2.2'
     end as cat
    , a.*
    from risk.test.risk_score_final_ep_volesti a
    where 1=1
    and auth_event_created_ts::date='2022-10-12'
    and cat is not null
        
), shadow as (
    
    select distinct policy_name, decision_id, auth_id, user_id
        ,row_number() over (partition by auth_id order by policy_name) as trigger_order
        from chime.decision_platform.real_time_auth a
        where 1=1
        and policy_name like 'hr_mobilewallet_vrs_ato_suslogin%'
        and original_timestamp::date='2022-10-12'
        and policy_result='criteria_met'
        and decision_outcome in ('hard_block','merchant_block','deny','prompt_override','sanction_block')
 
)

select distinct 
coalesce(a.auth_id,b.auth_id) as merged_auth_id
,case when a.auth_id is null then 'in prod only'
      when b.auth_id is null then 'in simu only'
      else 'in both' end as recon_cat
,coalesce(a.cat, b.policy_name) as policy_name
,b.decision_id
,coalesce(a.user_id,b.user_id) as user_id
,a.COUNT__PHONE_CHANGE_P7D
,a.final_amt
,a.risk_score
,a.max_atom_score_p30, a.MAX_ATOM_SCORE_P2H, max_atom_score_p1d
,NUNIQUE__DEVICE_IDS_P1D, NUNIQUE__DEVICE_IDS_P2H, NUNIQUE__DEVICE_IDS_P5M
,count__email_change_by_user_p7d
,NUNIQUE__TIMEZONES_P2H
,nunique__network_carriers_p2h
,NUNIQUE__AFRICA_NETWORK_CARRIERS_P7d
,NUNIQUE__INTNL_NETWORK_CARRIER_P7D
,NUNIQUE__IPS_P1H
,NUNIQUE__DEVICE_IDS_P1H
,NUNIQUE__OS_VERSIONS_P1H
    from simu a
    full outer join shadow b on (a.user_id=b.user_id and a.auth_id=b.auth_id)
    
    
;


with t1 as(
 select a.*
        ,row_number() over (partition by auth_id order by policy_name) as trigger_order
        from chime.decision_platform.real_time_auth a
        where 1=1
        and policy_name like 'hr_mobilewallet_vrs_ato_suslogin%'
        and original_timestamp::date='2022-10-12'
        and policy_result='criteria_met'
        and decision_outcome in ('hard_block','merchant_block','deny','prompt_override','sanction_block')
 

)
    
    select policy_name, count(distinct auth_id) as cnt, count(distinct case when trigger_order=1 then auth_id end) as cnt_dedup
        from t1
        group by 1
        order by 1;
                                                             


select 
COUNT__PHONE_CHANGE_P7D
,a.final_amt
,a.risk_score
,a.max_atom_score_p30, a.MAX_ATOM_SCORE_P2H, max_atom_score_p1d
,NUNIQUE__DEVICE_IDS_P1D, NUNIQUE__DEVICE_IDS_P2H, NUNIQUE__DEVICE_IDS_P5M
,count__email_change_by_user_p7d
,NUNIQUE__TIMEZONES_P2H
,nunique__network_carriers_p2h
,NUNIQUE__AFRICA_NETWORK_CARRIERS_P7d
,NUNIQUE__INTNL_NETWORK_CARRIER_P7D
,NUNIQUE__IPS_P1H
,NUNIQUE__DEVICE_IDS_P1H
,NUNIQUE__OS_VERSIONS_P1H
    from risk.test.risk_score_final_ep_volesti a
    where 1=1
    and auth_id='6153122950'
;

/*rae view*/
select req_amt, final_amt, risk_score, entry_type, auth_id
    from edw_db.core.fct_realtime_auth_event
    where 1=1
    --and auth_id='1616662338'
    and user_id='22527286'
    and auth_event_created_ts::date='2022-10-12'
;

select settled_amt
    from edw_db.core.ftr_transaction 
    where 1=1
    and user_id='22527286'
    and convert_timezone('America/Los_Angeles',transaction_timestamp)::date='2022-10-12'
;

--6153122950
select datediff(hour,b.auth_event_created_ts,a.timestamp) as hour_diff,b.auth_event_created_ts, a.*
from edw_db.feature_store.atom_app_events_v2 a
inner join risk.test.risk_score_final_ep_volesti  b on (a.user_id=b.user_id and a.timestamp between dateadd(day,-1,b.auth_event_created_ts) and b.auth_event_created_ts)
where 1=1
and a.user_id=14817111
and b.auth_id='6153122950'
order by timestamp desc
;