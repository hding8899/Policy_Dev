/*>>>>>>>> 
ep pulling

1) NOT contantless
2) approved txn
3) debit amt(req amt<0)
4) jun-aug, 2022

key: user_id, auth_event_created_ts|auth_id
*/


create or replace table risk.test.risk_score_simu_v2 as(

    select  
    rae.auth_event_id
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
    and rae.auth_event_created_ts::date between '2022-06-01' and '2022-08-31'
    and rae.original_auth_id=0
    and rae.entry_type not like '%Contactless%'/*relaxed to test strategies for other txn type*/
    and rae.response_cd in ('00','10') /*approved txn*/
    and rae.req_amt<0 /*debit spending only*/
    --and rae.card_network_cd='Visa' /*0-90 risk score applies to visa; contactless txn, mastercard and star is very minimum*/
    qualify row_number() over (partition by rae.auth_event_id order by o2.timestamp,dt.dispute_created_at)=1

);




/*basic stats*/
select count(*),count(distinct auth_event_id) from risk.test.risk_score_simu_v2;
--58,366,299
select top 10 * from risk.test.risk_score_simu_v2;

select entry_type,count(*)
from risk.test.risk_score_simu_v2
group by 1
;
  


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
usage: dvc tz platform etc

with feature store features simulated: 
    user_id__usage__2d__7d__v1___nunique__device_ids
    user_id__usage__2d__7d__v1___nunique__ips 
    etc.

*/
create or replace table risk.test.risk_score_login_hist_v2 as(
select 
a.auth_event_id
/*look back 7 days*/ 
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

    from risk.test.risk_score_simu_v2 a
    inner join (
                select user_id, session_timestamp, device_id, network_carrier, os_name, ip, timezone, platform, zip_code
                ,case when lower(network_carrier) not in
                          ('t-mobile', 'at&t', 'metro by t-mobile', 'verizon', 'boost mobile', 'null', 'cricket',
                           'tfw', 'sprint', '','metro', 'boost', 'home','spectrum', 'xfinity mobile', 'verizon wireless', 'assurance wireless',
                           'u.s. cellular', 'metropcs','carrier', 'google fi') then network_carrier end as intnl_network_carrier
                ,case when lower(network_carrier) like any ('%mtn%','%airtel%','%glo%','%9mobile%','%stay safe%','%besafe%', '% ng%','%nigeria%','%etisalat%') 
                           and lower(network_carrier) not in ('roaming indicator off', 'searching for service') 
                           then network_carrier end as africa_network_carriers
                ,case when lower(timezone) like '%africa%' then timezone end as africa_timezones
                from edw_db.feature_store.atom_user_sessions_v2
                
              ) b on (a.user_id=b.user_id and b.session_timestamp between dateadd(day,-7,a.auth_event_created_ts) and a.auth_event_created_ts)
    group by 1
);


select count(*) from risk.test.risk_score_login_hist_v2;
select top 10 * from  risk.test.risk_score_login_hist_v2;


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 
login atom score model score summary

with feature store features simulated: 
    user_id__atom_score__0s__2h__v1___nunique__device_ids 
    user_id__atom_score__0s__2h__v1___max__atom_score 

*/

create or replace table risk.test.risk_score_atom_v2 as(
select a.auth_event_id
,max(b.session_timestamp) as max_atom_score_ts
,max(case when b.session_timestamp>=dateadd(day,-3,a.auth_event_created_ts) then score end) as max_atom_score_p3d
,max(case when b.session_timestamp>=dateadd(day,-1,a.auth_event_created_ts) then score end) as max_atom_score_p1d
,max(case when b.session_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) then score end) as max_atom_score_p2h
    
,count(distinct case when b.session_timestamp>=dateadd(day,-3,a.auth_event_created_ts) then b.device_id end) as cnt_dist_dvc_p3d
,count(distinct case when b.session_timestamp>=dateadd(day,-1,a.auth_event_created_ts) then b.device_id end) as cnt_dist_dvc_p1d
,count(distinct case when b.session_timestamp>=dateadd(hour,-2,a.auth_event_created_ts) then b.device_id end) as cnt_dist_dvc_p2h
    
    from risk.test.risk_score_simu_v2 a
    left join ml.model_inference.ato_login_alerts b on (a.user_id=b.user_id and b.session_timestamp between dateadd(day,-3,a.auth_event_created_ts) and a.auth_event_created_ts)
    where 1=1
    and score<>0
    group by 1
);




select count(*) from risk.test.risk_score_atom_v2;
select top 10 * from  risk.test.risk_score_atom_v2;



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
app action: button tapped

with feature store features simulated:
    user_id__app_actions_prod_events__0s__2h__v1___count__event_log_in_button_tapped 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_login_failed 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_sign_out_button_tapped 
    user_id__app_actions_prod_events__0s__2h__v1___count__event_admin_button_clicked 

*/

create or replace table risk.test.risk_score_app_action_v2 as(
    
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
    from risk.test.risk_score_simu_v2 a
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


select top 10 * from risk.test.risk_score_app_action_v2 ;
select count(*) from risk.test.risk_score_app_action_v2 ;



/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
atm prod events: atm finder, pin tapped etc

with feature store features simulated:
    user_id__atm_prod_events__0s__2h__v1___count__event_atm_balance_inquiry 
    user_id__atm_prod_events__0s__2h__v1___count__event_atm_finder 
    user_id__atm_prod_events__0s__2h__v1___count__event_map_pin_tapped 

*/

create or replace table risk.test.risk_score_atm_events_v2 as(
    
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
    from risk.test.risk_score_simu_v2 a
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


select count(*) from risk.test.risk_score_atm_events_v2;
select top 10 * from risk.test.risk_score_atm_events_v2;


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
pii chg: phone email

with feature store features simulated:
    user_id__pii_update__0s__7d__v1___count__phone_change
    user_id__pii_update__0s__7d__v1___count__email_change


*/

create or replace table risk.test.risk_score_pii_update_v2 as(
    
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
    
    from risk.test.risk_score_simu_v2 a
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

select count(*) from risk.test.risk_score_pii_update_v2;
select top 10 * from risk.test.risk_score_pii_update_v2;





/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

final dev ep(after global exclu)

*/

create or replace table risk.test.risk_score_final_ep_v2 as (
    
    select a.*
    
    , case when a.dispute_ind=1 then a.final_amt else 0 end as disp_amt
    , case when a.dispute_unauth_ind=1 then a.final_amt else 0 end as unauth_disp_amt

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
    
  
    , g.max_atom_score_ts
    , g.max_atom_score_p3d
    , g.max_atom_score_p1d
    , g.max_atom_score_p2h
    , g.cnt_dist_dvc_p3d
    , g.cnt_dist_dvc_p1d
    , g.cnt_dist_dvc_p2h

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
    
   

        from risk.test.risk_score_simu_v2 a
        left join risk.test.risk_score_pii_update_v2 c on (a.auth_event_id=c.auth_event_id)
        left join risk.test.risk_score_login_hist_v2 f on (a.auth_event_id=f.auth_event_id)
        left join risk.test.risk_score_atom_v2 g on (a.auth_event_id=g.auth_event_id)
        left join risk.test.risk_score_app_action_v2 l on (a.auth_event_id=l.auth_event_id)

        where 1=1

);


describe table risk.test.risk_score_final_ep_v2 ;




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
policy dev exploration query

*/


select 

width_bucket(NUNIQUE__INTNL_NETWORK_CARRIER_P7D, 0, 20, 20) as bin
, min(NUNIQUE__INTNL_NETWORK_CARRIER_P7D) as min_val, max(NUNIQUE__INTNL_NETWORK_CARRIER_P7D) as max_val
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
    and COUNT__PHONE_CHANGE_P7D>0
    and MAX_ATOM_SCORE_P3D>=0.5
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
    --and risk_score>40
    --and CNT_LOGIN_DVC_P2D>3
    --and cnt_login_dvc_p2d>2
    --and cnt_eml_chg_p3>0
    --and CNT_LOGIN_INTLNC_P3D>0
    and final_amt>=200
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



/*policy dev: phone chg p7d*/
select 
entry_type
,case when final_amt>=200 and ((COUNT__EMAIL_CHANGE_P1D>=1 and nunique__device_ids_p2h>=1) )  then 'dec 1.0'
     when final_amt>=200 and (NUNIQUE__TIMEZONES_P2H>=2)  then 'dec 1.1'
     when final_amt>=200 and MAX_ATOM_SCORE_P3D>=0.5 and (NUNIQUE__TIMEZONES_P2H>=2
                                                          or nunique__network_carriers_p2h>=2 
                                                          or NUNIQUE__AFRICA_NETWORK_CARRIERS_P7d>=1 
                                                          OR NUNIQUE__INTNL_NETWORK_CARRIER_P7D>=1
                                                          OR NUNIQUE__IPS_P1H>=2
                                                          OR NUNIQUE__DEVICE_IDS_P1H>=2
                                                          OR NUNIQUE__OS_VERSIONS_P1H>=2
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
   from risk.test.risk_score_final_ep_v2
    where 1=1
    and count__phone_change_p7d>0
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
group by 1,2
order by 1,2
;
                                                             



/*policy dev: no phone chg p7d*/
select 
entry_type
,case 
     when final_amt>=50 and risk_score>=40 and NUNIQUE__DEVICE_IDS_P1D>=3 then 'dec 1.1'
     when final_amt>=50 and risk_score>=30 and NUNIQUE__NETWORK_CARRIERS_P1D>=2 and NUNIQUE__TIMEZONES_P1D>=2 then 'dec 1.2'
     when final_amt>=50 and MAX_ATOM_SCORE_P2H>=0.45 /*or CNT_DIST_DVC_P2H>=3*/ then 'dec 1.3'
     --when final_amt>=100 and risk_score>=45 and datediff(day,last_provision_ts,auth_event_created_ts)=0 and COUNT__EVENT_LOGIN_FAILED_P2H>0 then 'dec 1.4'     
      
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
   from risk.test.risk_score_final_ep_v2
    where 1=1
    and COUNT__PHONE_CHANGE_P7D=0
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
group by 1,2
order by 1,2
;

                                                             
                                                             
                                                             
