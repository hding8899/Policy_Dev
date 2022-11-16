/*
vintage rule migration doc:
https://docs.google.com/document/d/10UNXPZHEZrwZOd12J9zN5g26UjvnjPtRSamReBjuc3E/edit#

2022.09.22
Per Beth, all 4 rules(there are 2 rules are the same) are hard deny and all denied txn can be found from following tables:

 segment.chime_prod.visa_international_monitoring_program
 segment.chime_prod.suspected_card_pin_change_fraud
 segment.chime_prod.suspected_mobile_wallet_provisioning_fraud 
 segment.chime_prod.suspected_account_takeover
 
another helpful resource is that daily atom score/all real time decplat attributes could be found from EDW_DB.FEATURE_STORE:  EDW_DB.FEATURE_STORE.USER_ID__ATOM_SCORE__2H__1D__V1

*/


/*basic profiling of vintage rule fired txn*/

select min(timestamp::date) as min_date, max(timestamp::date) as max_date, count(*) as cnt_txn from segment.chime_prod.visa_international_monitoring_program;
--2021-08-03    2022-09-20  4,389

select min(timestamp::date) as min_date, max(timestamp::date) as max_date, count(*) as cnt_txn from segment.chime_prod.suspected_card_pin_change_fraud;
--2017-04-06    2022-09-19  4,264

select min(timestamp::date) as min_date, max(timestamp::date) as max_date, count(*) as cnt_txn from segment.chime_prod.suspected_mobile_wallet_provisioning_fraud;
--2018-05-16    2022-06-03  10,203

select min(timestamp::date) as min_date, max(timestamp::date) as max_date, count(*) as cnt_txn from segment.chime_prod.suspected_account_takeover;
--2021-06-24    2022-09-19  762



/*sample 50 each*/

create table risk.test.haoding_vintage_rule_sample as(
    
select top 50
a.id, a.event, a.user_id, a.timestamp, a.amount, a.merchant_name, b.status, c.available_balance
from segment.chime_prod.visa_international_monitoring_program a
left join chime.finance.members b on (a.user_id=b.id)
left join mysql_db.galileo.galileo_customers c on (a.user_id=c.user_id)
where 1=1
and a.timestamp::date >'2022-07-01'
qualify row_number() over (partition by a.user_id order by a.id, c.available_balance desc)=1
--order by a.timestamp desc

union all

select top 50
a.id, a.event, a.user_id, a.timestamp, a.amount, a.merchant_name, b.status, c.available_balance
from segment.chime_prod.suspected_card_pin_change_fraud a
left join chime.finance.members b on (a.user_id=b.id)
left join mysql_db.galileo.galileo_customers c on (a.user_id=c.user_id)
where 1=1
and a.timestamp::date >'2022-07-01'
qualify row_number() over (partition by a.user_id order by a.id, c.available_balance desc)=1
--order by a.timestamp desc

union all

select top 50
a.id, a.event, a.user_id, a.timestamp, a.amount, a.merchant_name, b.status, c.available_balance
from segment.chime_prod.suspected_mobile_wallet_provisioning_fraud a
left join chime.finance.members b on (a.user_id=b.id)
left join mysql_db.galileo.galileo_customers c on (a.user_id=c.user_id)
where 1=1
and a.timestamp::date >'2022-01-01'
qualify row_number() over (partition by a.user_id order by a.id, c.available_balance desc)=1


union all

select top 50
a.id, a.event, a.user_id, a.timestamp, a.amount, a.merchant_name, b.status, c.available_balance
from segment.chime_prod.suspected_account_takeover a
left join chime.finance.members b on (a.user_id=b.id)
left join mysql_db.galileo.galileo_customers c on (a.user_id=c.user_id)
where 1=1
and a.timestamp::date >'2022-06-01'
qualify row_number() over (partition by a.user_id order by a.id, c.available_balance desc)=1

);


select * from risk.test.haoding_vintage_rule_sample ;




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
overlapping evalution for good perf vintage rule after FI case review:

segment.chime_prod.suspected_account_takeover
segment.chime_prod.suspected_mobile_wallet_provisioning_fraud

FI Jira:https://chime.atlassian.net/browse/FRI-449
*/


create or replace table risk.test.vintage_perf_ato_provision_ep as(
    select 
    a.*
    ,rae.auth_id
    ,rae.response_cd
    ,abs(datediff(second, a.timestamp, rae.auth_event_created_ts)) as second_diff
    --,case when rae.response_cd in ('59') then coalesce(r.rules_denied,rta2.policy_name) end as rules_denied
    ,case when rae.response_cd not in ('00','10') then coalesce(r.rules_denied,rta2.policy_name) end as rules_denied
    ,case when dt.unique_transaction_id is not null then 1 else 0 end as dispute_ind
    ,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_ind
        from (
              select event,id,convert_timezone('America/Los_Angeles',timestamp) as timestamp,try_to_number(user_id) as user_id,merchant_name,try_to_double(amount) as amount,shadow_mode from segment.chime_prod.suspected_account_takeover
              union all
              select event,id,convert_timezone('America/Los_Angeles',timestamp) as timestamp,try_to_number(user_id) as user_id,merchant_name,try_to_double(amount) as amount,shadow_mode from segment.chime_prod.suspected_mobile_wallet_provisioning_fraud
          ) a
        left join edw_db.core.fct_realtime_auth_event rae on (a.user_id=rae.user_id and a.merchant_name=(case when a.event='suspected_mobile_wallet_provisioning_fraud' then a.merchant_name else rae.auth_event_merchant_name_raw end) and a.amount=rae.req_amt)
        left join edw_db.core.fct_realtime_auth_event as dual on rae.user_id=dual.user_id and rae.auth_id=dual.original_auth_id
        left join risk.prod.disputed_transactions as dt on (dt.user_id=rae.user_id and (dt.authorization_code=rae.auth_id or dt.authorization_code=dual.auth_id))
        left join segment.chime_prod.rules_denied r on rae.auth_event_id=r.realtime_auth_event /*2018 - 2022.05.25*/ 
        left join chime.decision_platform.real_time_auth rta2 on (rae.user_id=rta2.user_id and rae.auth_id=rta2.auth_id and rta2.is_shadow_mode='false' and policy_result='criteria_met' and decision_outcome in ('hard_block','merchant_block','deny','prompt_override','sanction_block'))  /*2021.11.10 - present*/

    where 1=1
    and (rae.original_auth_id=0 or rae.original_auth_id is null)

    qualify row_number() over (partition by a.id order by abs(timestampdiff(second,a.timestamp,rae.auth_event_created_ts)))=1
)   
;




/*profiling and qc*/
select event, count(*), sum(case when second_diff is null then 1 else 0 end) as cnt_no_rae_match, sum(case when second_diff>60 then 1 else 0 end) as cnt_diff_gt1min
    from risk.test.vintage_perf_ato_provision_ep
    group by 1
    order by 1
    ;
 
 
/*ato rule by shadow and mth*/

select case when shadow_mode=FALSE or shadow_mode is null then 0 else 1 end as shadow_ind,trunc(timestamp::date, 'month') as mth, count(*) as cnt
    from segment.chime_prod.suspected_account_takeover
    where 1=1
    group by 1,2
    order by 1,2
;


-- policy(ato) launched Jun,2021, and all shadow triggers are from Jun and jul of 2021;


/*mb_fraud rule by shadow and mth*/
select case when shadow_mode=FALSE or shadow_mode is null then 0 else 1 end as shadow_ind,case when (shadow_mode is null or shadow_mode=FALSE) and timestamp::date<'2022-01-01' then trunc(timestamp::date, 'year') else trunc(timestamp::date, 'month') end as mth, count(*) as cnt
    from segment.chime_prod.suspected_mobile_wallet_provisioning_fraud
    where 1=1
    group by 1,2
    order by 1,2
;
-- policy(mb) launched May,2018, and all shadow triggers are from jul,2021; policy has not triggered any vol since 2022.06;

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
Perf Evaluation
*/

select 
event
,sum(1) as cnt_txn
/*shadow mode perf - dispute rate*/
,sum(case when shadow_mode=TRUE then 1 else 0 end) as cnt_shadow_txn
,sum(case when shadow_mode=TRUE and dispute_unauth_ind=1 then 1 else 0 end) as cnt_shadow_disp
,sum(case when shadow_mode=TRUE and dispute_unauth_ind=1 then 1 else 0 end)/sum(case when shadow_mode=TRUE then 1 else 0 end) as rate_shadow_disp_inc
,sum(case when shadow_mode=TRUE and dispute_unauth_ind=1 then -1*amount else 0 end) as sum_shadow_disp
,sum(case when shadow_mode=TRUE and dispute_unauth_ind=1 then -1*amount else 0 end)/sum(case when shadow_mode=TRUE then -1*amount else 0 end) as rate_shadow_disp_dol
/*nonshadow mode perf - dispute rate*/
,sum(case when (shadow_mode=FALSE or shadow_mode is null) then 1 else 0 end) as cnt_nonshadow_txn
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and dispute_unauth_ind=1 then 1 else 0 end) as cnt_nonshadow_disp
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and dispute_unauth_ind=1 then 1 else 0 end)/sum(case when (shadow_mode=FALSE or shadow_mode is null) then 1 else 0 end) as rate_nonshadow_disp_inc
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and dispute_unauth_ind=1 then -1*amount else 0 end) as sum_nonshadow_disp
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and dispute_unauth_ind=1 then -1*amount else 0 end)/sum(case when shadow_mode=TRUE then -1*amount else 0 end) as rate_nonshadow_disp_dol
/*nonshadow mode perf - rule trigger rate*/
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and rules_denied is not null then 1 else 0 end) as cnt_nonshadow_overlap
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and rules_denied is not null then 1 else 0 end)/sum(case when (shadow_mode=FALSE or shadow_mode is null) then 1 else 0 end) as rate_nonshadow_overlap_inc
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and rules_denied is not null then -1*amount else 0 end) as sum_nonshadow_overlap
,sum(case when (shadow_mode=FALSE or shadow_mode is null) and rules_denied is not null then -1*amount else 0 end)/sum(case when shadow_mode=TRUE then -1*amount else 0 end) as rate_nonshadow_overlap_dol

    from risk.test.vintage_perf_ato_provision_ep
    where 1=1
    --and response_cd in ('00','10')
    group by 1
    order by 1
;


-- both rules shadow mode indicated high dispute rate 
-- both rules non-shadow mode indicated super low overlapping rate
-- both rules are supposed to be migrated to decplat


/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
Rule Simulation and Recon
*/

/*>>>>>>>>>>

policy:suspected_account_takeover

ep:
    - 2022.07-09(125 triggered in prod; jul-74, aug-16, sep-35)
    - risk_score>40
    - Entry Type in ('Magnetic Stripe', 'Contactless')
    - Card Present = 1
    - MCC in ('5411') - grocery/supermarket
    - Transaction State != User State
    - past 24 hours login
    - past 24 hours phone chg

*/

create or replace table risk.test.suspected_account_takeover_simu as (

    select 
        rae.auth_id
        ,rae.auth_event_id
        ,rae.auth_event_created_ts

        ,rae.user_id
        ,m.state_code as user_state
        ,case when rae.card_network_cd='Mastercard' then trim(substr(rae.auth_event_merchant_name_raw,38))
              when rae.card_network_cd='Visa' then trim(substr(rae.auth_event_merchant_name_raw,37,2))
         else null end as merchant_state
            
        ,case when user_state<>merchant_state then 1 else 0 end as user_mrch_state_diff
    
        ,rae.response_cd

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
        
        , b.session_timestamp as login_ts
        , c.created_at as phone_chg_ts
    
        ,case when dt.unique_transaction_id is not null then 1 else 0 end as dispute_ind
        ,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_ind
        from edw_db.core.fct_realtime_auth_event rae
        left join edw_db.core.fct_realtime_auth_event as dual on rae.user_id=dual.user_id and rae.auth_id=dual.original_auth_id
        left join risk.prod.disputed_transactions as dt on (dt.user_id=rae.user_id and (dt.authorization_code=rae.auth_id or dt.authorization_code=dual.auth_id))
        left join chime.finance.members m on (rae.user_id=m.id)
        left join ml.model_inference.ato_login_alerts b on (rae.user_id=b.user_id and b.session_timestamp between dateadd(hour,-24,rae.auth_event_created_ts) and rae.auth_event_created_ts)
        --left join analytics.looker.versions_pivot c on (rae.user_id=c.item_id and c.created_at between dateadd(hour,-24,rae.auth_event_created_ts) and rae.auth_event_created_ts and c.item_type='User' and (c.object='phone' or (c.object='status' and c.change_from='needs_enrollment' and c.change_to='active')))
        left join analytics.looker.versions_pivot c on (rae.user_id=c.item_id and c.created_at between dateadd(hour,-24,rae.auth_event_created_ts) and rae.auth_event_created_ts and c.item_type='User' and c.object='phone')
    
        --left join segment.chime_prod.phone_number_updated c on (rae.user_id=c.user_id and c.timestamp between dateadd(hour,-24,rae.auth_event_created_ts) and rae.auth_event_created_ts)
        where 1=1
        and trunc(rae.auth_event_created_ts::date,'month') between '2022-07-01' and '2022-09-01'
        and rae.risk_score>40
        and rae.is_card_present=TRUE
        and rae.entry_type in ('Magnetic Stripe', 'Contactless')
        and rae.mcc_cd in ('5411')
        and rae.original_auth_id=0 and rae.original_response_cd in ('00','10')
    
        qualify row_number() over (partition by rae.user_id, rae.auth_id order by b.session_timestamp desc, c.created_at desc)=1

);


/*basic profiling*/
select trunc(auth_event_created_ts::date,'month'), count(*) as cnt, count(case when login_ts is not null and phone_chg_ts is not null then auth_id end) as cnt_trigger_simu
from risk.test.suspected_account_takeover_simu
where 1=1
group by 1
order by 1
;

select top 100 * 
from risk.test.suspected_account_takeover_simu;




/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

policy:suspected_mobile_wallet_provisioning_fraud

ep:
    - 2022.03-05(484 triggered in prod; mar-160, apr-190, may-134)
    - past 48 hour visa provisioning event
    - mcc in ('5310', '5732', '5311', '5411', '5912', '5331')
    - card present
    - Entry Type = 'Magnetic Stripe'
    - Transaction State = 'FL'
    - Transaction State != User State
*/

create or replace table risk.test.suspected_mb_provisioning_simu as (
    with t1 as(
    select 
        rae.auth_id
        ,rae.auth_event_id
        ,rae.auth_event_created_ts

        ,rae.user_id
        ,coalesce(c.change_from,m.state_code) as user_state
        ,case when rae.card_network_cd='Mastercard' then trim(substr(rae.auth_event_merchant_name_raw,38))
              when rae.card_network_cd='Visa' then trim(substr(rae.auth_event_merchant_name_raw,37,2))
         else null end as merchant_state
            
        ,case when user_state<>merchant_state then 1 else 0 end as user_mrch_state_diff
    
        ,rae.response_cd

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
        
        ,case when (1=1
                and rae.is_card_present=TRUE
                and rae.entry_type in ('Magnetic Stripe')
                and rae.mcc_cd in ('5310', '5732', '5311', '5411', '5912', '5331')
                and merchant_state='FL'
                and user_mrch_state_diff=1
                ) then 'fl mb' else 'us mb' end as sub_rule_nm
        ,rae.original_response_cd
        
        ,case when dt.unique_transaction_id is not null then 1 else 0 end as dispute_ind
        ,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_ind

        from edw_db.core.fct_realtime_auth_event rae
        left join edw_db.core.fct_realtime_auth_event as dual on rae.user_id=dual.user_id and rae.auth_id=dual.original_auth_id
        left join risk.prod.disputed_transactions as dt on (dt.user_id=rae.user_id and (dt.authorization_code=rae.auth_id or dt.authorization_code=dual.auth_id))
        left join chime.finance.members m on (rae.user_id=m.id)
        left join analytics.looker.versions_pivot c on (rae.user_id=c.item_id and c.object='state_code' and c.item_type='User')
        where 1=1
        and rae.auth_event_created_ts::date between '2022-06-10' and '2022-08-31'
        and rae.original_auth_id=0 and rae.original_response_cd in ('00','10') /*remove dual auth and galloleo originally declined auth*/
        and(    /*fl mb fraud*/
                (1=1
                and rae.is_card_present=TRUE
                and rae.entry_type in ('Magnetic Stripe')
                and rae.mcc_cd in ('5310', '5732', '5311', '5411', '5912', '5331')
                and merchant_state='FL'
                and user_mrch_state_diff=1
                )
            or /*mb fraud*/
                (1=1
                and rae.is_card_present=TRUE
                and rae.entry_type in ('Magnetic Stripe', 'Contactless')
                and rae.mcc_cd in ('5411')
                and rae.risk_score>45
                and user_mrch_state_diff=1
                
                )
        )
        
        qualify row_number() over (partition by rae.user_id, rae.auth_id order by c.created_at)=1
        
        
    )
    select a.*, case when sub_rule_nm='fl mb' then b.auth_event_created_ts 
                     when sub_rule_nm='us mb' and datediff('hour',b.auth_event_created_ts,a.auth_event_created_ts)<=8 then b.auth_event_created_ts 
                end as provision_ts
              ,case when c.created_at is not null then 1 else 0 end as phone_chg_ind_p7d
              ,c.created_at as lst_phone_chg_ts_p7d
        from t1 a
        inner join edw_db.core.fct_realtime_auth_event b on (a.user_id=b.user_id and b.auth_event_created_ts between dateadd(hour,-48,a.auth_event_created_ts) and a.auth_event_created_ts and b.auth_event_merchant_name_raw ilike '%Visa Provisioning%' )
        left join analytics.looker.versions_pivot c on (a.user_id=c.item_id and c.created_at between dateadd(day,-7,a.auth_event_created_ts) and a.auth_event_created_ts and c.object='phone')
       
    qualify row_number() over (partition by a.user_id, a.auth_id order by b.auth_event_created_ts desc, c.created_at desc)=1
);



/*basic profiling*/
select trunc(auth_event_created_ts::date,'month'), sum(case when provision_ts is not null then 1 else 0 end) as cnt
from risk.test.suspected_mb_provisioning_simu
where 1=1
group by 1
order by 1
;

select phone_chg_ind_p7d
,entry_type
, case when response_cd in ('00','10') then 'appv auth' else 'blocked auth' end as auth_response_info
, count(*) as cnt
, sum(case when dispute_ind=1 then 1 else 0 end) as cnt_dispute
, sum(case when dispute_unauth_ind=1 then 1 else 0 end) as cnt_dispute_unauth

, sum(case when dispute_ind=1 then 1 else 0 end)/cnt as rate_dispute_inc
, sum(case when dispute_unauth_ind=1 then 1 else 0 end)/cnt as rate_dispute_unauth_inc

, sum(case when dispute_ind=1 then -1*req_amt else 0 end)/sum(-1*req_amt) as rate_dispute_dollar
, sum(case when dispute_unauth_ind=1 then -1*req_amt else 0 end)/sum(-1*req_amt) as rate_dispute_unauth_dollar
from risk.test.suspected_mb_provisioning_simu
where 1=1
and provision_ts is not null
group by 1,2,3
order by 1,2,3
;






/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

prod recon

*/
with prod as(
    select *
    from risk.test.vintage_perf_ato_provision_ep
    where 1=1
    and event='suspected_account_takeover'
    and trunc(timestamp,'month') between '2022-07-01' and '2022-09-01'
    
    --and event='suspected_mobile_wallet_provisioning_fraud'
    --and trunc(timestamp,'month') between '2022-03-01' and '2022-05-01'
    
), simu as(
    --/*
    select * 
    from risk.test.suspected_account_takeover_simu
    where 1=1
    and login_ts is not null and phone_chg_ts is not null
    and user_mrch_state_diff=1
    --and is_international=FALSE
    --*/
    /*
    select * 
    from risk.test.suspected_mb_provisioning_simu
    where 1=1
    and provision_ts is not null
    */
)
select 
case when a.user_id is null then 'simu only'
     when b.user_id is null then 'prod only'
     else 'both' end as cat 
,coalesce(a.user_id,b.user_id) as user_id
,coalesce(a.auth_id,b.auth_id) as auth_id
,coalesce(a.timestamp, b.auth_event_created_ts) as auth_ts
,coalesce(a.merchant_name,b.auth_event_merchant_name_raw) as mrch_nm
,coalesce(a.amount,b.req_amt) as amt
,b.*
    from prod a 
    full outer join simu b on (a.user_id=b.user_id and a.auth_id=b.auth_id)
order by 1
;


/*dispute rate by different recon cat*/

with prod as(
    select *
    from risk.test.vintage_perf_ato_provision_ep
    where 1=1
    --/*
    and event='suspected_account_takeover'
    and trunc(timestamp,'month') between '2022-07-01' and '2022-09-01'
    --*/
    
    /*
    and event='suspected_mobile_wallet_provisioning_fraud'
    and trunc(timestamp,'month') between '2022-03-01' and '2022-05-01'
    */
), simu as(
    --/*
    select * 
    from risk.test.suspected_account_takeover_simu
    where 1=1
    and login_ts is not null and phone_chg_ts is not null
    and user_mrch_state_diff=1
    --*/
    
    /*
    select * 
    from risk.test.suspected_mb_provisioning_simu
    where 1=1
    and provision_ts is not null
    */
), t1 as(
select 
case when a.user_id is null then 'simu only'
     when b.user_id is null then 'prod only'
     else 'both' end as cat 
,coalesce(a.user_id,b.user_id) as user_id
,coalesce(a.auth_id,b.auth_id) as auth_id
,coalesce(a.timestamp, b.auth_event_created_ts) as auth_ts
,coalesce(a.merchant_name,b.auth_event_merchant_name_raw) as mrch_nm
,coalesce(a.amount,b.req_amt) as amt
,b.response_cd
    from prod a 
    full outer join simu b on (a.user_id=b.user_id and a.auth_id=b.auth_id)
    where 1=1
    --and cat='simu only'
)

select b.user_id, b.auth_id
, count(*) as cnt_decplat
, sum(case when policy_result='criteria_met' then 1 else 0 end) as cnt_decplat_criteria_met
, sum(case when policy_result='criteria_met' and decision_outcome in ('hard_block','merchant_block','deny','prompt_override','sanction_block') then 1 else 0 end) as cnt_decplat_by_authrules
    from chime.decision_platform.real_time_auth a
    right join (select distinct user_id, auth_id from t1 where 1=1 and cat<>'simu only') b on (a.user_id=b.user_id and a.auth_id=b.auth_id)
    where 1=1
    group by 1,2
;


/*
select cat
,count(*) as cnt_all
,sum(case when dispute_ind=1 then 1 else 0 end) as cnt_disp
,sum(case when dispute_unauth_ind=1 then 1 else 0 end) as cnt_disp_unauth
,cnt_disp/nullifzero(cnt_all) as rate_disp
,cnt_disp_unauth/nullifzero(cnt_all) as rate_disp_unauth

,sum(case when response_cd in ('00','10') then 1 else 0 end) as cnt_appv
,sum(case when response_cd in ('00','10') and dispute_ind=1 then 1 else 0 end) as cnt_disp2
,sum(case when response_cd in ('00','10') and dispute_unauth_ind=1 then 1 else 0 end) as cnt_disp_unauth2
,cnt_disp/nullifzero(cnt_appv) as rate_disp2
,cnt_disp_unauth/nullifzero(cnt_appv) as rate_disp_unauth2
    from t1
    group by 1
;
*/


select *
    from risk.test.vintage_perf_ato_provision_ep
    where 1=1
    and user_id=20031287
    ;

select *
    from chime.decision_platform.real_time_auth
    where 1=1
    and user_id=20031287
    and original_timestamp::date='2022-09-27'
    and decision_id='94b06d60-3a66-4f7a-a7ca-814c92074d23'
    order by original_timestamp desc
;

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
For individual case review
*/

/*rae view*/

select auth_event_created_ts, req_amt, risk_score, entry_type, auth_event_merchant_name_raw, mcc_cd ,m.state_code as user_state
, case when rae.card_network_cd='Mastercard' then trim(substr(rae.auth_event_merchant_name_raw,38))
              when rae.card_network_cd='Visa' then trim(substr(rae.auth_event_merchant_name_raw,37,2))
         else null end as merchant_state
, is_card_present, mti_cd, original_auth_id, original_response_cd, response_cd
    from edw_db.core.fct_realtime_auth_event rae
    left join chime.finance.members m on (rae.user_id=m.id)
    where 1=1
    and user_id=38487028
    and auth_id=20940771
;

/*provisioning view*/

select *
    from edw_db.core.fct_realtime_auth_event
    where 1=1
    and auth_event_merchant_name_raw ilike '%Visa Provisioning%'
    and auth_event_created_ts between dateadd(hour,-60,to_timestamp('2022-03-12 11:41:37')) and to_timestamp('2022-03-12 11:41:37')
    and user_id=17516391
    order by auth_event_created_ts desc
;


/*phone chg view*/
with t1 as(

select rae.user_id, auth_event_created_ts, req_amt, risk_score, entry_type, auth_event_merchant_name_raw, mcc_cd ,m.state_code as user_state
, case when rae.card_network_cd='Mastercard' then trim(substr(rae.auth_event_merchant_name_raw,38))
              when rae.card_network_cd='Visa' then trim(substr(rae.auth_event_merchant_name_raw,37,2))
         else null end as merchant_state
    from edw_db.core.fct_realtime_auth_event rae
    left join chime.finance.members m on (rae.user_id=m.id)
    where 1=1
    and user_id=43823924
    and auth_id=209474466

)
select a.auth_event_created_ts, b.*
    from t1 a
    left join segment.chime_prod.phone_number_updated b on (a.user_id=b.user_id and b.timestamp between dateadd(hour,-24,a.auth_event_created_ts) and a.auth_event_created_ts)
    order by b.timestamp desc
;

select *
from segment.chime_prod.phone_number_updated
where user_id=20031287
order by timestamp desc
;


select *
    from analytics.looker.versions_pivot
    where 1=1
    and item_id=36989487
    order by created_at desc
;

/*login view*/

select *
    from edw_db.feature_store.atom_user_sessions_v2
    where 1=1
    and user_id=20031287
    order by session_timestamp desc
;


select convert_timezone('America/Los_Angeles',a.session_timestamp) as session_ts, convert_timezone('America/Los_Angeles',a.timestamp) as ts,a.*
    from ml.model_inference.ato_login_alerts a
    where 1=1
    and user_id=36892137
    order by a.session_timestamp desc
;


select *
    from segment.chime_prod.login_success 
    where 1=1
    and user_id=11608038
    order by timestamp desc
;

--------------------------------------------

