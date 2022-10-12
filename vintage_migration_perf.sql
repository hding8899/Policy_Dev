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
--2021-08-03	2022-09-20	4,389

select min(timestamp::date) as min_date, max(timestamp::date) as max_date, count(*) as cnt_txn from segment.chime_prod.suspected_card_pin_change_fraud;
--2017-04-06	2022-09-19	4,264

select min(timestamp::date) as min_date, max(timestamp::date) as max_date, count(*) as cnt_txn from segment.chime_prod.suspected_mobile_wallet_provisioning_fraud;
--2018-05-16	2022-06-03	10,203

select min(timestamp::date) as min_date, max(timestamp::date) as max_date, count(*) as cnt_txn from segment.chime_prod.suspected_account_takeover;
--2021-06-24	2022-09-19	762



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
    ,rae.response_cd
    ,abs(datediff(second, a.timestamp, rae.auth_event_created_ts)) as second_diff
    ,case when rae.response_cd in ('59') then coalesce(r.rules_denied,rta2.policy_name) end as rules_denied
    ,case when dt.unique_transaction_id is not null then 1 else 0 end as dispute_ind
    ,case when dt.unique_transaction_id is not null and dt.reason in ('unauthorized_transfer','unauthorized_transaction','unauthorized_external_transfer') then 1 else 0 end as dispute_unauth_ind
        from (
              select event,id,timestamp,try_to_number(user_id) as user_id,merchant_name,try_to_double(amount) as amount,shadow_mode from segment.chime_prod.suspected_account_takeover
              union all
              select event,id,timestamp,try_to_number(user_id) as user_id,merchant_name,try_to_double(amount) as amount,shadow_mode from segment.chime_prod.suspected_mobile_wallet_provisioning_fraud
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
 
 select top 10 * from risk.test.vintage_perf_ato_provision_ep;

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
    group by 1
    order by 1
;


 