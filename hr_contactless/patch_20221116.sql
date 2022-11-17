
/*>>>>>>>>>>
https://chime.hex.tech/global/app/1d47fa92-b5ba-4530-8095-b35f8ab71da3/latest
(rule perf report)
https://app.snowflake.com/us-west-2/chime/wqPN6ZWthSZ#query
(rule perf report underlying table logic)
context:
per the report, hr_mobilewallet_vrs_ato_suslogin_1_1's confirmed dispute rate is decent, but approved txn's dispute rate is high(>50% in Nov,2022);1_2's volume is small with low confimred dispute rate;
the purpose of this anlysis is to identify: 
    1) based on current 1_1's logic, any way to hard block those who confirmed no fraud and then dispute
    2) if there is any room to push up the confimed dispute rate for 1_2 (not doing anything in this analysis!)
Execution procedure:
    1) pull all fired population from 1_1 & incremntal only & approved(allow override) only
    2) dispute indicator
    3) append with all attributes
    4) profiling and explore ways to push up apprv auth dispute rate for hard block
*/


/*>>>> driver table building*/
create or replace table risk.test.hding_hr_mobilewallet_revamp as(

select
    rta.user_id
    , rta.auth_id
    , rta.auth_event_id
    , rta.entry_type
    , rta.mcc_cd
    , rta.auth_event_created_ts
    , rta.auth_event_merchant_name_raw
    ,case when rta.card_network_cd='Mastercard' then trim(substr(rta.auth_event_merchant_name_raw,38))
          when rta.card_network_cd='Visa' then trim(substr(rta.auth_event_merchant_name_raw,37,2))
         else null end as merchant_state
    , m.state_cd as user_state
    , rta.req_amt
    , rta.req_amt*-1 as final_amt
    , rta.response_cd
    , a.policy_name
    , rta.risk_score
    , a.decision_id
    , case when b.user_id is not null then 1 else 0 end as incremental_ind
    , case when c.user_id is not null then 1 else 0 end as declined_ind
    , case when c.user_id is not null and delivery_status='delivered' and response_signal='fraudulent' then 1 else 0 end as sms_confirmed_fraud_ind /*sms delivered, responsed with fraud*/
    , case when d.user_id is not null then 1 else 0 end as apprv_ind /*appv auth indicator*/
    , case when declined_ind=1 then 1 else d.dispute_ind end as dispute_ind /*appv auth and dispute indicator*/
    , case when declined_ind=1 then 1 else d.dispute_unauth_ind end as dispute_unauth_ind
    
    from risk.test.decplat_policy_meet a
    left join edw_db.core.dim_member m on (a.user_id=m.user_id)
    left join edw_db.core.fct_realtime_auth_event rta on (a.user_id=rta.user_id and a.auth_id=rta.auth_id)
    left join risk.TEST.decplat_policy_meet_dedup b on (a.user_id=b.user_id and a.auth_id=b.auth_id)
    left join risk.test.decplat_prompt_override_fd_perf c on (a.user_id=c.user_id and a.auth_id=c.auth_id)
    left join risk.TEST.decplat_aprv_perf d on (a.user_id=d.user_id and a.auth_id=d.auth_id)
    where 1=1
    and incremental_ind=1 and apprv_ind=1 -- !!!!!
    and a.policy_name in ('hr_mobilewallet_vrs_ato_suslogin_1_1') 
    


);

call risk.test.feature_appending('risk.test.hding_hr_mobilewallet_revamp','sp_feature','risk.test.hding_hr_mobilewallet_revamp_final');

describe table risk.test.hding_hr_mobilewallet_revamp_final;



/*basic profiling*/

select policy_name, trunc(auth_event_created_ts::date,'month') as mth
, sum(apprv_ind) as total_apprv_auth
, sum(dispute_ind)/sum(apprv_ind) as appv_disp_rate
, sum(dispute_ind) as cnt_dispute, sum(case when dispute_ind=1 then -1*req_amt else 0 end) as sum_dispute
    from risk.test.hding_hr_mobilewallet_revamp
    where 1=1
    and incremental_ind=1
    group by 1,2
    order by 1,2
;



/*apprv auth or allow override reason breakdown:
    > fl_hurricane0928_allow_2 - allow override due to rule
    > override_list_v2 - allow override due to sms confirmed no fraud
*/

with t1 as(
select a.*, b.policy_name as allow_policy_name, decision_outcome
    from risk.test.hding_hr_mobilewallet_revamp a
    left join chime.decision_platform.real_time_auth  b on (a.decision_id=b.decision_id and b.policy_actions like '%allow%' and b.policy_result='criteria_met')
    where 1=1
    and b.policy_name not like 'hr_mobilewallet_vrs_ato_%'
qualify row_number() over (partition by a.user_id, a.auth_id order by b.policy_name)=1
)
select allow_policy_name, count(*) as cnt
, sum(dispute_ind)/sum(apprv_ind) as appv_disp_rate
    from t1
    group by 1
    order by 1
;

-- among 135 auth, fl_hurricane0928_allow_2 override allowed 78 of it and rest are due to sms confimration of no fraud;

/*
ALLOW_POLICY_NAME	        CNT	APPV_DISP_RATE
fl_hurricane0928_allow_2	73	0.547945
override_list_v2	        62	0.241935
*/




/*patch strategy dev*/

select 1
--width_bucket(avg__dollar_approved_p2d, 1, 1000, 10)
--, min(avg__dollar_approved_p2d) as min_val, max(avg__dollar_approved_p2d) as max_val
, count(*) as cnt
, sum(case when a.dispute_ind=1 then -1*a.req_amt else 0 end)/sum(-1*a.req_amt) as dispute_rate_dollar
, sum(zeroifnull(a.dispute_ind))/count(*) as dispute_rate_cnt

,sum(case when a.dispute_ind=1 then -1*a.req_amt else 0 end) as sum_dispute_dollar
,sum(zeroifnull(a.dispute_ind)) as cnt_dispute
from risk.test.hding_hr_mobilewallet_revamp a
left join risk.test.hding_hr_mobilewallet_revamp_final b on (a.user_id=b.user_id and a.auth_id=b.auth_id)
where 1=1
and a.incremental_ind=1
and a.apprv_ind=1
and a.policy_name in ('hr_mobilewallet_vrs_ato_suslogin_1_1') 
--and b.count__phone_change_p7d=0

and max_atom_score_p1d>=0.5
and a.mcc_cd in ('5912','5541')
and avg__dollar_approved_p2d<140

group by 1
order by 1
;

/*
CNT	DISPUTE_RATE_DOLLAR	DISPUTE_RATE_CNT	SUM_DISPUTE_DOLLAR	CNT_DISPUTE
47	0.8833436	        0.87234	            8,062.86	        41
*/
