with first_joined as (
SELECT * FROM
  (SELECT distinct binary_user_id , first_value(date_joined)over w as first_date_joined
  FROM `business-intelligence-240201.sandbox.bo_audit_signup`  where broker = 'vr'
  WINDOW w AS (PARTITION BY binary_user_id ORDER BY stamp ROWS between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING))
  WHERE first_date_joined>'2019-01-01'
),
mt5 as (
  SELECT mt5_login.binary_user_id , mt5_login.creation_stamp , mt5_login.loginid 
  FROM `business-intelligence-240201.bi.mt5_to_binary_user` as mt5_login
  WHERE mt5_login.mt5_broker <> 'MTD' 
  AND mt5_login.creation_stamp >= '2019-01-01'
),
vr_features as(
SELECT distinct 
  first_joined.binary_user_id , 
  first_joined.first_date_joined, 
  count(*) over w as count_audit_rows_vr_account,
  count(distinct(loginid))over (PARTITION BY vr.binary_user_id) as number_of_vr_accounts,
  first_value(email_domain) over w as signup_email_domain, 
  case when(length(last_value(myaffiliates_token)over w) =32)then 1 else 0 end as vr_affiliated_signup,
  case when (last_value(residence)over w is not null) THEN 1 ELSE 0 END as vr_residence ,
  case when (last_value(citizen)over w is not null) THEN 1 ELSE 0 END as vr_citizen,
  case when (last_value(place_of_birth)over w is not null) THEN 1 ELSE 0 END as vr_place_of_birth,
  case when (last_value(date_of_birth)over w is not null) THEN 1 ELSE 0 END as vr_date_of_birth,
  case when (last_value(gender)over w is not null) THEN 1 ELSE 0 END as vr_gender,
  last_value(residence) over w as vr_residence_value,
  last_value(citizen)over w as vr_citizen_value,
  last_value(place_of_birth)over w as vr_place_of_birth_value,
  last_value(date_of_birth)over w as vr_date_of_birth_value,
  last_value(gender) over w as vr_gender_value

--   cast(DATE_DIFF (CURRENT_DATE  ,DATE (date_of_birth) , year)/10 as INT64) as vr_age_in_decade,
FROM first_joined
LEFT JOIN `business-intelligence-240201.sandbox.bo_audit_signup` as vr ON first_joined.binary_user_id = vr.binary_user_id
where broker = 'vr' 
  AND stamp <= TIMESTAMP_ADD(first_joined.first_date_joined, INTERVAL 24 HOUR) AND vr.operation <> 'DELETE'
WINDOW w AS (PARTITION BY vr.binary_user_id ORDER BY stamp ROWS between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)
),
real_ as (
SELECT 
  distinct first_joined.binary_user_id , 
  count(*) over w as count_audit_rows_real_account,
  count(distinct(loginid))over (PARTITION BY real.binary_user_id) as number_of_real_accounts, 
  case when (last_value(residence)over w is not null) then 1 else 0 END as real_residence, 
  case when (last_value(date_of_birth)over w is not null) then 1 else 0 END as real_date_of_birth,
  cast(DATE_DIFF (CURRENT_DATE  ,DATE (last_value(date_of_birth)over w) , year)/10 as INT64) as real_age_in_decade,
  case when (last_value(citizen)over w is not null) THEN 1 ELSE 0 END AS real_citizen,
  case when (last_value(place_of_birth)over w is not null) THEN 1 ELSE 0 END AS real_place_of_birth,
  last_value(gender)over w as real_gender ,
  cast(TIMESTAMP_DIFF (first_value(date_joined)over w  , first_date_joined , hour) as int64) as hours_from_vr_to_real,
  last_value (allow_login) over w as real_allow_login,
  last_value (aml_risk_classification) over w as real_aml_risk_classification,
  last_value (myaffiliates_token_registered) over w as real_myaffiliates_token_registered, 
  last_value(first_time_login) OVER w AS real_first_time_login,
  last_value(checked_affiliate_exposures) OVER w AS real_checked_affiliate_exposures,
  last_value(source) OVER w AS real_source,
  last_value(account_opening_reason) OVER w AS real_account_opening_reason,
  last_value(residence)over w as real_residence_value,
  last_value(citizen)over w as real_citizen_value,
  last_value(place_of_birth)over w as real_place_of_birth_value,
  last_value(date_of_birth)over w real_date_of_birth_value,
  last_value(gender)over w as real_gender_value
  
  --   case when (last_value(gender)over w is not null) THEN 1 ELSE 0 END AS real_gender,
FROM first_joined
LEFT JOIN `business-intelligence-240201.sandbox.bo_audit_signup` as real ON first_joined.binary_user_id = real.binary_user_id
where broker in ('cr' )  
  AND stamp <= TIMESTAMP_ADD(first_joined.first_date_joined, INTERVAL 24 HOUR) 
  AND real.operation <> 'DELETE'
WINDOW w AS (PARTITION BY real.binary_user_id ORDER BY stamp ROWS between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)
),
status as (
SELECT 
  binary_user_id , 
  sum(status_value) as value_inserted_statuses
FROM (
    SELECT 
      auc.binary_user_id ,
      first_joined.first_date_joined , 
      auc.status_code, 
      auc.operation,
      (CASE
        WHEN REGEXP_CONTAINS(auc.status_code, 'crs_tin_information|tnc_approval|financial_risk_approval|allow_document_upload|professional')  THEN 1
        WHEN REGEXP_CONTAINS(auc.status_code,'document_under_review|age_verification|proveid_requested|ukgc_authenticated|max_turnover_limit_not_set') THEN 1
        WHEN REGEXP_CONTAINS(auc.status_code,'address_verified|professional_requested|ukrts_max_turnover_limit_not_set')  THEN 1
        WHEN REGEXP_CONTAINS(auc.status_code,'withdrawal_locked|unwelcome|cashier_locked|disabled|no_withdrawal_or_trading|closed|no_trading') THEN -1
        ELSE 0 END) as status_value
    from first_joined
    join `business-intelligence-240201.sandbox.bo_audit_client_status` as auc
      on first_joined.binary_user_id = auc.binary_user_id 
      and auc.stamp <= TIMESTAMP_ADD(first_joined.first_date_joined , INTERVAL 24 HOUR)
      and auc.client_loginid not like 'V%'
    group by 1,2,3,4,5)
where operation = 'INSERT'
group by binary_user_id
),
docs as (
select 
	first_joined.binary_user_id, 
  countif(acad.operation = 'INSERT')as number_of_inserted_documents
from `business-intelligence-240201.sandbox.bo_audit_client_authentication_document` as acad
join first_joined on first_joined.binary_user_id = acad.binary_user_id 
where acad.stamp <= TIMESTAMP_ADD(first_joined.first_date_joined , INTERVAL 24 HOUR) 
group by 1
)
SELECT distinct
  vr_features.binary_user_id, 
  DATE(vr_features.first_date_joined) as first_date_joined,
  case when (mt5.loginid is not null) THEN '1' ELSE '0' END AS has_mt5,
  vr_features.signup_email_domain,
  vr_features.count_audit_rows_vr_account ,
  real_.count_audit_rows_real_account ,
  vr_features.number_of_vr_accounts ,
  real_.number_of_real_accounts ,
  vr_features.vr_residence ,
--   real_.real_residence,
  vr_features.vr_citizen ,
  real_.real_citizen ,
  vr_features.vr_place_of_birth,
  real_.real_place_of_birth ,
--   vr_features.vr_date_of_birth,
  real_.real_age_in_decade,
  vr_features.vr_gender ,
  real_.real_gender ,
  vr_features.vr_affiliated_signup,
  real_.real_myaffiliates_token_registered,
  real_.hours_from_vr_to_real, 
  real_.real_allow_login , 
  real_.real_aml_risk_classification, 
  real_.real_first_time_login , 
  real_.real_checked_affiliate_exposures , 
  real_.real_source, 
  real_.real_account_opening_reason ,
  status.value_inserted_statuses,
  docs.number_of_inserted_documents,
  
  case when (vr_features.vr_residence_value <>real_.real_residence_value) THEN '1' ELSE '0' END as residence_changed,
  case when (vr_features.vr_citizen_value <>real_.real_citizen_value) THEN '1' ELSE '0' END as citizen_changed,
  case when (vr_features.vr_place_of_birth_value <>real_.real_place_of_birth_value) THEN '1' ELSE '0' END as place_of_birth_changed,
  case when (vr_features.vr_date_of_birth_value <>real_.real_date_of_birth_value) THEN '1' ELSE '0' END as date_of_birth_changed,
  case when (vr_features.vr_gender_value <>real_.real_gender_value) THEN '1' ELSE '0' END as gender_changed

FROM vr_features
LEFT JOIN real_
on real_.binary_user_id =vr_features.binary_user_id
LEFT JOIN mt5 
  on mt5.binary_user_id = vr_features.binary_user_id
  AND mt5.creation_stamp <= TIMESTAMP_ADD(vr_features.first_date_joined, INTERVAL 24 HOUR)
LEFT JOIN status
  ON status.binary_user_id = vr_features.binary_user_id
LEFT JOIN docs
  on docs.binary_user_id = vr_features.binary_user_id 
where vr_features.binary_user_id is not null

