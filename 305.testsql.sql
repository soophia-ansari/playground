select
  a.org_id as "ACO ID",
  attribution_type as "Attribution Type",
  attribution_curr_period_flag as "Attributed at Period Flag",
  assignable_curr_period_flag as "Assignable at Period Flag",
  at_time_network_nh as "Attributed Parent Group ID",
  at_time_tin_fac_nh as "Attributed Group ID",
  e.tin_name as "Attributed Group Name",
  g.network_name as "Attributed Parent Group Name",
  at_time_primary_prov_nh as "Attributed Provider",
  d.name as "Attributed Provider Name",
  assgn_medicare_status_cd as "Medicare Status Code",
  patient_cohort as "Medicare Patient Cohort",
  risk_score as "HCC Score",
  patient_frailty_group as "Frailty Group",
  pk_ip_stay_id as "Inpatient Stay ID",
  fk_patient_id as "Patient ID",
  fk_facility_id as "Facility ID",
  coalesce(f.name,split_part(a.fk_facility_id,'|',2)) as "Facility Name",
  primary_rendering_provider_id as "Primary Provider ID",
  stay_from_dt as "Stay From Date",
  stay_thru_dt as "Stay Through Date",
  stay_length_of_stay as "Stay Length in Days",
  a.ip_type as "Inpatient Stay Type",
  a.ed_flag as "ER Admission Flag",
  a.readmission_flag as "Stay has Readmission Flag",
  a.cost_within_30_days as "Total Cost within 30 Days of Stay",
  a.pcp_visit_within_30_days as "PCP Visit within 14 Days of Inpatient Stay Flag",
  a.pcp_tin_visit_within_30_days as "PCP in TIN Visit within 14 Days of Inpatient Stay Flag",
  tcm_eligible_moderate_complexity as "TCM Eligible Moderate Complexity Flag",
  tcm_eligible_high_complexity as "TCM Eligible High Complexity Flag",
  tcm_compliant as "TCM Compliant Flag",
  a.total_cost as "Total Paid Amount",
  get(a.claim_drg_cd_list,0)::string as "DRG Code",
  c.drg_mdc_cd as "DRG MDC Code",
  c.label as "DRG Label",
  get(stay_admission_source_cd_list,0)::string as "Admission Source Code List",
  get(stay_admission_type_cd_list,0)::string as "Admission Type Code List",
  dgc.discharge_label as "Stay Discharge Label",
  coalesce(df.name,get(a.discharge_facility_id,0)::string) as "Discharge Facility Name",
  ahrq_pqi_measure_cd as "AHRQ PQI Avoidable Measure Code",
  planned_admission_flag as "Planned Admission Flag",
  get(fk_diagnosis_id_list,0)::string as "Diagnosis ID List",
  index_admission_flag as "Stay is Readmission Flag",
  associated_part_b_claim_paid_amt as "Associated Part B Cost",
  fk_pac_ip_stay_id as "Post Acute Care Stay ID",
  readmission_ip_stay_id as "Readmission Inpatient Stay ID",
  rendering_facility_network_id_list as "Rendering Facility Network List",
  rendering_provider_network_id_list as "Rendering Provider Network List",
  get(rendering_tin_network_id_list,0)::string as "Rendering TIN Network List",
  primary_rendering_facility_network_id as "Primary Rendering Facility Network List",
  primary_rendering_provider_network_id as "Primary Rendering Provider Network List",
  primary_rendering_tin_network_id as "Primary Rendering TIN Network List",
  split_part(get(a.discharge_facility_id,0)::string,'|',2) as "Discharge Facility ID"
FROM insights.metric_value_hosp_ip a
left join (select * from insights.provider where name is not null) d
on d.pk_provider_id = a.at_time_primary_prov_nh
and d.org_id = a.org_id
left join (select distinct org_group_id, tin_name from insights.metric_value_denormalized where tin_name is not null and org_level_category_cd = 'at_time_tin'
) e
on e.org_group_id = a.at_time_tin_fac_nh
left join (select distinct org_group_id, network_name from insights.metric_value_denormalized where network_name is not null and org_level_category_cd = 'at_time_network'
) g
on g.org_group_id = a.at_time_network_nh
left join (SELECT value AS dschg_code, label as discharge_label FROM dev_common_fe.ref.code WHERE lower(type_cd) LIKE '%dschrg%') dgc
        on dgc.dschg_code = get(a.stay_discharge_status_cd_list,0)::string
left join (select * from insights.facility where name <> 'Unknown') f
on f.pk_facility_id = a.fk_facility_id
left join (select * from insights.facility where name <> 'Unknown') df
on df.pk_facility_id = get(a.discharge_facility_id,0)::string
left join dev_common_fe.ref.code_drg c
 on get(a.claim_drg_cd_list,0) = c.value
order by 1

select
  at_time_network_nh as "Attributed Parent Group ID",
  at_time_tin_fac_nh as "Attributed Group ID",
  e.tin_name as "Attributed Group Name",
  g.network_name as "Attributed Parent Group Name",
  at_time_primary_prov_nh as "Attributed Provider",
  get(a.claim_drg_cd_list,0)::string as "DRG Code",
  c.label as "DRG Label",
    patient_frailty_group as "Frailty Group",
  pk_ip_stay_id as "Inpatient Stay ID",
  patient_cohort as "Medicare Patient Cohort",
   associated_part_b_claim_paid_amt as "Associated Part B Cost"
   FROM insights.metric_value_hosp_ip a
      left join (select distinct org_group_id, tin_name 
        from insights.metric_value_denormalized 
        where tin_name is not null and org_level_category_cd = 'at_time_tin'
        ) e
           on e.org_group_id = a.at_time_tin_fac_nh
     left join (select distinct org_group_id, network_name 
        from insights.metric_value_denormalized 
        where network_name is not null and org_level_category_cd = 'at_time_network'
        ) g
          on g.org_group_id = a.at_time_network_nh
      left join {{env}}_COMMON_FE.ref.code_drg c
          on get(a.claim_drg_cd_list,0) = c.value
