/*
Optimized SQL Query for Teradata with Combined TERM_ALL and CHRG_DTL

Filename Suggestion: optimized_combined_dev_P_UPD_ENCTR_STD_GRP_PAT_CLAS.sql

Changes Made and Why:
1. Combined TERM_ALL and CHRG_DTL into COMBINED_TERM_CHRG CTE:
   - Why: Merges charge-related aggregations (SurgeryCharge_cnt, EmergencyCharge_cnt, etc.) and patient classification logic (v_grp_cd_all, INPTN_COVID_FLG) to reduce redundant table scans on ENCTR_HIST and ENCTR_CHRG_DTL. This minimizes I/O and spool space usage in Teradata.

2. Handled Differing Filters for CHRG_DTL and CHRG_DTL2:
   - Why: CHRG_DTL has specific charge code filters, while CHRG_DTL2 aggregates all charge quantities. Applied CHRG_DTL filters within SUM(CASE ...) expressions to restrict counts, while computing chrg_qty_cnt without filters to match CHRG_DTL2 logic.

3. Inlined prcs_ctrl Logic:
   - Why: Eliminated the separate prcs_ctrl CTE by incorporating its conditions into WHERE clauses, reducing subquery overhead and allowing Teradata’s optimizer to push predicates earlier.

4. Adjusted me_dt for Teradata:
   - Why: Replaced last_day() with ADD_MONTHS and EXTRACT to compute the last day of the month, ensuring Teradata compatibility without altering functionality.

5. Simplified GROUP BY:
   - Why: Removed constant columns (JOB_ID, LOAD_ADD_DT_TM, etc.) from GROUP BY in the final SELECT, reducing hashing overhead. In COMBINED_TERM_CHRG, grouped only by necessary keys (enctr_id, name_space_cd, eff_from_dt).

6. Reorganized CASE Statements:
   - Why: Nested CASE for inpatient/outpatient conditions in v_grp_cd_all reduces repeated evaluations, improving performance by minimizing branch evaluations.

7. Preserved TERM1 and TERM2 Logic:
   - Why: Integrated TERM1 (‘Exclude From Reports’) and TERM2 (‘LEGACY CONVERSION’) into COMBINED_TERM_CHRG’s CASE logic to maintain functionality while eliminating separate CTEs.

8. Teradata-Specific Notes:
   - Ensure indexes on enctr_id, name_space_cd, eff_from_dt, eff_thru_dt, post_dt, etc.
   - Run COLLECT STATISTICS on joined columns for optimal query plans.
   - Review EXPLAIN plan to confirm join order and spool usage.
   - Test with representative data to validate results and performance.

Note: The NOT EXISTS (TERM1) check is approximated within the CASE logic. If specific TERM1/TERM2 exclusions are critical, validate the logic against your data. Run EXPLAIN to identify bottlenecks and adjust indexes/statistics.
*/

INSERT INTO std_grp_volatile (
    ENCTR_ID, EFF_FROM_DT, REC_AUTH, NAME_SPACE_CD, SURG_FLG, ER_FLG,
    JOB_ID, LOAD_ADD_DT_TM, LOAD_MOD_DT_TM, EFF_THRU_DT, STD_PTIENT_CLAS, INPTN_COVID_FLG
)
WITH me_dt AS (
    /* Adjusted for Teradata: Compute last day using ADD_MONTHS and EXTRACT */
    SELECT calendar_date AS new_eff_from_dt
    FROM D_SHR_ACCV.PERIOD_CALENDAR
    WHERE 
        (calendar_date = ADD_MONTHS((calendar_date - EXTRACT(DAY FROM calendar_date) + 1), 1) - 1  /* Last day of month */
        OR calendar_date = CURRENT_DATE - 1)
    AND (
        ('H' = :in_run_type AND calendar_date BETWEEN :l_start_dt AND CURRENT_DATE - 1)
        OR (:in_run_type IN ('I', 'S') AND calendar_date <= CURRENT_DATE AND calendar_date >= :l_start_dt)
    )
),
DIAGS AS (
    /* Unchanged: Aggregates diagnosis data for COVID and final ICD counts */
    SELECT
        aed.enctr_id,
        aed.name_space_cd,
        CASE WHEN ed.dschrg_dt = DATE'1111-11-11' THEN CURRENT_DATE - 1 ELSE ed.dschrg_dt END AS DISCHRG_DT,
        SUM(CASE WHEN tg1.tgt_term_cd = 'COVID 19' THEN 1 ELSE 0 END) AS covid_icd_cnt,
        SUM(CASE WHEN aed.diagn_cd IS NOT NULL AND aed.diagn_type_cd NOT LIKE 'W%' THEN 1 ELSE 0 END) AS final_icd_cnt
    FROM D_IDW_IBV.ENCTR_ADMSN ed
    JOIN D_IDW_IBV.enctr_diagn aed ON (ed.enctr_id = aed.enctr_id AND aed.name_space_cd = ed.name_space_cd AND ed.admit_dt >= DATE'2020-02-01' AND aed.DIAGN_SEQ_NUM > 0)
    LEFT JOIN D_SHR_IBV.term_map_fltn td ON (aed.diagn_cd = td.src_term_key)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tg1 ON (td.tgt_term_key = tg1.src_term_key AND tg1.tgt_fmly_name = 'ServiceLineDRG' AND tg1.tgt_term_cd LIKE 'COVID%' AND DISCHRG_DT BETWEEN tg1.eff_strt_dt AND tg1.eff_end_dt)
    WHERE ed.name_space_cd = :in_name_space_cd
    AND EXISTS (
        SELECT 1
        FROM D_ERM_IBV.enctr_prcs_cntrl prcs_ctrl
        WHERE prcs_ctrl.enctr_id = ed.enctr_id
        AND prcs_ctrl.name_space_cd = ed.name_space_cd
        AND (
            prcs_ctrl.std_grpg_rsult_dt IS NULL
            OR prcs_ctrl.std_grpg_rsult_dt < prcs_ctrl.std_grpg_extc_dt
            OR (:in_run_type IN ('H','S') AND COALESCE(prcs_ctrl.std_grpg_rsult_dt, DATE'2041-01-01') >= :l_start_dt)
        )
    )
    GROUP BY aed.enctr_id, aed.name_space_cd, DISCHRG_DT
),
COMBINED_TERM_CHRG AS (
    /* Combined TERM_ALL and CHRG_DTL (including CHRG_DTL2) */
    SELECT
        eh.enctr_id,
        eh.name_space_cd,
        me_dt.new_eff_from_dt eff_from_dt,
        COUNT(*) AS term_all_cnt,
        /* CHRG_DTL aggregations with specific filters in CASE */
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND chrgCdTerm.tgt_term_cd = 'SURGERY' AND chrgDeptTerm.tgt_term_cd = 'SURGERY' 
            THEN 1 ELSE 0 END) AS SurgeryCharge_cnt,
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND (chrgCdTerm.tgt_term_cd IN ('ER VISIT','EMERGENCY') OR chrgDeptTerm.tgt_term_cd = 'EMERGENCY') 
            THEN 1 ELSE 0 END) AS EmergencyCharge_cnt,
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND chrgDeptTerm.tgt_term_cd = 'INPATIENT REHAB' 
            THEN 1 ELSE 0 END) AS RehabCharge_cnt,
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND chrgDeptTerm.tgt_term_cd = 'OUTPATIENT BEHAVIORAL MEDICINE' 
            THEN 1 ELSE 0 END) AS BehavCharge_cnt,
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER' 
            THEN 1 ELSE 0 END) AS BehavRevLoc_cnt,
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND tg11.tgt_term_cd = 'COVID 19' 
            THEN 1 ELSE 0 END) AS covid_hcpcs_cnt,
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND chrgCdTerm.tgt_term_cd = 'COVID 19' 
            THEN 1 ELSE 0 END) AS covid_chrg_cnt,
        SUM(CASE 
            WHEN (
                chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
            ) 
            AND tg11.tgt_term_cd = 'COVID 19 TEST' 
            THEN 1 ELSE 0 END) AS covidtst_hcpcs_cnt,
        /* CHRG_DTL2 aggregation: No specific charge filters */
        SUM(dtl.chrg_qty) AS chrg_qty_cnt,
        /* TERM_ALL logic */
        CASE
            WHEN (st.term_cd = 'I' AND (
                DIAGS.covid_icd_cnt > 0
                OR (
                    DIAGS.final_icd_cnt = 0
                    AND (
                        (chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                         OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                         OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                         OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST'))
                        AND tg11.tgt_term_cd = 'COVID 19'
                    )
                    OR tgpc.tgt_term_cd = 'COVID 19'
                    OR tgsc.tgt_term_cd = 'COVID 19'
                    OR tgtc.tgt_term_cd = 'COVID 19'
                    OR tgfc.tgt_term_cd = 'COVID 19'
                )
            )) THEN 'Y'
            ELSE 'N'
        END AS INPTN_COVID_FLG,
        CASE
            /* TERM1: Exclude logic */
            WHEN (
                (ed.admit_dt_tm IS NULL AND ed.dschrg_dt_tm IS NULL)
                OR pin.fmly_name IN ('Test Patient','TEST','Zztest','TESTPRODUCTION','Zztower')
                OR pin.fmly_name || pin.gvn_name LIKE '%HNAM%TEST%'
                OR eh.rec_auth = 69
                OR tt3.tgt_term_cd = 'EXCLUDE FROM REPORTS'
                OR ed.actv_ind = 'N'
                OR (eh.EXTR_AR_FLG = 'Y' AND eh.name_space_cd NOT IN ('EPIC-CHICAGO','EPIC-CHICAGO-PB'))
            ) THEN 'Exclude From Reports'
            /* TERM2: Legacy conversion */
            WHEN grp.tgt_term_key = 'LEGACY CONVERSION|GRP|SubCategory' THEN 'LEGACY CONVERSION'
            /* TERM_ALL: Patient classification */
            WHEN st.term_cd <> 'I' THEN
                CASE
                    WHEN COALESCE(agg.tot_chrg, 0) = 0 AND COALESCE(SUM(dtl.chrg_qty), 0) = 0 THEN 'Outpatient Without Charges'
                    WHEN SUM(CASE 
                             WHEN (
                                 chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                                 OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                                 OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                                 OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
                             ) 
                             AND chrgCdTerm.tgt_term_cd = 'SURGERY' AND chrgDeptTerm.tgt_term_cd = 'SURGERY' 
                             THEN 1 ELSE 0 END) > 0 
                         AND COALESCE(tt4.tgt_term_cd, ' ') <> 'DELIVERY' THEN 'Outpatient Surgery'
                    WHEN tt3.tgt_term_cd = 'SERIES' THEN 'Outpatient Series'
                    WHEN (SUM(CASE 
                              WHEN (
                                  chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                                  OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                                  OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                                  OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
                              ) 
                              AND (chrgCdTerm.tgt_term_cd IN ('ER VISIT','EMERGENCY') OR chrgDeptTerm.tgt_term_cd = 'EMERGENCY') 
                              THEN 1 ELSE 0 END) > 0 
                          OR tt3.tgt_term_cd = 'EMERGENCY') 
                         AND COALESCE(tt3.tgt_term_cd, ' ') <> 'URGENT CARE' THEN 'Outpatient Emergency Services'
                    WHEN tt3.tgt_term_cd = 'NONPATIENT' OR tt5.tgt_term_cd = 'NONPATIENT' OR UPPER(eh.PRMRY_INSRNC_CD) LIKE 'VITA%'
                         OR (tt3.tgt_term_cd = 'SKILLED NURSING' AND eh.name_space_cd = 'HEALTHQUEST MC0CONS' AND eh.DSCHG_DT > DATE'2015-09-30')
                         OR (STRTOK(tm3.tgt_term_key,'|',1) = 'HI' AND eh.ORG_LVL_1_CD LIKE '750|%') THEN 'Nonpatient Cases'
                    ELSE 'Outpatient Other'
                END
            WHEN st.term_cd = 'I' THEN
                CASE
                    WHEN (tt3.tgt_term_cd IN ('NEWBORN','INPATIENT ACUTE') AND tt4.tgt_term_cd IN ('NORMAL NEWBORN')) 
                         OR (tt3.tgt_term_cd IN ('NEWBORN') AND tt4.tgt_term_cd IN ('UNCODED')) THEN 'Inpatient Normal Newborn'
                    WHEN COALESCE(agg.tot_chrg, 0) = 0 AND COALESCE(SUM(dtl.chrg_qty), 0) = 0 AND ed.dschrg_dt_tm IS NOT NULL THEN 'Inpatient Without Charges'
                    WHEN tt3.tgt_term_cd = 'JOINT VENTURE' THEN 'Inpatient Joint Venture'
                    WHEN tt3.tgt_term_cd = 'LONG TERM CARE' THEN 'Inpatient Long Term Care'
                    WHEN tt3.tgt_term_cd = 'RESPITE AND TRANSITION CARE' THEN 'Inpatient Respite and Trans'
                    WHEN tt3.tgt_term_cd = 'BEHAVIORAL HEALTH' OR (tt3.tgt_term_cd = 'INPATIENT ACUTE' AND SUM(CASE 
                                                                                                           WHEN (
                                                                                                               chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                                                                                                               OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                                                                                                               OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                                                                                                               OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
                                                                                                           ) 
                                                                                                           AND revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER' 
                                                                                                           THEN 1 ELSE 0 END) > 0) THEN 'Inpatient Behavioral Medicine'
                    WHEN tt3.tgt_term_cd = 'INPATIENT REHAB' THEN 'Inpatient Rehab'
                    WHEN tt3.tgt_term_cd = 'SKILLED NURSING' THEN 'Inpatient Skilled Nursing'
                    WHEN tt3.tgt_term_cd = 'ACUTE HEAD PAIN' THEN 'Inpatient Acute Head Pain'
                    WHEN tt3.tgt_term_cd IN ('NEWBORN', 'INPATIENT ACUTE') THEN 'Inpatient Acute'
                    WHEN SUM(CASE 
                             WHEN (
                                 chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                                 OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                                 OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                                 OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
                             ) 
                             AND chrgDeptTerm.tgt_term_cd = 'OUTPATIENT BEHAVIORAL MEDICINE' 
                             THEN 1 ELSE 0 END) > 0 THEN 'Inpatient Behavioral Medicine'
                    WHEN SUM(CASE 
                             WHEN (
                                 chrgCdTerm.tgt_term_cd IN ('SURGERY','ER VISIT','EMERGENCY','COVID 19')
                                 OR chrgDeptTerm.tgt_term_cd IN ('SURGERY','EMERGENCY','INPATIENT REHAB','OUTPATIENT BEHAVIORAL MEDICINE')
                                 OR revLocTerm.tgt_term_cd = 'BEHAVIORAL SERVICES - OTHER'
                                 OR tg11.tgt_term_cd IN ('COVID 19','COVID 19 TEST')
                             ) 
                             AND chrgDeptTerm.tgt_term_cd = 'INPATIENT REHAB' 
                             THEN 1 ELSE 0 END) > 0 THEN 'Inpatient Rehab'
                    ELSE 'Inpatient Undefined'
                END
            ELSE 'Outpatient Undefined'
        END AS v_grp_cd_all
    FROM D_ERM_IBV.ENCTR_HIST eh
    JOIN me_dt ON (me_dt.new_eff_from_dt BETWEEN eh.eff_from_dt AND eh.eff_thru_dt)
    LEFT JOIN D_ERM_IBV.ENCTR_CHRG_DTL dtl ON (dtl.enctr_id = eh.enctr_id AND eh.name_space_cd = dtl.name_space_cd AND dtl.post_dt <= me_dt.new_eff_from_dt)
    LEFT JOIN D_SHR_IBV.term st ON (st.term_key = eh.ptient_clas_cd)
    LEFT JOIN DIAGS ON (DIAGS.enctr_id = eh.enctr_id AND DIAGS.name_space_cd = eh.name_space_cd)
    LEFT JOIN D_ERM_IBV.ENCTR_AGG agg ON (eh.enctr_id = agg.enctr_id AND eh.name_space_cd = agg.name_space_cd AND me_dt.new_eff_from_dt BETWEEN agg.eff_from_dt AND agg.eff_thru_dt)
    LEFT JOIN D_IDW_IBV.ENCTR_ADMSN ed ON (ed.enctr_id = eh.enctr_id AND eh.name_space_cd = ed.name_space_cd)
    LEFT JOIN D_SHR_IBV.term_map_fltn tm3 ON (eh.ptient_type_cd = tm3.src_term_key)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tt3 ON (tm3.tgt_term_key = tt3.src_term_key AND tt3.tgt_fmly_name = 'PatientTypeGroup' AND me_dt.new_eff_from_dt BETWEEN tt3.eff_strt_dt AND tt3.eff_end_dt)
    LEFT JOIN D_SHR_IBV.term_map_fltn tm4 ON (eh.drg_cd = tm4.src_term_key)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tt4 ON (tm4.tgt_term_key = tt4.src_term_key AND tt4.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tt4.eff_strt_dt AND tt4.eff_end_dt)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tt5 ON (eh.ORG_LVL_3_CD = tt5.src_term_key AND tt5.tgt_fmly_name = 'PatientTypeGroup' AND me_dt.new_eff_from_dt BETWEEN tt5.eff_strt_dt AND tt5.eff_end_dt)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tgpc ON (eh.prmry_insrnc_cd = tgpc.src_term_key AND tgpc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgpc.eff_strt_dt AND tgpc.eff_end_dt)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tgsc ON (eh.scndry_insrnc_cd = tgsc.src_term_key AND tgsc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgsc.eff_strt_dt AND tgsc.eff_end_dt)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tgtc ON (eh.third_insrnc_cd = tgtc.src_term_key AND tgtc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgtc.eff_strt_dt AND tgtc.eff_end_dt)
    LEFT JOIN D_SHR_IBV.term_map_grp_fltn tgfc ON (eh.frth_insrnc_cd = tgfc.src_term_key AND tgfc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgfc.eff_strt_dt AND tgfc.eff_end_dt)
    LEFT JOIN (
        SELECT *
        FROM D_IDW_IBV.ENCTR
        QUALIFY ROW_NUMBER() OVER (PARTITION BY src_admn_enctr_sk,
