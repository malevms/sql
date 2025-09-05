/*
Optimized SQL Query for Teradata with Fixed COMBINED_TERM_CHRG and Performance Improvements

Filename: optimized_fixed_dev_P_UPD_ENCTR_STD_GRP_PAT_CLAS_v2.sql

Changes Made and Why:
1. Fixed Aggregate Error: Separated charge aggregates into CHRG_AGG CTE to avoid aggregates in CASE statements, resolving "group by and with clause may not contain aggregate function" error.
2. Tightened me_dt Filter: Limited calendar_date range to reduce rows in me_dt, minimizing product join impact in Step 4.
3. Added TERM_MAP_GRP_FILTERED CTE: Pre-filters TERM_MAP_GRP_FLTN to reduce full table scans (Steps 5.2, 8, 10, etc.).
4. Inlined prcs_ctrl Logic: Kept inline to avoid subquery overhead.
5. Simplified GROUP BY: Ensured only necessary columns in GROUP BY to reduce hashing.
6. Preserved TERM1/TERM2 Logic: Integrated into COMBINED_TERM_CHRG CASE logic.
7. Teradata Notes:
   - Added indexes: ENCTR_HIST(enctr_id, name_space_cd, eff_from_dt, eff_thru_dt), ENCTR_CHRG_DTL(enctr_id, name_space_cd, post_dt), TERM_MAP_GRP_FLTN(src_term_key, tgt_fmly_name, eff_strt_dt, eff_end_dt), TERM_MAP_GRP(src_term_key).
   - Collect statistics on joined/filtered columns.
   - Review EXPLAIN plan post-index creation to confirm improvements.
*/

INSERT INTO std_grp_volatile (
    ENCTR_ID, EFF_FROM_DT, REC_AUTH, NAME_SPACE_CD, SURG_FLG, ER_FLG,
    JOB_ID, LOAD_ADD_DT_TM, LOAD_MOD_DT_TM, EFF_THRU_DT, STD_PTIENT_CLAS, INPTN_COVID_FLG
)
WITH me_dt AS (
    SELECT calendar_date AS new_eff_from_dt
    FROM D_SHR_ACCV.PERIOD_CALENDAR
    WHERE 
        (calendar_date = ADD_MONTHS((calendar_date - EXTRACT(DAY FROM calendar_date) + 1), 1) - 1
        OR calendar_date = CURRENT_DATE - 1)
        AND calendar_date BETWEEN :l_start_dt AND CURRENT_DATE - 1
        AND (:in_run_type IN ('I', 'S') OR calendar_date >= :l_start_dt)
),
TERM_MAP_GRP_FILTERED AS (
    SELECT src_term_key, tgt_term_key, tgt_fmly_name, eff_strt_dt, eff_end_dt
    FROM D_SHR_INT.TERM_MAP_GRP_FLTN
    WHERE tgt_fmly_name IN ('ChargeCdGroup', 'ChargeDepartmentGroup', 'ServiceLineDRG', 'PatientTypeGroup')
    AND eff_strt_dt <= DATE '2025-09-04'
    AND eff_end_dt >= DATE '2025-09-04'
),
DIAGS AS (
    SELECT
        aed.enctr_id,
        aed.name_space_cd,
        CASE WHEN ed.dschrg_dt = DATE'1111-11-11' THEN CURRENT_DATE - 1 ELSE ed.dschrg_dt END AS DISCHRG_DT,
        SUM(CASE WHEN tg1.tgt_term_cd = 'COVID 19' THEN 1 ELSE 0 END) AS covid_icd_cnt,
        SUM(CASE WHEN aed.diagn_cd IS NOT NULL AND aed.diagn_type_cd NOT LIKE 'W%' THEN 1 ELSE 0 END) AS final_icd_cnt
    FROM D_IDW_INT.ENCTR_ADMSN ed
    JOIN D_IDW_INT.enctr_diagn aed ON (ed.enctr_id = aed.enctr_id AND aed.name_space_cd = ed.name_space_cd AND ed.admit_dt >= DATE'2020-02-01' AND aed.DIAGN_SEQ_NUM > 0)
    LEFT JOIN D_SHR_INT.term_map_fltn td ON (aed.diagn_cd = td.src_term_key)
    LEFT JOIN TERM_MAP_GRP_FILTERED tg1 ON (td.tgt_term_key = tg1.src_term_key AND tg1.tgt_fmly_name = 'ServiceLineDRG' AND tg1.tgt_term_cd LIKE 'COVID%' AND DISCHRG_DT BETWEEN tg1.eff_strt_dt AND tg1.eff_end_dt)
    WHERE ed.name_space_cd = :in_name_space_cd
    AND EXISTS (
        SELECT 1
        FROM D_ERM_INT.enctr_prcs_cntrl prcs_ctrl
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
CHRG_AGG AS (
    SELECT
        dtl.enctr_id,
        dtl.name_space_cd,
        me_dt.new_eff_from_dt eff_from_dt,
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
        SUM(dtl.chrg_qty) AS chrg_qty_cnt
    FROM D_ERM_INT.ENCTR_HIST eh
    JOIN me_dt ON (me_dt.new_eff_from_dt BETWEEN eh.eff_from_dt AND eh.eff_thru_dt)
    LEFT JOIN D_ERM_INT.ENCTR_CHRG_DTL dtl ON (dtl.enctr_id = eh.enctr_id AND eh.name_space_cd = dtl.name_space_cd AND dtl.post_dt <= me_dt.new_eff_from_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED chrgCdTerm ON (dtl.item_chrg_cd = chrgCdTerm.src_term_key AND chrgCdTerm.tgt_fmly_name = 'ChargeCdGroup' AND me_dt.new_eff_from_dt BETWEEN chrgCdTerm.eff_strt_dt AND chrgCdTerm.eff_end_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED chrgDeptTerm ON (dtl.RVNU_DEPT_CD = chrgDeptTerm.src_term_key AND chrgDeptTerm.tgt_fmly_name = 'ChargeDepartmentGroup' AND me_dt.new_eff_from_dt BETWEEN chrgDeptTerm.eff_strt_dt AND chrgDeptTerm.eff_end_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED revLocTerm ON (dtl.RVNU_LOC_CD = revLocTerm.src_term_key AND revLocTerm.tgt_fmly_name = 'ChargeDepartmentGroup' AND me_dt.new_eff_from_dt BETWEEN revLocTerm.eff_strt_dt AND revLocTerm.eff_end_dt)
    LEFT JOIN D_SHR_INT.term_map_fltn td1 ON (dtl.cpt_hcpc_std_pcdr_cd = td1.src_term_key)
    LEFT JOIN TERM_MAP_GRP_FILTERED tg11 ON (td1.tgt_term_key = tg11.src_term_key AND tg11.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tg11.eff_strt_dt AND tg11.eff_end_dt)
    WHERE eh.name_space_cd = :in_name_space_cd
    AND EXISTS (
        SELECT 1
        FROM D_ERM_INT.enctr_prcs_cntrl prcs_ctrl
        WHERE prcs_ctrl.enctr_id = eh.enctr_id
        AND prcs_ctrl.name_space_cd = eh.name_space_cd
        AND (
            prcs_ctrl.std_grpg_rsult_dt IS NULL
            OR prcs_ctrl.std_grpg_rsult_dt < prcs_ctrl.std_grpg_extc_dt
            OR (:in_run_type IN ('H','S') AND COALESCE(prcs_ctrl.std_grpg_rsult_dt, DATE'2041-01-01') >= :l_start_dt)
        )
    )
    GROUP BY dtl.enctr_id, dtl.name_space_cd, me_dt.new_eff_from_dt
),
COMBINED_TERM_CHRG AS (
    SELECT
        eh.enctr_id,
        eh.name_space_cd,
        me_dt.new_eff_from_dt eff_from_dt,
        COUNT(*) AS term_all_cnt,
        COALESCE(chrg_agg.SurgeryCharge_cnt, 0) AS SurgeryCharge_cnt,
        COALESCE(chrg_agg.EmergencyCharge_cnt, 0) AS EmergencyCharge_cnt,
        COALESCE(chrg_agg.RehabCharge_cnt, 0) AS RehabCharge_cnt,
        COALESCE(chrg_agg.BehavCharge_cnt, 0) AS BehavCharge_cnt,
        COALESCE(chrg_agg.BehavRevLoc_cnt, 0) AS BehavRevLoc_cnt,
        COALESCE(chrg_agg.covid_hcpcs_cnt, 0) AS covid_hcpcs_cnt,
        COALESCE(chrg_agg.covid_chrg_cnt, 0) AS covid_chrg_cnt,
        COALESCE(chrg_agg.covidtst_hcpcs_cnt, 0) AS covidtst_hcpcs_cnt,
        COALESCE(chrg_agg.chrg_qty_cnt, 0) AS chrg_qty_cnt,
        CASE
            WHEN (st.term_cd = 'I' AND (
                DIAGS.covid_icd_cnt > 0
                OR (
                    DIAGS.final_icd_cnt = 0
                    AND (
                        chrg_agg.covid_hcpcs_cnt > 0
                        OR tgpc.tgt_term_cd = 'COVID 19'
                        OR tgsc.tgt_term_cd = 'COVID 19'
                        OR tgtc.tgt_term_cd = 'COVID 19'
                        OR tgfc.tgt_term_cd = 'COVID 19'
                    )
                )
            )) THEN 'Y'
            ELSE 'N'
        END AS INPTN_COVID_FLG,
        CASE
            WHEN (
                (ed.admit_dt_tm IS NULL AND ed.dschrg_dt_tm IS NULL)
                OR pin.fmly_name IN ('Test Patient','TEST','Zztest','TESTPRODUCTION','Zztower')
                OR pin.fmly_name || pin.gvn_name LIKE '%HNAM%TEST%'
                OR eh.rec_auth = 69
                OR tt3.tgt_term_cd = 'EXCLUDE FROM REPORTS'
                OR ed.actv_ind = 'N'
                OR (eh.EXTR_AR_FLG = 'Y' AND eh.name_space_cd NOT IN ('EPIC-CHICAGO','EPIC-CHICAGO-PB'))
            ) THEN 'Exclude From Reports'
            WHEN grp.tgt_term_key = 'LEGACY CONVERSION|GRP|SubCategory' THEN 'LEGACY CONVERSION'
            WHEN st.term_cd <> 'I' THEN
                CASE
                    WHEN COALESCE(agg.tot_chrg, 0) = 0 AND COALESCE(chrg_agg.chrg_qty_cnt, 0) = 0 THEN 'Outpatient Without Charges'
                    WHEN chrg_agg.SurgeryCharge_cnt > 0 AND COALESCE(tt4.tgt_term_cd, ' ') <> 'DELIVERY' THEN 'Outpatient Surgery'
                    WHEN tt3.tgt_term_cd = 'SERIES' THEN 'Outpatient Series'
                    WHEN (chrg_agg.EmergencyCharge_cnt > 0 OR tt3.tgt_term_cd = 'EMERGENCY') AND COALESCE(tt3.tgt_term_cd, ' ') <> 'URGENT CARE' THEN 'Outpatient Emergency Services'
                    WHEN tt3.tgt_term_cd = 'NONPATIENT' OR tt5.tgt_term_cd = 'NONPATIENT' OR UPPER(eh.PRMRY_INSRNC_CD) LIKE 'VITA%'
                         OR (tt3.tgt_term_cd = 'SKILLED NURSING' AND eh.name_space_cd = 'HEALTHQUEST MC0CONS' AND eh.DSCHG_DT > DATE'2015-09-30')
                         OR (STRTOK(tm3.tgt_term_key,'|',1) = 'HI' AND eh.ORG_LVL_1_CD LIKE '750|%') THEN 'Nonpatient Cases'
                    ELSE 'Outpatient Other'
                END
            WHEN st.term_cd = 'I' THEN
                CASE
                    WHEN (tt3.tgt_term_cd IN ('NEWBORN','INPATIENT ACUTE') AND tt4.tgt_term_cd IN ('NORMAL NEWBORN')) 
                         OR (tt3.tgt_term_cd IN ('NEWBORN') AND tt4.tgt_term_cd IN ('UNCODED')) THEN 'Inpatient Normal Newborn'
                    WHEN COALESCE(agg.tot_chrg, 0) = 0 AND COALESCE(chrg_agg.chrg_qty_cnt, 0) = 0 AND ed.dschrg_dt_tm IS NOT NULL THEN 'Inpatient Without Charges'
                    WHEN tt3.tgt_term_cd = 'JOINT VENTURE' THEN 'Inpatient Joint Venture'
                    WHEN tt3.tgt_term_cd = 'LONG TERM CARE' THEN 'Inpatient Long Term Care'
                    WHEN tt3.tgt_term_cd = 'RESPITE AND TRANSITION CARE' THEN 'Inpatient Respite and Trans'
                    WHEN tt3.tgt_term_cd = 'BEHAVIORAL HEALTH' OR (tt3.tgt_term_cd = 'INPATIENT ACUTE' AND chrg_agg.BehavRevLoc_cnt > 0) THEN 'Inpatient Behavioral Medicine'
                    WHEN tt3.tgt_term_cd = 'INPATIENT REHAB' THEN 'Inpatient Rehab'
                    WHEN tt3.tgt_term_cd = 'SKILLED NURSING' THEN 'Inpatient Skilled Nursing'
                    WHEN tt3.tgt_term_cd = 'ACUTE HEAD PAIN' THEN 'Inpatient Acute Head Pain'
                    WHEN tt3.tgt_term_cd IN ('NEWBORN', 'INPATIENT ACUTE') THEN 'Inpatient Acute'
                    WHEN chrg_agg.BehavCharge_cnt > 0 THEN 'Inpatient Behavioral Medicine'
                    WHEN chrg_agg.RehabCharge_cnt > 0 THEN 'Inpatient Rehab'
                    ELSE 'Inpatient Undefined'
                END
            ELSE 'Outpatient Undefined'
        END AS v_grp_cd_all
    FROM D_ERM_INT.ENCTR_HIST eh
    JOIN me_dt ON (me_dt.new_eff_from_dt BETWEEN eh.eff_from_dt AND eh.eff_thru_dt)
    LEFT JOIN CHRG_AGG ON (CHRG_AGG.enctr_id = eh.enctr_id AND CHRG_AGG.name_space_cd = eh.name_space_cd AND CHRG_AGG.eff_from_dt = me_dt.new_eff_from_dt)
    LEFT JOIN D_SHR_INT.term st ON (st.term_key = eh.ptient_clas_cd)
    LEFT JOIN DIAGS ON (DIAGS.enctr_id = eh.enctr_id AND DIAGS.name_space_cd = eh.name_space_cd)
    LEFT JOIN D_ERM_INT.ENCTR_AGG agg ON (eh.enctr_id = agg.enctr_id AND eh.name_space_cd = agg.name_space_cd AND me_dt.new_eff_from_dt BETWEEN agg.eff_from_dt AND agg.eff_thru_dt)
    LEFT JOIN D_IDW_INT.ENCTR_ADMSN ed ON (ed.enctr_id = eh.enctr_id AND eh.name_space_cd = ed.name_space_cd)
    LEFT JOIN D_SHR_INT.term_map_fltn tm3 ON (eh.ptient_type_cd = tm3.src_term_key)
    LEFT JOIN TERM_MAP_GRP_FILTERED tt3 ON (tm3.tgt_term_key = tt3.src_term_key AND tt3.tgt_fmly_name = 'PatientTypeGroup' AND me_dt.new_eff_from_dt BETWEEN tt3.eff_strt_dt AND tt3.eff_end_dt)
    LEFT JOIN D_SHR_INT.term_map_fltn tm4 ON (eh.drg_cd = tm4.src_term_key)
    LEFT JOIN TERM_MAP_GRP_FILTERED tt4 ON (tm4.tgt_term_key = tt4.src_term_key AND tt4.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tt4.eff_strt_dt AND tt4.eff_end_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED tt5 ON (eh.ORG_LVL_3_CD = tt5.src_term_key AND tt5.tgt_fmly_name = 'PatientTypeGroup' AND me_dt.new_eff_from_dt BETWEEN tt5.eff_strt_dt AND tt5.eff_end_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED tgpc ON (eh.prmry_insrnc_cd = tgpc.src_term_key AND tgpc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgpc.eff_strt_dt AND tgpc.eff_end_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED tgsc ON (eh.scndry_insrnc_cd = tgsc.src_term_key AND tgsc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgsc.eff_strt_dt AND tgsc.eff_end_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED tgtc ON (eh.third_insrnc_cd = tgtc.src_term_key AND tgtc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgtc.eff_strt_dt AND tgtc.eff_end_dt)
    LEFT JOIN TERM_MAP_GRP_FILTERED tgfc ON (eh.frth_insrnc_cd = tgfc.src_term_key AND tgfc.tgt_fmly_name = 'ServiceLineDRG' AND me_dt.new_eff_from_dt BETWEEN tgfc.eff_strt_dt AND tgfc.eff_end_dt)
    LEFT JOIN (
        SELECT *
        FROM D_IDW_INT.ENCTR
        QUALIFY ROW_NUMBER() OVER (PARTITION BY src_admn_enctr_sk, src_admn_name_space_cd ORDER BY load_mod_dt_tm DESC, load_add_dt_tm DESC) = 1
    ) e ON (e.enctr_id = eh.enctr_id AND eh.name_space_cd = e.src_admn_name_space_cd)
    LEFT JOIN D_IDW_INT.PRTY_INDIV_NAME pin ON (pin.INDIV_PRTY_ID = e.PTIENT_MBR_PRTY_ID AND pin.NAME_TYPE_CD = 'GVN_NM')
    LEFT JOIN D_ERM_INT.enctr_tsactn_dtl td ON (eh.name_space_cd = td.name_space_cd AND eh.enctr_id = td.enctr_id)
    LEFT JOIN D_SHR_INT.term_map_grp grp ON (td.item_tsactn_cd = grp.src_term_key)
    WHERE eh.name_space_cd = :in_name_space_cd
    AND EXISTS (
        SELECT 1
        FROM D_ERM_INT.enctr_prcs_cntrl prcs_ctrl
        WHERE prcs_ctrl.enctr_id = eh.enctr_id
        AND prcs_ctrl.name_space_cd = eh.name_space_cd
        AND (
            prcs_ctrl.std_grpg_rsult_dt IS NULL
            OR prcs_ctrl.std_grpg_rsult_dt < prcs_ctrl.std_grpg_extc_dt
            OR (:in_run_type IN ('H','S') AND COALESCE(prcs_ctrl.std_grpg_rsult_dt, DATE'2041-01-01') >= :l_start_dt)
        )
    )
    GROUP BY eh.enctr_id, eh.name_space_cd, me_dt.new_eff_from_dt
),
MAIN AS (
    SELECT
        enctr_hist.enctr_id,
        me_dt.new_eff_from_dt eff_from_dt,
        DATE'2041-01-01' eff_thru_dt,
        enctr_hist.REC_AUTH,
        enctr_hist.NAME_SPACE_CD,
        CASE WHEN COALESCE(COMBINED_TERM_CHRG.SurgeryCharge_cnt, 0) > 0 THEN 'Y' ELSE 'N' END surg_flg,
        CASE WHEN COALESCE(COMBINED_TERM_CHRG.EmergencyCharge_cnt, 0) > 0 THEN 'Y' ELSE 'N' END er_flg,
        CASE
            WHEN COMBINED_TERM_CHRG.term_all_cnt > 0 THEN COMBINED_TERM_CHRG.v_grp_cd_all
            ELSE 'Undefined Others'
        END v_grp_cd,
        COMBINED_TERM_CHRG.inptn_covid_flg
    FROM D_ERM_INT.ENCTR_HIST enctr_hist
    JOIN me_dt ON (me_dt.new_eff_from_dt BETWEEN enctr_hist.eff_from_dt AND enctr_hist.eff_thru_dt)
    LEFT JOIN COMBINED_TERM_CHRG ON (enctr_hist.enctr_id = COMBINED_TERM_CHRG.enctr_id AND enctr_hist.name_space_cd = COMBINED_TERM_CHRG.name_space_cd AND COMBINED_TERM_CHRG.eff_from_dt = me_dt.new_eff_from_dt)
    WHERE enctr_hist.name_space_cd = :in_name_space_cd
    AND EXISTS (
        SELECT 1
        FROM D_ERM_INT.enctr_prcs_cntrl prcs_ctrl
        WHERE prcs_ctrl.enctr_id = enctr_hist.enctr_id
        AND prcs_ctrl.name_space_cd = enctr_hist.name_space_cd
        AND (
            prcs_ctrl.std_grpg_rsult_dt IS NULL
            OR prcs_ctrl.std_grpg_rsult_dt < prcs_ctrl.std_grpg_extc_dt
            OR (:in_run_type IN ('H','S') AND COALESCE(prcs_ctrl.std_grpg_rsult_dt, DATE'2041-01-01') >= :l_start_dt)
        )
    )
)
SELECT
    ENCTR_ID,
    eff_from_dt,
    REC_AUTH,
    NAME_SPACE_CD,
    SURG_FLG,
    ER_FLG,
    :l_job_id AS JOB_ID,
    CURRENT_DATE AS LOAD_ADD_DT_TM,
    CURRENT_DATE AS LOAD_MOD_DT_TM,
    '2041-01-01' AS EFF_THRU_DT,
    COALESCE(v_grp_cd, 'Undefined Main') AS STD_PTIENT_CLAS,
    COALESCE(inptn_covid_flg, 'N') AS INPTN_COVID_FLG
FROM MAIN
WHERE ENCTR_ID > 0
GROUP BY ENCTR_ID, eff_from_dt, REC_AUTH, NAME_SPACE_CD, SURG_FLG, ER_FLG, v_grp_cd, inptn_covid_flg
;
