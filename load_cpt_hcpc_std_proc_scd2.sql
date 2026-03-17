CREATE OR REPLACE PROCEDURE load_cpt_hcpc_std_proc_scd2
AS
    v_proc_start     TIMESTAMP := SYSTIMESTAMP;
    v_rows_expired   NUMBER := 0;
    v_rows_inserted  NUMBER := 0;
    v_rows_updated   NUMBER := 0;
BEGIN
    -- =========================================================================
    -- Step 1: Expire records in target that no longer exist in source
    -- (based on natural key CPT_HCPC_STD_PROC_CD)
    -- Only expire currently active versions
    -- =========================================================================
    UPDATE cpt_hcpc_std_proc t
    SET    eff_end_dt     = TRUNC(SYSDATE),
           cpt_v3_mod_dt  = SYSDATE
    WHERE  t.eff_end_dt   = DATE '9999-12-31'
      AND  NOT EXISTS (
               SELECT 1
                 FROM (
                     SELECT cpt_hcpc_std_proc_cd
                       FROM cpt_hcpc_std_proc_qtr_dtl
                      GROUP BY cpt_hcpc_std_proc_cd   -- existence check only
                 ) s
                WHERE s.cpt_hcpc_std_proc_cd = t.cpt_hcpc_std_proc_cd
           );

    v_rows_expired := SQL%ROWCOUNT;

    -- =========================================================================
    -- Step 2: MERGE - Insert new versions or expire + insert on change
    -- Using latest source record per CPT_HCPC_STD_PROC_CD
    -- =========================================================================
    MERGE INTO cpt_hcpc_std_proc t
    USING (
        SELECT
            s.cpt_hcpc_std_proc_cd,
            s.cpt_hcpc_std_proc_key,               -- use source-provided version key
            TO_DATE(TO_CHAR(s.cpt_eff_from_dt_key, 'FM00000000'), 'YYYYMMDD') AS eff_start_dt,
            DATE '9999-12-31'                                             AS eff_end_dt,
            s.proc_desc,
            s.proc_desc                                                   AS proc_desc_abbr,
            s.proc_apc_pmt_status,
            s.job_id                                                      AS cpt_job_id,
            s.load_add_dt                                                 AS cpt_v3_add_dt,
            s.load_mod_dt                                                 AS cpt_v3_mod_dt   -- source mod date for new version
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY cpt_hcpc_std_proc_cd
                    ORDER BY cpt_eff_thru_dt_key DESC
                ) AS rn
            FROM cpt_hcpc_std_proc_qtr_dtl
        ) s
        WHERE s.rn = 1
    ) s
    ON (
           t.cpt_hcpc_std_proc_cd = s.cpt_hcpc_std_proc_cd
       AND t.eff_end_dt           = DATE '9999-12-31'          -- only current version
       AND NVL(t.proc_desc, '~')  = NVL(s.proc_desc, '~')      -- sentinel for NULL-safe compare
    )
    WHEN MATCHED THEN
        UPDATE SET
            eff_end_dt     = TRUNC(SYSDATE),
            cpt_v3_mod_dt  = SYSDATE
        WHERE t.proc_desc IS DISTINCT FROM s.proc_desc          -- only expire if description changed
    WHEN NOT MATCHED THEN
        INSERT (
            cpt_hcpc_std_proc_cd,
            cpt_hcpc_std_proc_key,
            act_inact_ind,
            eff_end_dt,
            eff_start_dt,
            proc_asc_grp,
            proc_desc,
            proc_desc_abbr,
            cpt_v3_add_dt,
            cpt_v3_mod_dt,
            cpt_job_id,
            proc_apc_pmt_status
        )
        VALUES (
            s.cpt_hcpc_std_proc_cd,
            s.cpt_hcpc_std_proc_key,          -- ← from latest source row
            NULL,
            s.eff_end_dt,
            s.eff_start_dt,
            NULL,
            s.proc_desc,
            s.proc_desc_abbr,
            s.cpt_v3_add_dt,
            s.cpt_v3_mod_dt,
            s.cpt_job_id,
            s.proc_apc_pmt_status
        );

    v_rows_inserted := SQL%ROWCOUNT - v_rows_updated;  -- rough estimate; actual inserts = total merged - updates

    COMMIT;

    -- Basic execution log (create table proc_log if needed)
    INSERT INTO proc_log (
        proc_name, start_ts, rows_expired, rows_inserted, duration_sec
    ) VALUES (
        'LOAD_CPT_HCPC_STD_PROC_SCD2',
        v_proc_start,
        v_rows_expired,
        v_rows_inserted,
        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_proc_start))
    );

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        -- Add error logging here if desired
        RAISE_APPLICATION_ERROR(-20001, 'Error in load_cpt_hcpc_std_proc_scd2: ' || SQLERRM);
END load_cpt_hcpc_std_proc_scd2;
/
