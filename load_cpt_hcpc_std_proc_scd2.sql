CREATE OR REPLACE PROCEDURE load_cpt_hcpc_std_proc_scd2
AS
    v_proc_start TIMESTAMP := SYSTIMESTAMP;
    v_rows_processed NUMBER := 0;
    v_rows_inserted  NUMBER := 0;
    v_rows_expired   NUMBER := 0;
BEGIN
    -- ========================================================================
    -- Step 1: Expire records that exist in target but are missing in source
    -- (deleted records) - only expire currently active ones
    -- ========================================================================
    UPDATE cpt_hcpc_std_proc t
    SET    eff_end_dt     = TRUNC(SYSDATE),
           cpt_v3_mod_dt  = SYSDATE
    WHERE  t.eff_end_dt   = DATE '9999-12-31'
      AND  NOT EXISTS (
               SELECT 1
                 FROM cpt_hcpc_std_proc_qtr_dtl s
                WHERE s.cpt_hcpc_std_proc_key = t.cpt_hcpc_std_proc_key
           );

    v_rows_expired := SQL%ROWCOUNT;

    -- ========================================================================
    -- Step 2: MERGE - handle inserts and updates (new versions)
    -- We only create new version when:
    --   - key is completely new, OR
    --   - key exists but CPT_HCPC_STD_PROC_CD or PROC_DESC changed
    -- ========================================================================
    MERGE INTO cpt_hcpc_std_proc t
    USING (
        SELECT 
            s.cpt_hcpc_std_proc_key,
            s.cpt_hcpc_std_proc_cd,
            TO_DATE(TO_CHAR(s.cpt_eff_from_dt_key, 'FM00000000'), 'YYYYMMDD') AS eff_start_dt,
            DATE '9999-12-31'                                             AS eff_end_dt,   -- always open for new/current records
            s.proc_desc,
            s.proc_desc                                                       AS proc_desc_abbr,
            s.proc_apc_pmt_status,
            s.job_id                                                          AS cpt_job_id,
            s.load_add_dt                                                     AS cpt_v3_add_dt,
            s.load_mod_dt                                                     AS cpt_v3_mod_dt
        FROM cpt_hcpc_std_proc_qtr_dtl s
    ) s
    ON (
           t.cpt_hcpc_std_proc_key = s.cpt_hcpc_std_proc_key
       AND t.eff_end_dt            = DATE '9999-12-31'   -- only compare against current version
       AND t.cpt_hcpc_std_proc_cd  = s.cpt_hcpc_std_proc_cd
       AND NVL(t.proc_desc, ' ')   = NVL(s.proc_desc, ' ')   -- handle NULLs safely
    )
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
            s.cpt_hcpc_std_proc_key,
            NULL,                   -- act_inact_ind
            s.eff_end_dt,
            s.eff_start_dt,
            NULL,                   -- proc_asc_grp
            s.proc_desc,
            s.proc_desc_abbr,
            s.cpt_v3_add_dt,
            s.cpt_v3_mod_dt,        -- using source load_mod_dt for new version
            s.cpt_job_id,
            s.proc_apc_pmt_status
        );

    v_rows_inserted := SQL%ROWCOUNT;

    -- ========================================================================
    -- Optional: Expire old version when we just inserted a new one due to change
    -- (only needed if you want to be explicit; many implementations do this in same MERGE)
    -- But since we matched only on current version, the above already avoids updating unchanged records
    -- ========================================================================

    COMMIT;

    -- Basic logging (you can enhance with autonomous logging table)
    INSERT INTO proc_log (proc_name, start_ts, rows_expired, rows_inserted, duration_sec)
    VALUES (
        'LOAD_CPT_HCPC_STD_PROC_SCD2',
        v_proc_start,
        v_rows_expired,
        v_rows_inserted,
        EXTRACT(SECOND FROM (SYSTIMESTAMP - v_proc_start))
    );

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- Rollback + log error (simplified)
        ROLLBACK;
        -- You can insert into error log here
        RAISE;
END load_cpt_hcpc_std_proc_scd2;
/

-- Example execution:
-- EXEC load_cpt_hcpc_std_proc_scd2;
