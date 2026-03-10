create or replace package pkg_load_cpt_hcpc_qtr_dtl as

/*********************************************************************************
FILENAME: pkg_load_cpt_hcpc_qtr_dtl.sql
AUTHOR: Jim McCarthy
DATE: 20100915
DESC: This package is owned by the common schema, IN300.
      It is used to load cpt detail data from a temp table + master table.
      NOTE: the temp table is created at runtime in load_cpt_hcpc_qtr_dtl.sh

      TEMP TABLE DDL:
     drop table tmp_cpt_detail;
     create table tmp_cpt_detail (cpt_cd varchar2(10)
                                 ,cpt_ind varchar2(2)
                                 ,descr varchar2(100)
                                 ,eff_from_dt date
                                 ,eff_thru_dt date);

MODS:
 JRM 20101011 Added code to carry deleted rows forward to the next quarter.
 JRM 20120817 Changed to run off date parameter, added file name to job_history and display output.
 Anne 20200716 Modified to accommodate reloading a file for an existing quarter.
 [2025] Added upsert into master table cpt_hcpc_std_proc
*********************************************************************************/

c_commit_count       constant integer := 20000;

procedure p_load_cpt_hcpc_qtr_dtl
(i_filename                 in varchar2
,i_load_qtr_eff_from_dt_key in integer
);

end pkg_load_cpt_hcpc_qtr_dtl;
/

create or replace package body pkg_load_cpt_hcpc_qtr_dtl as

procedure p_carry_forward_deleted_rows
(io_insert_deleted_cnt        in out nocopy pls_integer
,i_job_id                     integer
,i_load_qtr_eff_from_dt_key   integer
,o_load_qtr_eff_thru_dt_key   out nocopy integer
,o_prior_qtr_eff_from_dt_key  out nocopy integer
,o_prior_qtr_eff_thru_dt_key  out nocopy integer
) as

cursor get_deleted_rows
(i_prior_qtr_eff_from_dt_key    integer
,i_prior_qtr_eff_thru_dt_key    integer
) is
select *
  from cpt_hcpc_std_proc_qtr_dtl
 where cpt_eff_from_dt_key >= i_prior_qtr_eff_from_dt_key
   and cpt_eff_thru_dt_key <= i_prior_qtr_eff_thru_dt_key
   and proc_apc_pmt_status = 'D';

v_load_qtr_start_dt   date;

begin
   v_load_qtr_start_dt         := to_date(i_load_qtr_eff_from_dt_key,'yyyymmdd');
   o_load_qtr_eff_thru_dt_key  := to_number(to_char(last_day(add_months(v_load_qtr_start_dt,2)),'yyyymmdd'));
   o_prior_qtr_eff_from_dt_key := to_number(to_char(add_months(v_load_qtr_start_dt,-3),'yyyymmdd'));
   o_prior_qtr_eff_thru_dt_key := to_number(to_char(v_load_qtr_start_dt-1,'yyyymmdd'));

   for j in get_deleted_rows (o_prior_qtr_eff_from_dt_key,o_prior_qtr_eff_thru_dt_key) loop
      insert into cpt_hcpc_std_proc_qtr_dtl
      (cpt_hcpc_std_proc_key
      ,cpt_eff_from_dt_key
      ,cpt_eff_thru_dt_key
      ,cpt_hcpc_std_proc_cd
      ,job_id
      ,load_add_dt
      ,proc_apc_pmt_status
      ,proc_desc
      ,load_mod_dt
      )
      values
      (j.cpt_hcpc_std_proc_key
      ,i_load_qtr_eff_from_dt_key
      ,o_load_qtr_eff_thru_dt_key
      ,j.cpt_hcpc_std_proc_cd
      ,i_job_id
      ,sysdate
      ,j.proc_apc_pmt_status
      ,substr(j.proc_desc,1,40)
      ,sysdate
      );

      io_insert_deleted_cnt := io_insert_deleted_cnt + 1;
   end loop;
end p_carry_forward_deleted_rows;

procedure p_load_cpt_hcpc_qtr_dtl
(i_filename              varchar2
,i_load_qtr_eff_from_dt_key integer
) as

cursor c1
(i_eff_from_dt_key integer
,i_eff_thru_dt_key  integer) is
select *
  from tmp_cpt_detail
 where trunc(eff_from_dt) >= to_date(i_eff_from_dt_key,'yyyymmdd')
   and trunc(eff_thru_dt) <= to_date(i_eff_thru_dt_key,'yyyymmdd');

r_dtl                          cpt_hcpc_std_proc_qtr_dtl%rowtype;
r_job_history                  job_history%rowtype;
v_del_carry_fwd_cnt            pls_integer := 0;
v_ins_deleted_code             pls_integer := 0;
v_load_qtr_eff_thru_dt_key     integer;
v_prior_qtr_eff_from_dt_key    integer;
v_prior_qtr_eff_thru_dt_key    integer;

begin

   r_job_history.job_id        := pkg_job_history.f_get_job_id_nextval;
   r_job_history.job_ref_id    := r_job_history.job_id;
   r_job_history.business_unit := 'IN300';
   r_job_history.site_id       := '001';
   r_job_history.script_name   := 'pkg_load_cpt_hcpc_qtr_dtl.sql';
   r_job_history.file_name     := i_filename;
   r_job_history.prcs_name     := 'Load CPT_HCPC_STD_PROC_QTR_DTL + master';

   r_job_history := pkg_job_history.f_init_job_history(r_job_history);
   r_job_history.stat_cd  := 'P';

   r_dtl.load_mod_dt := r_job_history.start_dt;
   r_dtl.load_add_dt := r_job_history.start_dt;
   r_dtl.job_id      := r_job_history.job_id;

   delete from cpt_hcpc_std_proc_qtr_dtl 
    where cpt_eff_from_dt_key = i_load_qtr_eff_from_dt_key;
   r_job_history.del_cnt := sql%rowcount;

   p_carry_forward_deleted_rows(v_del_carry_fwd_cnt
                               ,r_job_history.job_id
                               ,i_load_qtr_eff_from_dt_key
                               ,v_load_qtr_eff_thru_dt_key
                               ,v_prior_qtr_eff_from_dt_key
                               ,v_prior_qtr_eff_thru_dt_key
                               );

   for i in c1 (i_load_qtr_eff_from_dt_key, v_load_qtr_eff_thru_dt_key) loop

      r_job_history.prcs_cnt := r_job_history.prcs_cnt + 1;
      
      r_dtl.cpt_hcpc_std_proc_cd   := i.cpt_cd;
      r_dtl.cpt_hcpc_std_proc_key  := pkg_in300_key.f_get_cpt_key(r_dtl.cpt_hcpc_std_proc_cd
                                                                 ,r_job_history.business_unit
                                                                 ,r_job_history.prcs_name
                                                                 ,r_job_history.job_id);

      -- ────────────────────────────────────────────────────────────────
      -- UPSERT into MASTER table cpt_hcpc_std_proc (current/latest version)
      -- ────────────────────────────────────────────────────────────────
      begin
         insert into cpt_hcpc_std_proc (
            CPT_HCPC_STD_PROC_KEY,
            CPT_HCPC_STD_PROC_CD,
            PROC_DESC_ORIC_DESC_ABBR,
            PROC_APC_PMT_STATUS,
            CPT_V3_ADD_DT,
            CPT_V3_MOD_DT,
            CPT_JOB_ID
            -- ACT_INACT_IND, EFF_START_DT, EFF_END_DT, PROC_ASC_GRP left as NULL
         )
         values (
            r_dtl.cpt_hcpc_std_proc_key,
            i.cpt_cd,
            substr(trim(i.descr), 1, 100),   -- adjust length if column is smaller
            i.cpt_ind,
            r_job_history.start_dt,
            r_job_history.start_dt,
            r_job_history.job_id
         );

      exception
         when dup_val_on_index then
            update cpt_hcpc_std_proc
               set CPT_HCPC_STD_PROC_CD     = i.cpt_cd,                      -- usually redundant but safe
                   PROC_DESC_ORIC_DESC_ABBR = substr(trim(i.descr), 1, 100),
                   PROC_APC_PMT_STATUS      = i.cpt_ind,
                   CPT_V3_MOD_DT            = r_job_history.start_dt,
                   CPT_JOB_ID               = r_job_history.job_id
             where CPT_HCPC_STD_PROC_KEY = r_dtl.cpt_hcpc_std_proc_key;
      end;
      -- ────────────────────────────────────────────────────────────────

      r_dtl.cpt_eff_from_dt_key    := to_number(to_char(i.eff_from_dt,'yyyymmdd'));
      r_dtl.cpt_eff_thru_dt_key    := to_number(to_char(i.eff_thru_dt,'yyyymmdd'));
      r_dtl.proc_apc_pmt_status    := i.cpt_ind;
      r_dtl.proc_desc              := substr(trim(i.descr),1,40);

      if r_job_history.prcs_cnt = 1 
         and i_load_qtr_eff_from_dt_key <> r_dtl.cpt_eff_from_dt_key then
         rollback;
         dbms_output.put_line('****** ERROR ******');
         dbms_output.put_line('Input Quarter Start Date ('||i_load_qtr_eff_from_dt_key
                            ||') does NOT match start date in file ('||r_dtl.cpt_eff_from_dt_key||').');
         goto ENDPROG;         
      end if;
     
      begin
         insert into cpt_hcpc_std_proc_qtr_dtl values r_dtl;
         r_job_history.insert_cnt := r_job_history.insert_cnt + 1;

         if r_dtl.proc_apc_pmt_status = 'D' then
            v_ins_deleted_code := v_ins_deleted_code + 1;
         end if;

      exception when dup_val_on_index then
         update cpt_hcpc_std_proc_qtr_dtl
            set cpt_hcpc_std_proc_cd  = r_dtl.cpt_hcpc_std_proc_cd
               ,proc_apc_pmt_status   = r_dtl.proc_apc_pmt_status
               ,proc_desc             = r_dtl.proc_desc
               ,load_mod_dt           = r_dtl.load_mod_dt
               ,job_id                = r_dtl.job_id
          where cpt_hcpc_std_proc_key = r_dtl.cpt_hcpc_std_proc_key
            and cpt_eff_from_dt_key   = r_dtl.cpt_eff_from_dt_key
            and cpt_eff_thru_dt_key   = r_dtl.cpt_eff_thru_dt_key;

         r_job_history.updt_cnt := r_job_history.updt_cnt + sql%rowcount;
      end;

      if (mod(r_job_history.prcs_cnt, c_commit_count) = 0) then
         pkg_job_history.p_updt_job_history(r_job_history);
      end if;
      
   end loop;

   r_job_history.stat_cd := 'C';
   pkg_job_history.p_updt_job_history(r_job_history);

   dbms_output.put_line('Job ID: '||r_job_history.job_id);
   dbms_output.put_line(r_job_history.prcs_name||' complete at >> '||to_char(sysdate,'hh24:mi:ss Mon dd yyyy'));
   dbms_output.put_line('load file: '||r_job_history.file_name);
   dbms_output.put_line('Dt key parm: '||i_load_qtr_eff_from_dt_key);
   dbms_output.put_line('-----------------------------');
   dbms_output.put_line('Load qtr eff from dt: '||i_load_qtr_eff_from_dt_key);
   dbms_output.put_line('Load qtr eff thru dt: '||v_load_qtr_eff_thru_dt_key);
   dbms_output.put_line('Prior qtr eff from dt: '||v_prior_qtr_eff_from_dt_key);
   dbms_output.put_line('Prior qtr eff thru dt: '||v_prior_qtr_eff_thru_dt_key);
   dbms_output.put_line('Logically Deleted rows carried forward: '||v_del_carry_fwd_cnt);
   dbms_output.put_line('Logically Deleted rows inserted: '||v_ins_deleted_code);
   dbms_output.put_line('Records updated (detail):    ' ||r_job_history.updt_cnt);
   dbms_output.put_line('Records inserted (detail):   '||r_job_history.insert_cnt);
   dbms_output.put_line('Records processed:           ' ||r_job_history.prcs_cnt);
   dbms_output.put_line('Existing QTR Rows deleted:   '||r_job_history.del_cnt);
   dbms_output.put_line('Duration: '||r_job_history.duration);
   dbms_output.put_line('-----------------------------');

   <<ENDPROG>>
   dbms_output.put_line('End of Program');

end p_load_cpt_hcpc_qtr_dtl;

end pkg_load_cpt_hcpc_qtr_dtl;
/
