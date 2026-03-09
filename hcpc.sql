create or replace package pkg_load_cpt_hcpc_qtr_dtl as
/*********************************************************************************
FILENAME: pkg_load_cpt_hcpc_qtr_dtl.sql
AUTHOR: Jim McCarthy
DATE: 20100915
DESC: This package is owned by the common schema, IN300.
      It is used to load cpt detail data from a temp table.
      NOTE: the temp table is created at runtime in load_cpt_hcpc_qtr_dtl.sh,
            so this package won't compile until runtime.

      TEMP TABLE DDL:
     drop table tmp_cpt_detail;
     create table tmp_cpt_detail (cpt_cd varchar2(10)
                                 ,cpt_ind varchar2(2)
                                 ,descr varchar2(100)
                                 ,eff_from_dt date
                                 ,eff_thru_dt date);



FILE: NONE

PARMAMETERS:
   1. APPL_HOME: Root directory of path to the datafile.

NOTE: The directory where the input datafile resides must be configured
      as a read/writable directory by adding it to the utl_file parameter in
      the init.ora of the target db. See the DBA for details.


---------------------------------------------------------------------
---------------------------------------------------------------------
---------------------------------------------------------------------
BACKFILL CODE TO POPULATE INDICATOR MCARE_ALLOW_BAD_DEBT_IND AFTER POPULATING TABLE CPT_HCPC_STD_PROC_QTR_DTL
---------------------------------------------------------------------
alter trigger st_enc_remit_dtl disable;

update enc_remit_dtl  erd
set erd.mcare_allow_bad_debt_ind =
   (select nvl(case qd.proc_apc_pmt_status when 'A' then 'N' WHEN 'D' THEN 'N' else 'Y' end, 'Y')
      from in300.cpt_hcpc_std_proc_qtr_dtl qd
     where erd.cpt_hcpc_std_proc_key = qd.cpt_hcpc_std_proc_key
       and erd.claim_thru_dt_key between qd.cpt_eff_from_dt_key and qd.cpt_eff_thru_dt_key
       and erd.claim_thru_dt_key >= 20040101
   )
where erd.level_column = 'SERVICE'
and erd.cpt_hcpc_std_proc_key in
   (select qd2.cpt_hcpc_std_proc_key
      from in300.cpt_hcpc_std_proc_qtr_dtl qd2
     where erd.cpt_hcpc_std_proc_key = qd2.cpt_hcpc_std_proc_key
       and erd.claim_thru_dt_key between qd2.cpt_eff_from_dt_key and qd2.cpt_eff_thru_dt_key
       and erd.claim_thru_dt_key >= 20040101
   );

alter trigger st_enc_remit_dtl enable;
---------------------------------------------------------------------
---------------------------------------------------------------------
---------------------------------------------------------------------

THIS QUERY FIND REMITS THAT HAVE A DELETED CPT CODE

select ENC_REMIT_DTL_KEY
      ,ORG_DIS_KEY
      ,CLAIM_NBR_KEY
      ,ENC_KEY
      ,ERD.CLAIM_FROM_DT_KEY
      ,ERD.CLAIM_THRU_DT_KEY
      ,VOUCHER_DT_KEY
      ,SVC_DT_KEY
      ,APC_KEY
      ,ERD.CPT_HCPC_STD_PROC_KEY
      ,qd.proc_apc_pmt_status
      ,MCARE_ALLOW_BAD_DEBT_IND
      ,QD.cpt_eff_from_dt_key
      ,QD.cpt_eff_thru_dt_key
      ,QD.CPT_HCPC_STD_PROC_CD
from enc_remit_dtl erd, in300.cpt_hcpc_std_proc_qtr_dtl qd
where erd.cpt_hcpc_std_proc_key = qd.cpt_hcpc_std_proc_key
and erd.claim_thru_dt_key between qd.cpt_eff_from_dt_key and qd.cpt_eff_thru_dt_key
and erd.mcare_allow_bad_debt_ind = 'N'
and qd.proc_apc_pmt_status = 'D'

THIS QUERY WILL SHOW THE PMT_STATUS IN BOTH TABLES AND THE RESULTS OF THE PMT_STATUS CASE STATEMENT.

 select ENC_REMIT_DTL_KEY
--      ,ORG_DIS_KEY
--      ,CLAIM_NBR_KEY
--      ,ENC_KEY
      ,ERD.CLAIM_FROM_DT_KEY
      ,ERD.CLAIM_THRU_DT_KEY
--      ,VOUCHER_DT_KEY
--      ,SVC_DT_KEY
--      ,APC_KEY
      ,ERD.CPT_HCPC_STD_PROC_KEY
      ,qd.proc_apc_pmt_status                                                                  qd_ind
      ,nvl(case qd.proc_apc_pmt_status when 'A' then 'N' WHEN 'D' THEN 'N' else 'Y' end, 'Y')  mcase
      ,MCARE_ALLOW_BAD_DEBT_IND                                                                erd_ind
      ,QD.cpt_eff_from_dt_key
      ,QD.cpt_eff_thru_dt_key
      ,QD.CPT_HCPC_STD_PROC_CD
from enc_remit_dtl erd, in300.cpt_hcpc_std_proc_qtr_dtl qd
where erd.cpt_hcpc_std_proc_key = qd.cpt_hcpc_std_proc_key
and erd.claim_thru_dt_key between qd.cpt_eff_from_dt_key and qd.cpt_eff_thru_dt_key
and qd.cpt_eff_from_dt_key = 20120101;

select *
from cpt_hcpc_std_proc_qtr_dtl
where job_id = 11586984
and proc_apc_pmt_status = 'D'
------------------------------------------------------------------------------------


 MODS:
 --------------------------------------------------------------------
 JRM 20101011 Added code to carry deleted rows forward to the next quarter.
 JRM 20120817 Changed to run off date parameter, added file name to job_history and display output.
 Anne 20200716 Modified to accommodate reloading a file for an existing quarter.
*********************************************************************************/

/********* PACKAGE CONSTANTS *********************/
c_commit_count       constant integer := 20000;

procedure p_load_cpt_hcpc_qtr_dtl
(i_filename                 in varchar2
,i_load_qtr_eff_from_dt_key in integer
);

end pkg_load_cpt_hcpc_qtr_dtl;
/
--===========================================================
--===========================================================
--===========================================================

create or replace package body pkg_load_cpt_hcpc_qtr_dtl as

procedure p_carry_forward_deleted_rows
(io_insert_deleted_cnt        in out nocopy pls_integer
,i_job_id                                integer
,i_load_qtr_eff_from_dt_key              integer
,o_load_qtr_eff_thru_dt_key   out nocopy integer
,o_prior_qtr_eff_from_dt_key  out nocopy integer
,o_prior_qtr_eff_thru_dt_key  out nocopy integer
) as

--get rows that have been logically deleted and insert them for next quarter
cursor get_deleted_rows
(i_prior_qtr_eff_from_dt_key    integer
,i_prior_qtr_eff_thru_dt_key    integer
) is
select *
  from cpt_hcpc_std_proc_qtr_dtl
 where cpt_eff_from_dt_key >= i_prior_qtr_eff_from_dt_key
   and cpt_eff_thru_dt_key <= i_prior_qtr_eff_thru_dt_key
   and proc_apc_pmt_status = 'D';   --logically deleted

v_load_qtr_start_dt   date;

begin

   --------------------------------------------------------------------------------------
   --NOTE: ALL QUARTER START/END DATES ARE DERIVED FROM THE DATE PASSED TO THE PROGRAM.
   --------------------------------------------------------------------------------------
   v_load_qtr_start_dt         := to_date(i_load_qtr_eff_from_dt_key,'yyyymmdd');
   o_load_qtr_eff_thru_dt_key  := to_number(to_char(last_day(add_months(v_load_qtr_start_dt,2)),'yyyymmdd')); --eg 3/31/12
   o_prior_qtr_eff_from_dt_key := to_number(to_char(add_months(v_load_qtr_start_dt,-3),'yyyymmdd'));
   o_prior_qtr_eff_thru_dt_key := to_number(to_char(v_load_qtr_start_dt-1,'yyyymmdd'));

   --get the 'logically deleted' rows from the prior quarter and insert them for the load quarter.
   for j in get_deleted_rows (o_prior_qtr_eff_from_dt_key,o_prior_qtr_eff_thru_dt_key) loop
      --insert the deleted that were logically deleted for the load quarter.
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
      ,i_load_qtr_eff_from_dt_key        --cpt_eff_from_dt_key
      ,o_load_qtr_eff_thru_dt_key        --cpt_eff_thru_dt_key
      ,j.cpt_hcpc_std_proc_cd
      ,i_job_id
      ,sysdate                            --load_add_dt
      ,j.proc_apc_pmt_status
      ,substr(j.proc_desc,1,40)
      ,sysdate                            --load_mod_dt
      );

      io_insert_deleted_cnt := io_insert_deleted_cnt + 1;

   end loop; --get_deleted_rows

end p_carry_forward_deleted_rows;
------------------------------------------------------------
------------------------------------------------------------
procedure p_load_cpt_hcpc_qtr_dtl
(i_filename              varchar2
,i_load_qtr_eff_from_dt_key integer
) as

cursor c1
(i_eff_from_dt_key integer
,i_eff_thru_dt_key  integer) is
select *
  from tmp_cpt_detail
 where trunc(eff_from_dt) >= to_date(i_eff_from_dt_key,'yyyymmdd')        --this eliminates rows having a start or end date outside the load quarter.
   and trunc(eff_thru_dt) <= to_date(i_eff_thru_dt_key,'yyyymmdd');

r_dtl                          cpt_hcpc_std_proc_qtr_dtl%rowtype;
r_job_history                  job_history%rowtype;
v_del_carry_fwd_cnt            pls_integer := 0;
v_ins_deleted_code             pls_integer := 0;
v_load_qtr_eff_thru_dt_key     integer;
v_prior_qtr_eff_from_dt_key    integer;
v_prior_qtr_eff_thru_dt_key    integer;

begin  --executable section

   --For CPT/HCPC loads we use the same value for job_id and job_ref_id.
   r_job_history.job_id        := pkg_job_history.f_get_job_id_nextval;
   r_job_history.job_ref_id    := r_job_history.job_id;
   r_job_history.business_unit := 'IN300';
   r_job_history.site_id       := '001';
   r_job_history.script_name   := 'pkg_load_cpt_hcpc_qtr_dtl.sql';
   r_job_history.file_name     := i_filename;
   r_job_history.prcs_name     := 'Load CPT_HCPC_STD_PROC_QTR_DTL';

   --Initialize cpt/hcpc job history by inserting a job_history record for this process.
   r_job_history := pkg_job_history.f_init_job_history(r_job_history);
   r_job_history.stat_cd  := 'P';   --Processing

   --contant for all rows
   r_dtl.load_mod_dt := r_job_history.start_dt;
   r_dtl.load_add_dt := r_job_history.start_dt;
   r_dtl.job_id      := r_job_history.job_id;
   
   --Delete any existing rows for the input quarter start date. (added 7/15/2020)
   delete from cpt_hcpc_std_proc_qtr_dtl where cpt_eff_from_dt_key = i_load_qtr_eff_from_dt_key;
   r_job_history.del_cnt := sql%rowcount;

   ----------------------------------------------------------------------------------------
   ----------------------------------------------------------------------------------------
   ----------------------------------------------------------------------------------------
   p_carry_forward_deleted_rows(v_del_carry_fwd_cnt
                               ,r_job_history.job_id
                               ,i_load_qtr_eff_from_dt_key
                               ,v_load_qtr_eff_thru_dt_key
                               ,v_prior_qtr_eff_from_dt_key
                               ,v_prior_qtr_eff_thru_dt_key
                               );
   ----------------------------------------------------------------------------------------
   ----------------------------------------------------------------------------------------
   ----------------------------------------------------------------------------------------

   --insert the rows from the temp table into CPT_HCPC_STD_PROC_QTR_DTL.
   for i in c1 (i_load_qtr_eff_from_dt_key, v_load_qtr_eff_thru_dt_key) loop

      r_job_history.prcs_cnt := r_job_history.prcs_cnt + 1;
      
      r_dtl.cpt_hcpc_std_proc_cd   := i.cpt_cd;
      r_dtl.cpt_hcpc_std_proc_key  := pkg_in300_key.f_get_cpt_key(r_dtl.cpt_hcpc_std_proc_cd
                                                                 ,r_job_history.business_unit
                                                                 ,r_job_history.prcs_name
                                                                 ,r_job_history.job_id);

      r_dtl.cpt_eff_from_dt_key    := to_number(to_char(i.eff_from_dt,'yyyymmdd'));
      r_dtl.cpt_eff_thru_dt_key    := to_number(to_char(i.eff_thru_dt,'yyyymmdd'));
      r_dtl.proc_apc_pmt_status    := i.cpt_ind;
      r_dtl.proc_desc              := substr(trim(i.descr),1,40);

      -- check to see if start date in the first row of the file matches input start date.  
      -- If not matching then abort.
      if r_job_history.prcs_cnt = 1 and i_load_qtr_eff_from_dt_key <> r_dtl.cpt_eff_from_dt_key then
        rollback;
        dbms_output.put_line('****** ERROR ******');
        dbms_output.put_line('****** Input Quarter Start Date ('||i_load_qtr_eff_from_dt_key
                           ||')does NOT match start date in the file ('||r_dtl.cpt_eff_from_dt_key||'). Load aborted!!!.');  
                           
        goto ENDPROG;         
      end if;
     
      begin

         insert into cpt_hcpc_std_proc_qtr_dtl values r_dtl;
         r_job_history.insert_cnt := r_job_history.insert_cnt + 1;

         if r_dtl.proc_apc_pmt_status = 'D' then
            v_ins_deleted_code := v_ins_deleted_code + 1;
         end if;

         exception when dup_val_on_index then
            --row already exists, update
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

      --We periodically update job history (which also COMMITs) as the loop processes.
      if (mod(r_job_history.prcs_cnt, c_commit_count) = 0) then
         pkg_job_history.p_updt_job_history(r_job_history);
      end if;
      
   end loop; --c1

   r_job_history.stat_cd := 'C';     --Mark cpt/hcpc job Completed
   pkg_job_history.p_updt_job_history(r_job_history);   --Final update and commit of DIAG job history.

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
   dbms_output.put_line('Records updated:    ' ||r_job_history.updt_cnt);
   dbms_output.put_line('Records inserted:    '||r_job_history.insert_cnt);
   dbms_output.put_line('Records processed:  ' ||r_job_history.prcs_cnt);
   dbms_output.put_line('Existing QTR Rows deleted: '||r_job_history.del_cnt);
   dbms_output.put_line('Duration: '||r_job_history.duration);
   dbms_output.put_line('-----------------------------');

   --This <<ENDPROG>> label is used so we can jump over the normal process logic without using
   <<ENDPROG>>
   dbms_output.put_line('End of Program');  -- need to have at least one statement after the label

end p_load_cpt_hcpc_qtr_dtl;

end pkg_load_cpt_hcpc_qtr_dtl;
/
