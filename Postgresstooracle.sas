/* ========================================
   SAS ETL: PostgreSQL -> SAS Datasets -> Oracle
   Tables: http_requests, _users, _workbooks, _views
   Date: October 31, 2025
   ======================================== */

/* Step 1: Connect to PostgreSQL (LIBNAME) */
/* Option A: Direct PostgreSQL Engine (requires SAS/ACCESS to PostgreSQL) */
LIBNAME pg_work POSTGRES 
    SERVER="pg_host"  /* e.g., localhost or your Azure host */
    PORT=5432
    DATABASE="workgroup"  /* Your DB name */
    USER="pg_user"
    PASSWORD="pg_password"
    SCHEMA="public";  /* Adjust if needed */

/* Option B: If no direct engine, use ODBC (requires ODBC driver installed) */
/*
LIBNAME pg_work ODBC 
    DSN="PostgreSQL_DSN"  /* Set up in ODBC Data Sources (Control Panel) */
    USER="pg_user"
    PASSWORD="pg_password"
    READBUFF=10000;  /* Buffer for faster read */
*/

/* Verify connection */
PROC SQL;
    DESCRIBE TABLE pg_work.http_requests (OBS=5);  /* Preview 5 rows */
QUIT;

/* Step 2: Extract Each Table to SAS Datasets */
/* Filter for recent data to speed up (e.g., last 7 days) */
DATA WORK.http_requests_sas;
    SET pg_work.http_requests;
    WHERE created_at >= DATE() - 7;  /* Adjust filter as needed */
RUN;

DATA WORK.users_sas;
    SET pg_work._users;
RUN;

DATA WORK.workbooks_sas;
    SET pg_work._workbooks;
RUN;

DATA WORK.views_sas;
    SET pg_work._views;
RUN;

/* Optional: Apply transformations (e.g., calculate runtime_sec if needed) */
DATA WORK.http_requests_sas;
    SET WORK.http_requests_sas;
    runtime_sec = (completed_at - created_at) * 86400;  /* Convert interval to seconds */
RUN;

/* Step 3: Connect to Oracle (LIBNAME) */
/* Option A: Direct Oracle Engine (requires SAS/ACCESS to Oracle) */
LIBNAME oracle_dest ORACLE 
    USER="oracle_user"
    PASSWORD="oracle_password"
    PATH="oracle_tns_alias"  /* e.g., your TNS entry or full string: (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=service))) */
    SCHEMA="your_oracle_schema";  /* e.g., SCOTT */

/* Option B: If using ODBC for Oracle */
/*
LIBNAME oracle_dest ODBC 
    DSN="Oracle_DSN"  /* Set up in ODBC Data Sources */
    USER="oracle_user"
    PASSWORD="oracle_password"
    READBUFF=10000;
*/

/* Verify connection */
PROC SQL;
    DESCRIBE TABLE oracle_dest.http_requests_oracle (OBS=0);  /* Check if table exists */
QUIT;

/* Step 4: Load SAS Datasets to Oracle Tables */
/* Assumes Oracle tables exist; uncomment CREATE if needed (use pass-through for DDL) */

/* Load http_requests */
PROC SQL;  /* Use pass-through for CREATE if table doesn't exist */
    CONNECT TO ORACLE (USER="oracle_user" PASSWORD="oracle_password" PATH="oracle_tns_alias");
    /* EXECUTE (CREATE TABLE http_requests_oracle AS SELECT * FROM pg_work.http_requests WHERE 1=0) BY ORACLE; */  /* Uncomment to create empty table */
    DISCONNECT FROM ORACLE;
QUIT;

DATA oracle_dest.http_requests_oracle;
    SET WORK.http_requests_sas;
RUN;

/* Load _users */
PROC SQL;
    CONNECT TO ORACLE (USER="oracle_user" PASSWORD="oracle_password" PATH="oracle_tns_alias");
    /* EXECUTE (CREATE TABLE users_oracle AS SELECT * FROM pg_work._users WHERE 1=0) BY ORACLE; */
    DISCONNECT FROM ORACLE;
QUIT;

DATA oracle_dest.users_oracle;
    SET WORK.users_sas;
RUN;

/* Load _workbooks */
PROC SQL;
    CONNECT TO ORACLE (USER="oracle_user" PASSWORD="oracle_password" PATH="oracle_tns_alias");
    /* EXECUTE (CREATE TABLE workbooks_oracle AS SELECT * FROM pg_work._workbooks WHERE 1=0) BY ORACLE; */
    DISCONNECT FROM ORACLE;
QUIT;

DATA oracle_dest.workbooks_oracle;
    SET WORK.workbooks_sas;
RUN;

/* Load _views */
PROC SQL;
    CONNECT TO ORACLE (USER="oracle_user" PASSWORD="oracle_password" PATH="oracle_tns_alias");
    /* EXECUTE (CREATE TABLE views_oracle AS SELECT * FROM pg_work._views WHERE 1=0) BY ORACLE; */
    DISCONNECT FROM ORACLE;
QUIT;

DATA oracle_dest.views_oracle;
    SET WORK.views_sas;
RUN;

/* Step 5: Clean Up (Optional) */
/* Clear librefs */
LIBNAME pg_work CLEAR;
LIBNAME oracle_dest CLEAR;

/* Delete temp SAS datasets */
PROC DATASETS LIBRARY=WORK NOLIST;
    DELETE http_requests_sas users_sas workbooks_sas views_sas;
QUIT;

/* Log success */
%PUT NOTE: Data extraction from PostgreSQL and load to Oracle completed successfully. Check Oracle tables: http_requests_oracle, users_oracle, etc.;
