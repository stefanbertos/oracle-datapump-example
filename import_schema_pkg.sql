/*
================================================================================
  Oracle 19c Data Pump Schema Import Package

  Purpose: PL/SQL package for importing schemas using DBMS_DATAPUMP
  Execute as SYSDBA in the target PDB (PROD)

  Usage:
    ALTER SESSION SET CONTAINER = PROD_PDB_NAME;
    SET SERVEROUTPUT ON SIZE UNLIMITED
    EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA('HR', 'HR_20240115_143022_%U.dmp', 'DATAPUMP_DIR');
================================================================================
*/

-- Create the package specification
CREATE OR REPLACE PACKAGE IMPORT_SCHEMA_PKG AS

    -- Import a single schema
    PROCEDURE IMPORT_SCHEMA(
        p_schema_name       IN VARCHAR2,
        p_dump_file         IN VARCHAR2,
        p_directory         IN VARCHAR2 DEFAULT 'DATAPUMP_DIR',
        p_parallel          IN NUMBER   DEFAULT 1,  -- Default 1 for XE compatibility
        p_table_exists      IN VARCHAR2 DEFAULT 'REPLACE',  -- SKIP, REPLACE, APPEND, TRUNCATE
        p_remap_schema      IN VARCHAR2 DEFAULT NULL,       -- Format: 'SOURCE_SCHEMA:TARGET_SCHEMA'
        p_remap_tablespace  IN VARCHAR2 DEFAULT NULL        -- Format: 'SOURCE_TS:TARGET_TS'
    );

    -- Get job status
    PROCEDURE GET_JOB_STATUS(
        p_job_name IN VARCHAR2 DEFAULT NULL
    );

    -- Stop a running import job
    PROCEDURE STOP_IMPORT_JOB(
        p_job_name IN VARCHAR2
    );

    -- List available dump files in directory
    PROCEDURE LIST_DUMP_FILES(
        p_directory IN VARCHAR2 DEFAULT 'DATAPUMP_DIR'
    );

END IMPORT_SCHEMA_PKG;
/

-- Create the package body
CREATE OR REPLACE PACKAGE BODY IMPORT_SCHEMA_PKG AS

    -- Private procedure to log messages
    PROCEDURE LOG_MSG(p_message IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' - ' || p_message);
    END LOG_MSG;

    -- Import a single schema
    PROCEDURE IMPORT_SCHEMA(
        p_schema_name       IN VARCHAR2,
        p_dump_file         IN VARCHAR2,
        p_directory         IN VARCHAR2 DEFAULT 'DATAPUMP_DIR',
        p_parallel          IN NUMBER   DEFAULT 1,  -- Default 1 for XE compatibility
        p_table_exists      IN VARCHAR2 DEFAULT 'REPLACE',
        p_remap_schema      IN VARCHAR2 DEFAULT NULL,
        p_remap_tablespace  IN VARCHAR2 DEFAULT NULL
    ) IS
        v_handle        NUMBER;
        v_job_name      VARCHAR2(100);
        v_job_state     VARCHAR2(30);
        v_status        ku$_status;
        v_timestamp     VARCHAR2(20);
        v_log_file      VARCHAR2(200);
        v_pct_done      NUMBER;
        v_ind           NUMBER;
        v_source_schema VARCHAR2(128);
        v_target_schema VARCHAR2(128);
        v_source_ts     VARCHAR2(128);
        v_target_ts     VARCHAR2(128);
        v_pos           NUMBER;
    BEGIN
        -- Generate unique job name and log file
        v_timestamp := TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS');
        v_job_name  := 'IMP_' || UPPER(p_schema_name) || '_' || v_timestamp;
        v_log_file  := UPPER(p_schema_name) || '_' || v_timestamp || '_import.log';

        LOG_MSG('Starting import for schema: ' || UPPER(p_schema_name));
        LOG_MSG('Job Name: ' || v_job_name);
        LOG_MSG('Dump File: ' || p_dump_file);
        LOG_MSG('Log File: ' || v_log_file);
        LOG_MSG('Directory: ' || p_directory);
        LOG_MSG('Table Exists Action: ' || p_table_exists);

        -- Open Data Pump job
        v_handle := DBMS_DATAPUMP.OPEN(
            operation   => 'IMPORT',
            job_mode    => 'SCHEMA',
            job_name    => v_job_name,
            version     => 'LATEST'
        );

        LOG_MSG('Job handle opened: ' || v_handle);

        -- Add dump file
        DBMS_DATAPUMP.ADD_FILE(
            handle      => v_handle,
            filename    => p_dump_file,
            directory   => p_directory,
            filetype    => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE
        );

        -- Add log file
        DBMS_DATAPUMP.ADD_FILE(
            handle      => v_handle,
            filename    => v_log_file,
            directory   => p_directory,
            filetype    => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE,
            reusefile   => 1
        );

        -- Set schema filter
        DBMS_DATAPUMP.METADATA_FILTER(
            handle      => v_handle,
            name        => 'SCHEMA_EXPR',
            value       => 'IN (''' || UPPER(p_schema_name) || ''')'
        );

        -- Set table exists action
        DBMS_DATAPUMP.SET_PARAMETER(
            handle  => v_handle,
            name    => 'TABLE_EXISTS_ACTION',
            value   => UPPER(p_table_exists)
        );

        -- Set parallel degree
        IF p_parallel > 1 THEN
            DBMS_DATAPUMP.SET_PARALLEL(
                handle  => v_handle,
                degree  => p_parallel
            );
            LOG_MSG('Parallel degree set to: ' || p_parallel);
        END IF;

        -- Schema remapping if specified
        IF p_remap_schema IS NOT NULL THEN
            v_pos := INSTR(p_remap_schema, ':');
            IF v_pos > 0 THEN
                v_source_schema := UPPER(TRIM(SUBSTR(p_remap_schema, 1, v_pos - 1)));
                v_target_schema := UPPER(TRIM(SUBSTR(p_remap_schema, v_pos + 1)));

                DBMS_DATAPUMP.METADATA_REMAP(
                    handle      => v_handle,
                    name        => 'REMAP_SCHEMA',
                    old_value   => v_source_schema,
                    value       => v_target_schema
                );
                LOG_MSG('Schema remap: ' || v_source_schema || ' -> ' || v_target_schema);
            END IF;
        END IF;

        -- Tablespace remapping if specified
        IF p_remap_tablespace IS NOT NULL THEN
            v_pos := INSTR(p_remap_tablespace, ':');
            IF v_pos > 0 THEN
                v_source_ts := UPPER(TRIM(SUBSTR(p_remap_tablespace, 1, v_pos - 1)));
                v_target_ts := UPPER(TRIM(SUBSTR(p_remap_tablespace, v_pos + 1)));

                DBMS_DATAPUMP.METADATA_REMAP(
                    handle      => v_handle,
                    name        => 'REMAP_TABLESPACE',
                    old_value   => v_source_ts,
                    value       => v_target_ts
                );
                LOG_MSG('Tablespace remap: ' || v_source_ts || ' -> ' || v_target_ts);
            END IF;
        END IF;

        -- Start the job
        LOG_MSG('Starting Data Pump job...');
        DBMS_DATAPUMP.START_JOB(handle => v_handle);

        -- Monitor job progress
        LOOP
            BEGIN
                DBMS_DATAPUMP.GET_STATUS(
                    handle      => v_handle,
                    mask        => DBMS_DATAPUMP.KU$_STATUS_JOB_STATUS +
                                   DBMS_DATAPUMP.KU$_STATUS_JOB_ERROR +
                                   DBMS_DATAPUMP.KU$_STATUS_WIP,
                    timeout     => 10,
                    job_state   => v_job_state,
                    status      => v_status
                );

                -- Get percentage done if available
                IF v_status IS NOT NULL AND v_status.job_status IS NOT NULL THEN
                    v_pct_done := v_status.job_status.percent_done;
                    LOG_MSG('Progress: ' || NVL(v_pct_done, 0) || '% - State: ' || v_job_state);
                END IF;

                -- Check for errors
                IF v_status IS NOT NULL AND v_status.error IS NOT NULL THEN
                    v_ind := v_status.error.FIRST;
                    WHILE v_ind IS NOT NULL LOOP
                        LOG_MSG('ERROR: ' || v_status.error(v_ind).logtext);
                        v_ind := v_status.error.NEXT(v_ind);
                    END LOOP;
                END IF;

                -- Exit when job is done
                EXIT WHEN v_job_state IN ('COMPLETED', 'STOPPED', 'NOT RUNNING');

            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE = -31626 THEN
                        EXIT;
                    ELSE
                        RAISE;
                    END IF;
            END;
        END LOOP;

        -- Detach from job
        BEGIN
            DBMS_DATAPUMP.DETACH(handle => v_handle);
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        LOG_MSG('Import completed with state: ' || v_job_state);
        LOG_MSG('Log file location: ' || p_directory || '/' || v_log_file);

        -- Recompile invalid objects
        LOG_MSG('Recompiling schema objects...');
        BEGIN
            IF p_remap_schema IS NOT NULL THEN
                DBMS_UTILITY.COMPILE_SCHEMA(v_target_schema, FALSE);
            ELSE
                DBMS_UTILITY.COMPILE_SCHEMA(UPPER(p_schema_name), FALSE);
            END IF;
            LOG_MSG('Schema recompilation completed');
        EXCEPTION
            WHEN OTHERS THEN
                LOG_MSG('Warning during recompilation: ' || SQLERRM);
        END;

    EXCEPTION
        WHEN OTHERS THEN
            LOG_MSG('ERROR: ' || SQLERRM);
            BEGIN
                DBMS_DATAPUMP.DETACH(handle => v_handle);
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END;
            RAISE;
    END IMPORT_SCHEMA;

    -- Get job status
    PROCEDURE GET_JOB_STATUS(
        p_job_name IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        LOG_MSG('Current Data Pump Jobs:');
        LOG_MSG('============================================');

        FOR rec IN (
            SELECT owner_name, job_name, operation, job_mode, state,
                   degree, attached_sessions
            FROM dba_datapump_jobs
            WHERE (p_job_name IS NULL OR job_name = UPPER(p_job_name))
            ORDER BY job_name
        ) LOOP
            LOG_MSG('Job: ' || rec.job_name);
            LOG_MSG('  Owner: ' || rec.owner_name);
            LOG_MSG('  Operation: ' || rec.operation);
            LOG_MSG('  Mode: ' || rec.job_mode);
            LOG_MSG('  State: ' || rec.state);
            LOG_MSG('  Parallel: ' || rec.degree);
            LOG_MSG('  Sessions: ' || rec.attached_sessions);
            LOG_MSG('');
        END LOOP;

    END GET_JOB_STATUS;

    -- Stop a running import job
    PROCEDURE STOP_IMPORT_JOB(
        p_job_name IN VARCHAR2
    ) IS
        v_handle NUMBER;
    BEGIN
        LOG_MSG('Attempting to stop job: ' || p_job_name);

        v_handle := DBMS_DATAPUMP.ATTACH(
            job_name    => UPPER(p_job_name),
            job_owner   => USER
        );

        DBMS_DATAPUMP.STOP_JOB(
            handle      => v_handle,
            immediate   => 1,
            keep_master => 0
        );

        LOG_MSG('Job stopped successfully');

    EXCEPTION
        WHEN OTHERS THEN
            LOG_MSG('Error stopping job: ' || SQLERRM);
            RAISE;
    END STOP_IMPORT_JOB;

    -- List available dump files in directory
    PROCEDURE LIST_DUMP_FILES(
        p_directory IN VARCHAR2 DEFAULT 'DATAPUMP_DIR'
    ) IS
        v_dir_path VARCHAR2(500);
    BEGIN
        -- Get directory path
        SELECT directory_path INTO v_dir_path
        FROM dba_directories
        WHERE directory_name = UPPER(p_directory);

        LOG_MSG('Directory: ' || p_directory);
        LOG_MSG('Path: ' || v_dir_path);
        LOG_MSG('============================================');
        LOG_MSG('Note: List dump files using OS command:');
        LOG_MSG('  ls -la ' || v_dir_path || '/*.dmp');
        LOG_MSG('');
        LOG_MSG('Or query from V$DATAFILE_HEADER if using ASM');

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            LOG_MSG('Directory ' || p_directory || ' not found');
    END LIST_DUMP_FILES;

END IMPORT_SCHEMA_PKG;
/

-- Show any compilation errors
SHOW ERRORS PACKAGE IMPORT_SCHEMA_PKG;
SHOW ERRORS PACKAGE BODY IMPORT_SCHEMA_PKG;

/*
================================================================================
  USAGE EXAMPLES
================================================================================

-- 1. Connect as SYSDBA and set PDB context (PROD)
sqlplus / as sysdba
ALTER SESSION SET CONTAINER = PROD_PDB_NAME;
SET SERVEROUTPUT ON SIZE UNLIMITED

-- 2. Install the package
@import_schema_pkg.sql

-- 3. Import single schema (same name)
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA('HR', 'HR_20240115_143022_%U.dmp', 'DATAPUMP_DIR');

-- 4. Import with schema remap (different name in PROD)
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA(
    p_schema_name      => 'HR',
    p_dump_file        => 'HR_20240115_143022_%U.dmp',
    p_directory        => 'DATAPUMP_DIR',
    p_remap_schema     => 'HR:HR_PROD',
    p_table_exists     => 'REPLACE'
);

-- 5. Import with tablespace remap
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA(
    p_schema_name      => 'HR',
    p_dump_file        => 'HR_20240115_143022_%U.dmp',
    p_directory        => 'DATAPUMP_DIR',
    p_remap_tablespace => 'UAT_DATA:PROD_DATA',
    p_table_exists     => 'REPLACE'
);

-- 6. Check running jobs
EXEC IMPORT_SCHEMA_PKG.GET_JOB_STATUS;

-- 7. Stop a running job
EXEC IMPORT_SCHEMA_PKG.STOP_IMPORT_JOB('IMP_HR_20240115_150000');

================================================================================
*/
