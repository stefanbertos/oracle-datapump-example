/*
================================================================================
  Oracle 19c Data Pump Schema Export Package

  Purpose: PL/SQL package for exporting schemas using DBMS_DATAPUMP
  Execute as SYSDBA in the target PDB

  Usage:
    ALTER SESSION SET CONTAINER = YOUR_PDB_NAME;
    SET SERVEROUTPUT ON SIZE UNLIMITED
    EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA('SCHEMA_NAME', 'DATAPUMP_DIR');
================================================================================
*/

-- Create the package specification
CREATE OR REPLACE PACKAGE EXPORT_SCHEMA_PKG AS

    -- Export a single schema
    PROCEDURE EXPORT_SCHEMA(
        p_schema_name   IN VARCHAR2,
        p_directory     IN VARCHAR2 DEFAULT 'DATAPUMP_DIR',
        p_parallel      IN NUMBER   DEFAULT 1,  -- Default 1 for XE compatibility
        p_compression   IN VARCHAR2 DEFAULT 'NONE'  -- XE doesn't support compression
    );

    -- Export multiple schemas
    PROCEDURE EXPORT_SCHEMAS(
        p_schema_list   IN VARCHAR2,  -- Comma-separated list: 'SCHEMA1,SCHEMA2,SCHEMA3'
        p_directory     IN VARCHAR2 DEFAULT 'DATAPUMP_DIR',
        p_parallel      IN NUMBER   DEFAULT 1,  -- Default 1 for XE compatibility
        p_compression   IN VARCHAR2 DEFAULT 'NONE'  -- XE doesn't support compression
    );

    -- Get job status
    PROCEDURE GET_JOB_STATUS(
        p_job_name IN VARCHAR2 DEFAULT NULL
    );

    -- Stop a running export job
    PROCEDURE STOP_EXPORT_JOB(
        p_job_name IN VARCHAR2
    );

    -- List exportable schemas (non-Oracle maintained)
    PROCEDURE LIST_SCHEMAS;

    -- Validate directory exists and has permissions
    PROCEDURE CHECK_DIRECTORY(
        p_directory IN VARCHAR2 DEFAULT 'DATAPUMP_DIR'
    );

END EXPORT_SCHEMA_PKG;
/

-- Create the package body
CREATE OR REPLACE PACKAGE BODY EXPORT_SCHEMA_PKG AS

    -- Private procedure to log messages
    PROCEDURE LOG_MSG(p_message IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' - ' || p_message);
    END LOG_MSG;

    -- Export a single schema
    PROCEDURE EXPORT_SCHEMA(
        p_schema_name   IN VARCHAR2,
        p_directory     IN VARCHAR2 DEFAULT 'DATAPUMP_DIR',
        p_parallel      IN NUMBER   DEFAULT 1,  -- Default 1 for XE compatibility
        p_compression   IN VARCHAR2 DEFAULT 'NONE'  -- XE doesn't support compression
    ) IS
        v_handle        NUMBER;
        v_job_name      VARCHAR2(100);
        v_job_state     VARCHAR2(30);
        v_status        ku$_status;
        v_log_entry     ku$_logentry;
        v_timestamp     VARCHAR2(20);
        v_dump_file     VARCHAR2(200);
        v_log_file      VARCHAR2(200);
        v_pct_done      NUMBER;
        v_ind           NUMBER;
        v_oracle_maintained VARCHAR2(1);
        v_actual_parallel NUMBER := p_parallel;
    BEGIN
        -- Generate unique job name and file names
        v_timestamp := TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS');
        v_job_name  := 'EXP_' || UPPER(p_schema_name) || '_' || v_timestamp;

        -- Use %U only if parallel > 1, otherwise simple filename
        IF p_parallel > 1 THEN
            v_dump_file := UPPER(p_schema_name) || '_' || v_timestamp || '_%U.dmp';
        ELSE
            v_dump_file := UPPER(p_schema_name) || '_' || v_timestamp || '.dmp';
        END IF;
        v_log_file  := UPPER(p_schema_name) || '_' || v_timestamp || '_export.log';

        LOG_MSG('Starting export for schema: ' || UPPER(p_schema_name));
        LOG_MSG('Job Name: ' || v_job_name);
        LOG_MSG('Dump File: ' || v_dump_file);
        LOG_MSG('Log File: ' || v_log_file);
        LOG_MSG('Directory: ' || p_directory);
        LOG_MSG('Parallel: ' || p_parallel);

        -- Verify directory exists
        DECLARE
            v_dir_path VARCHAR2(500);
        BEGIN
            SELECT directory_path INTO v_dir_path
            FROM dba_directories
            WHERE directory_name = UPPER(p_directory);

            LOG_MSG('Directory path: ' || v_dir_path);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20002,
                    'Directory ' || p_directory || ' does not exist. ' ||
                    'Create with: CREATE DIRECTORY ' || p_directory || ' AS ''/path/to/dir'';');
        END;

        -- Verify schema exists and check if Oracle-maintained
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*), MAX(oracle_maintained)
            INTO v_count, v_oracle_maintained
            FROM dba_users
            WHERE username = UPPER(p_schema_name);

            IF v_count = 0 THEN
                RAISE_APPLICATION_ERROR(-20001, 'Schema ' || p_schema_name || ' does not exist');
            END IF;

            -- Warn about Oracle-maintained schemas
            IF v_oracle_maintained = 'Y' THEN
                LOG_MSG('WARNING: ' || p_schema_name || ' is an Oracle-maintained schema.');
                LOG_MSG('WARNING: Export may fail or have restrictions. Consider skipping this schema.');
            END IF;
        END;

        -- Open Data Pump job
        v_handle := DBMS_DATAPUMP.OPEN(
            operation   => 'EXPORT',
            job_mode    => 'SCHEMA',
            job_name    => v_job_name,
            version     => 'LATEST'
        );

        LOG_MSG('Job handle opened: ' || v_handle);

        -- Add dump file
        BEGIN
            DBMS_DATAPUMP.ADD_FILE(
                handle      => v_handle,
                filename    => v_dump_file,
                directory   => p_directory,
                filetype    => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE
            );
        EXCEPTION
            WHEN OTHERS THEN
                LOG_MSG('ERROR adding dump file: ' || SQLERRM);
                LOG_MSG('Check: 1) Directory exists  2) OS path exists  3) Write permissions');
                DBMS_DATAPUMP.DETACH(handle => v_handle);
                RAISE;
        END;

        -- Add log file
        BEGIN
            DBMS_DATAPUMP.ADD_FILE(
                handle      => v_handle,
                filename    => v_log_file,
                directory   => p_directory,
                filetype    => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE
            );
        EXCEPTION
            WHEN OTHERS THEN
                LOG_MSG('ERROR adding log file: ' || SQLERRM);
                DBMS_DATAPUMP.DETACH(handle => v_handle);
                RAISE;
        END;

        -- Set schema filter
        DBMS_DATAPUMP.METADATA_FILTER(
            handle      => v_handle,
            name        => 'SCHEMA_EXPR',
            value       => 'IN (''' || UPPER(p_schema_name) || ''')'
        );

        -- Set parallel degree
        IF p_parallel > 1 THEN
            DBMS_DATAPUMP.SET_PARALLEL(
                handle  => v_handle,
                degree  => p_parallel
            );
            LOG_MSG('Parallel degree set to: ' || p_parallel);
        END IF;

        -- Set compression (skip for NONE - XE doesn't support compression)
        IF p_compression IS NOT NULL AND UPPER(p_compression) != 'NONE' THEN
            BEGIN
                DBMS_DATAPUMP.SET_PARAMETER(
                    handle  => v_handle,
                    name    => 'COMPRESSION',
                    value   => UPPER(p_compression)
                );
                LOG_MSG('Compression set to: ' || p_compression);
            EXCEPTION
                WHEN OTHERS THEN
                    LOG_MSG('WARNING: Compression not supported (ORA-00439). Continuing without compression.');
            END;
        ELSE
            LOG_MSG('Compression: NONE (disabled for XE compatibility)');
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
                    IF SQLCODE = -31626 THEN -- Job doesn't exist anymore
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

        LOG_MSG('Export completed with state: ' || v_job_state);
        LOG_MSG('Dump file location: ' || p_directory || '/' || v_dump_file);
        LOG_MSG('Log file location: ' || p_directory || '/' || v_log_file);

    EXCEPTION
        WHEN OTHERS THEN
            LOG_MSG('ERROR: ' || SQLERRM);
            BEGIN
                DBMS_DATAPUMP.DETACH(handle => v_handle);
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END;
            RAISE;
    END EXPORT_SCHEMA;

    -- Export multiple schemas
    PROCEDURE EXPORT_SCHEMAS(
        p_schema_list   IN VARCHAR2,
        p_directory     IN VARCHAR2 DEFAULT 'DATAPUMP_DIR',
        p_parallel      IN NUMBER   DEFAULT 1,  -- Default 1 for XE compatibility
        p_compression   IN VARCHAR2 DEFAULT 'NONE'  -- XE doesn't support compression
    ) IS
        v_schema_list   VARCHAR2(4000) := p_schema_list;
        v_schema_name   VARCHAR2(128);
        v_pos           NUMBER;
    BEGIN
        LOG_MSG('Starting batch export for schemas: ' || p_schema_list);
        LOG_MSG('============================================');

        -- Parse comma-separated list and export each schema
        WHILE v_schema_list IS NOT NULL LOOP
            v_pos := INSTR(v_schema_list, ',');

            IF v_pos > 0 THEN
                v_schema_name := TRIM(SUBSTR(v_schema_list, 1, v_pos - 1));
                v_schema_list := SUBSTR(v_schema_list, v_pos + 1);
            ELSE
                v_schema_name := TRIM(v_schema_list);
                v_schema_list := NULL;
            END IF;

            IF v_schema_name IS NOT NULL THEN
                LOG_MSG('');
                LOG_MSG('Processing schema: ' || v_schema_name);
                LOG_MSG('--------------------------------------------');

                BEGIN
                    EXPORT_SCHEMA(
                        p_schema_name   => v_schema_name,
                        p_directory     => p_directory,
                        p_parallel      => p_parallel,
                        p_compression   => p_compression
                    );
                EXCEPTION
                    WHEN OTHERS THEN
                        LOG_MSG('Failed to export schema ' || v_schema_name || ': ' || SQLERRM);
                        -- Continue with next schema
                END;
            END IF;
        END LOOP;

        LOG_MSG('');
        LOG_MSG('============================================');
        LOG_MSG('Batch export completed');

    END EXPORT_SCHEMAS;

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

    -- Stop a running export job
    PROCEDURE STOP_EXPORT_JOB(
        p_job_name IN VARCHAR2
    ) IS
        v_handle NUMBER;
    BEGIN
        LOG_MSG('Attempting to stop job: ' || p_job_name);

        -- Attach to the job
        v_handle := DBMS_DATAPUMP.ATTACH(
            job_name    => UPPER(p_job_name),
            job_owner   => USER
        );

        -- Stop the job
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
    END STOP_EXPORT_JOB;

    -- List exportable schemas (non-Oracle maintained)
    PROCEDURE LIST_SCHEMAS IS
    BEGIN
        LOG_MSG('Exportable Schemas (non-Oracle maintained):');
        LOG_MSG('============================================');

        FOR rec IN (
            SELECT username, created, account_status, oracle_maintained
            FROM dba_users
            WHERE oracle_maintained = 'N'
            ORDER BY username
        ) LOOP
            LOG_MSG(RPAD(rec.username, 30) || ' | ' ||
                    TO_CHAR(rec.created, 'YYYY-MM-DD') || ' | ' ||
                    rec.account_status);
        END LOOP;

        LOG_MSG('');
        LOG_MSG('Oracle-maintained schemas (usually should NOT be exported):');
        LOG_MSG('============================================');

        FOR rec IN (
            SELECT username
            FROM dba_users
            WHERE oracle_maintained = 'Y'
            ORDER BY username
        ) LOOP
            LOG_MSG('  ' || rec.username);
        END LOOP;

    END LIST_SCHEMAS;

    -- Validate directory exists and has permissions
    PROCEDURE CHECK_DIRECTORY(
        p_directory IN VARCHAR2 DEFAULT 'DATAPUMP_DIR'
    ) IS
        v_dir_path VARCHAR2(500);
        v_read_priv VARCHAR2(10);
        v_write_priv VARCHAR2(10);
    BEGIN
        LOG_MSG('Checking directory: ' || p_directory);
        LOG_MSG('============================================');

        -- Check if directory exists
        BEGIN
            SELECT directory_path INTO v_dir_path
            FROM dba_directories
            WHERE directory_name = UPPER(p_directory);

            LOG_MSG('Directory exists: YES');
            LOG_MSG('Directory path: ' || v_dir_path);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                LOG_MSG('ERROR: Directory ' || p_directory || ' does not exist!');
                LOG_MSG('Create it with: CREATE DIRECTORY ' || p_directory || ' AS ''/path/to/dir'';');
                RETURN;
        END;

        -- Check privileges
        BEGIN
            SELECT MAX(CASE WHEN privilege = 'READ' THEN 'YES' ELSE 'NO' END),
                   MAX(CASE WHEN privilege = 'WRITE' THEN 'YES' ELSE 'NO' END)
            INTO v_read_priv, v_write_priv
            FROM dba_tab_privs
            WHERE table_name = UPPER(p_directory)
            AND grantee IN (USER, 'PUBLIC');

            LOG_MSG('READ privilege: ' || NVL(v_read_priv, 'NO'));
            LOG_MSG('WRITE privilege: ' || NVL(v_write_priv, 'NO'));

            IF NVL(v_write_priv, 'NO') = 'NO' THEN
                LOG_MSG('WARNING: No WRITE privilege. Grant with:');
                LOG_MSG('  GRANT READ, WRITE ON DIRECTORY ' || p_directory || ' TO ' || USER || ';');
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                LOG_MSG('Could not check privileges: ' || SQLERRM);
        END;

        LOG_MSG('');
        LOG_MSG('IMPORTANT: Verify the OS directory exists:');
        LOG_MSG('  ls -la ' || v_dir_path);
        LOG_MSG('');
        LOG_MSG('If directory does not exist on OS, create it:');
        LOG_MSG('  mkdir -p ' || v_dir_path);
        LOG_MSG('  chown oracle:oinstall ' || v_dir_path);
        LOG_MSG('  chmod 750 ' || v_dir_path);

    END CHECK_DIRECTORY;

END EXPORT_SCHEMA_PKG;
/

-- Show any compilation errors
SHOW ERRORS PACKAGE EXPORT_SCHEMA_PKG;
SHOW ERRORS PACKAGE BODY EXPORT_SCHEMA_PKG;

-- Grant execute to public (optional - remove if security concern)
-- GRANT EXECUTE ON EXPORT_SCHEMA_PKG TO PUBLIC;

/*
================================================================================
  USAGE EXAMPLES
================================================================================

-- 1. Connect as SYSDBA and set PDB context
sqlplus / as sysdba
ALTER SESSION SET CONTAINER = YOUR_PDB_NAME;
SET SERVEROUTPUT ON SIZE UNLIMITED

-- 2. Install the package
@export_schema_pkg.sql

-- 3. Check directory setup (run this first to diagnose issues!)
EXEC EXPORT_SCHEMA_PKG.CHECK_DIRECTORY('DATAPUMP_DIR');

-- 4. List exportable schemas (non-Oracle maintained)
EXEC EXPORT_SCHEMA_PKG.LIST_SCHEMAS;

-- 5. Export single schema
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA('HR', 'DATAPUMP_DIR');

-- 6. Export single schema with parallel (Enterprise Edition only, not XE)
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA(p_schema_name => 'HR', p_directory => 'DATAPUMP_DIR', p_parallel => 4, p_compression => 'ALL');

-- 7. Export multiple schemas
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMAS('HR,OE,SH', 'DATAPUMP_DIR');

-- 8. Check running jobs
EXEC EXPORT_SCHEMA_PKG.GET_JOB_STATUS;

-- 9. Stop a running job
EXEC EXPORT_SCHEMA_PKG.STOP_EXPORT_JOB('EXP_HR_20240115_143022');

================================================================================
*/
