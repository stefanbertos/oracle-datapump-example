# Oracle 19c Data Pump - Schema Export/Import Guide

Guide for exporting schemas from UAT and importing to PROD using Oracle Data Pump in a multi-PDB environment.

**Environment:** UAT and PROD are on separate Unix/Linux hosts.

## Prerequisites
Install f.e. Oracle 19g XE 

default oracle user/password = oracle12345



### Directory Setup (Run as SYSDBA on both UAT and PROD)

```sql
-- Connect to CDB
sqlplus / as sysdba

-- Switch to your PDB
ALTER SESSION SET CONTAINER = YOUR_PDB_NAME;

-- Create directory object (adjust path for your OS)
-- Linux/Unix:
CREATE OR REPLACE DIRECTORY DATAPUMP_DIR AS '/u01/app/oracle/datapump';

-- Windows:
-- CREATE OR REPLACE DIRECTORY DATAPUMP_DIR AS 'E:\datapump';

-- Verify directory
SELECT directory_name, directory_path FROM dba_directories WHERE directory_name = 'DATAPUMP_DIR';
```

### Required Grants (Run as SYSDBA)

```sql
-- Connect as SYSDBA
sqlplus / as sysdba

-- IMPORTANT: List available PDBs first
SELECT name, open_mode FROM v$pdbs;

-- CRITICAL: You MUST connect to a PDB before creating local users
-- ORA-65096 error means you're still in CDB$ROOT
ALTER SESSION SET CONTAINER = YOUR_PDB_NAME;

-- Verify you're in the correct PDB (should NOT show CDB$ROOT)
SHOW CON_NAME;

-- Grant privileges to the user who will run exports (e.g., DATAPUMP_ADMIN)
-- Option 1: Create dedicated admin user for Data Pump operations
CREATE USER datapump_admin IDENTIFIED BY "YourSecurePassword123"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;

-- Grant necessary privileges
GRANT CREATE SESSION TO datapump_admin;
GRANT DATAPUMP_EXP_FULL_DATABASE TO datapump_admin;
GRANT DATAPUMP_IMP_FULL_DATABASE TO datapump_admin;
GRANT READ, WRITE ON DIRECTORY DATAPUMP_DIR TO datapump_admin;

-- Option 2: If running as SYSDBA, ensure directory permissions
GRANT READ, WRITE ON DIRECTORY DATAPUMP_DIR TO SYS;

-- Grant to specific schema owners if needed
GRANT READ, WRITE ON DIRECTORY DATAPUMP_DIR TO SCHEMA_OWNER;
```

### Verify Grants

```sql
-- Check directory privileges
SELECT grantee, privilege, directory_name
FROM dba_tab_privs
WHERE table_name = 'DATAPUMP_DIR';

-- Check Data Pump roles
SELECT grantee, granted_role
FROM dba_role_privs
WHERE granted_role LIKE 'DATAPUMP%';
```

## Export Schemas from UAT

### Manual Export (Command Line)

```bash
# Set Oracle environment
export ORACLE_SID=YOUR_CDB_SID
export ORACLE_HOME=/u01/app/oracle/product/19c/dbhome_1
export PATH=$ORACLE_HOME/bin:$PATH

# Export single schema
expdp \"sys@UAT_PDB as sysdba\" \
    SCHEMAS=SCHEMA_NAME \
    DIRECTORY=DATAPUMP_DIR \
    DUMPFILE=SCHEMA_NAME_UAT_%U.dmp \
    LOGFILE=SCHEMA_NAME_UAT_export.log \
    PARALLEL=4 \
    COMPRESSION=ALL \
    FLASHBACK_TIME=SYSTIMESTAMP
```

### Export Using SQLPlus (as SYSDBA)

```sql
-- Connect as SYSDBA
sqlplus / as sysdba

-- Set container to your PDB
ALTER SESSION SET CONTAINER = UAT_PDB_NAME;

-- Check current container
SHOW CON_NAME;

-- List schemas to export
SELECT username, created, account_status
FROM dba_users
WHERE oracle_maintained = 'N'
ORDER BY username;

-- Execute export procedure (install package first - see PL/SQL Packages section)
SET SERVEROUTPUT ON SIZE UNLIMITED
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA('XDB', 'DATAPUMP_DIR');
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA('SCHEMA2', 'DATAPUMP_DIR');
```

## Import Schemas to PROD

### Pre-Import Steps on PROD

```sql
-- Connect as SYSDBA
sqlplus / as sysdba

-- Set container to PROD PDB
ALTER SESSION SET CONTAINER = PROD_PDB_NAME;

-- Check if schema exists (drop if refresh is needed)
SELECT username FROM dba_users WHERE username = 'SCHEMA_NAME';

-- Optional: Drop existing schema for full refresh
-- DROP USER SCHEMA_NAME CASCADE;

-- Verify tablespaces exist
SELECT tablespace_name, status FROM dba_tablespaces;

-- Create tablespaces if needed (match UAT structure)
-- CREATE TABLESPACE ts_name DATAFILE '+DATA' SIZE 1G AUTOEXTEND ON;
```

### Import Command

```bash
# Import single schema
impdp \"sys@PROD_PDB as sysdba\" \
    SCHEMAS=SCHEMA_NAME \
    DIRECTORY=DATAPUMP_DIR \
    DUMPFILE=SCHEMA_NAME_UAT_%U.dmp \
    LOGFILE=SCHEMA_NAME_PROD_import.log \
    PARALLEL=4 \
    TABLE_EXISTS_ACTION=REPLACE \
    REMAP_TABLESPACE=UAT_TS:PROD_TS

# Import with schema remap (different schema name in PROD)
impdp \"sys@PROD_PDB as sysdba\" \
    SCHEMAS=SCHEMA_NAME \
    DIRECTORY=DATAPUMP_DIR \
    DUMPFILE=SCHEMA_NAME_UAT_%U.dmp \
    LOGFILE=SCHEMA_NAME_PROD_import.log \
    REMAP_SCHEMA=SCHEMA_NAME:PROD_SCHEMA_NAME \
    TABLE_EXISTS_ACTION=REPLACE
```

## Multi-Schema Export Script

```bash
#!/bin/bash
# export_all_schemas.sh

SCHEMAS="SCHEMA1 SCHEMA2 SCHEMA3"
PDB_NAME="UAT_PDB"
DIRECTORY="DATAPUMP_DIR"
DATE_STAMP=$(date +%Y%m%d_%H%M%S)

for SCHEMA in $SCHEMAS; do
    echo "Exporting schema: $SCHEMA"
    expdp \"sys@${PDB_NAME} as sysdba\" \
        SCHEMAS=$SCHEMA \
        DIRECTORY=$DIRECTORY \
        DUMPFILE=${SCHEMA}_${DATE_STAMP}_%U.dmp \
        LOGFILE=${SCHEMA}_${DATE_STAMP}_export.log \
        PARALLEL=4 \
        COMPRESSION=ALL
done
```

## Monitoring Export/Import Jobs

```sql
-- View running Data Pump jobs
SELECT owner_name, job_name, operation, job_mode, state, degree
FROM dba_datapump_jobs
WHERE state = 'EXECUTING';

-- View job progress
SELECT sid, serial#, sofar, totalwork,
       ROUND(sofar/totalwork*100,2) pct_done
FROM v$session_longops
WHERE opname LIKE 'DATAPUMP%'
AND sofar <> totalwork;

-- Attach to running job (from command line)
-- expdp \"sys@PDB as sysdba\" ATTACH=SYS_EXPORT_SCHEMA_01
```

## Troubleshooting

### ORA-65096: invalid common user or role name

This error occurs when trying to create a local user while connected to CDB$ROOT instead of a PDB.

```sql
-- Check current container
SHOW CON_NAME;

-- If it shows CDB$ROOT, you need to switch to a PDB
-- List available PDBs
SELECT name, open_mode FROM v$pdbs;

-- Switch to your PDB
ALTER SESSION SET CONTAINER = XEPDB1;  -- or your PDB name

-- Verify switch was successful
SHOW CON_NAME;

-- Now you can create local users without C## prefix
```

### Common Issues

```sql
-- Check for invalid objects after import
SELECT owner, object_type, object_name, status
FROM dba_objects
WHERE owner = 'SCHEMA_NAME'
AND status = 'INVALID';

-- Recompile invalid objects
EXEC DBMS_UTILITY.COMPILE_SCHEMA('SCHEMA_NAME', FALSE);

-- Check Data Pump master table (if job fails)
SELECT * FROM SYS.DATAPUMP_MASTER_TABLE;
```

### Cleanup Failed Jobs

```sql
-- Find orphaned master tables
SELECT owner, table_name
FROM dba_tables
WHERE table_name LIKE 'SYS_EXPORT%' OR table_name LIKE 'SYS_IMPORT%';

-- Drop orphaned master table
DROP TABLE SYS.SYS_EXPORT_SCHEMA_01;

-- Stop a running job
BEGIN
    DBMS_DATAPUMP.STOP_JOB(
        job_name => 'SYS_EXPORT_SCHEMA_01',
        job_owner => 'SYS',
        force => 1
    );
END;
/
```

## File Transfer Between Hosts (UAT to PROD)

Since UAT and PROD are on separate Unix hosts, you must transfer dump files after export.

### Step 1: Verify Export Files on UAT Host

```bash
# On UAT server - check exported files
ls -lh /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp
ls -lh /u01/app/oracle/datapump/SCHEMA_NAME_*.log

# Check total size before transfer
du -sh /u01/app/oracle/datapump/SCHEMA_NAME_*
```

### Step 2: Verify Disk Space on PROD Host

```bash
# On PROD server - check available space in datapump directory
df -h /u01/app/oracle/datapump
```

### Step 3: Transfer Files (Choose One Method)

```bash
# Method 1: Using scp (simple, good for smaller files)
# Run from UAT server:
scp /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp oracle@prod-server:/u01/app/oracle/datapump/
scp /u01/app/oracle/datapump/SCHEMA_NAME_*.log oracle@prod-server:/u01/app/oracle/datapump/

# Method 2: Using rsync (better for large files, supports resume)
# Run from UAT server:
rsync -avz --progress /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp oracle@prod-server:/u01/app/oracle/datapump/

# Method 3: Using rsync with bandwidth limit (avoid network saturation)
rsync -avz --progress --bwlimit=50000 /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp oracle@prod-server:/u01/app/oracle/datapump/

# Method 4: Pull from PROD server (if push not allowed)
# Run from PROD server:
scp oracle@uat-server:/u01/app/oracle/datapump/SCHEMA_NAME_*.dmp /u01/app/oracle/datapump/
```

### Step 4: Verify Transfer on PROD Host

```bash
# On PROD server - verify files arrived correctly
ls -lh /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp

# Compare checksums (run on both servers)
# On UAT:
md5sum /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp

# On PROD:
md5sum /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp

# Set correct ownership (if needed)
chown oracle:oinstall /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp
chmod 640 /u01/app/oracle/datapump/SCHEMA_NAME_*.dmp
```

### Batch Transfer Script

```bash
#!/bin/bash
# transfer_dumps.sh - Run on UAT server

UAT_DIR="/u01/app/oracle/datapump"
PROD_SERVER="oracle@prod-server"
PROD_DIR="/u01/app/oracle/datapump"
SCHEMAS="SCHEMA1 SCHEMA2 SCHEMA3"

for SCHEMA in $SCHEMAS; do
    echo "Transferring $SCHEMA dump files..."
    rsync -avz --progress ${UAT_DIR}/${SCHEMA}_*.dmp ${PROD_SERVER}:${PROD_DIR}/

    if [ $? -eq 0 ]; then
        echo "$SCHEMA transfer completed successfully"
    else
        echo "ERROR: $SCHEMA transfer failed"
        exit 1
    fi
done

echo "All transfers completed"
```

## PL/SQL Packages for Automation

This repository includes PL/SQL packages for automated schema export/import operations.

### Install Export Package (UAT)

```sql
-- Connect as SYSDBA
sqlplus / as sysdba

-- Set container
ALTER SESSION SET CONTAINER = UAT_PDB_NAME;

-- Install the package
@export_schema_pkg.sql

-- Verify installation
SELECT object_name, object_type, status
FROM dba_objects
WHERE object_name = 'EXPORT_SCHEMA_PKG';
```

### Using Export Package

```sql
SET SERVEROUTPUT ON SIZE UNLIMITED

-- Export single schema
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA('HR', 'DATAPUMP_DIR');

-- Export with custom parallel degree
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA(
    p_schema_name => 'HR',
    p_directory   => 'DATAPUMP_DIR',
    p_parallel    => 8,
    p_compression => 'ALL'
);

-- Export multiple schemas at once
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMAS('HR,OE,SH', 'DATAPUMP_DIR');

-- Check job status
EXEC EXPORT_SCHEMA_PKG.GET_JOB_STATUS;

-- Stop a running job if needed
EXEC EXPORT_SCHEMA_PKG.STOP_EXPORT_JOB('EXP_HR_20240115_143022');
```

### Install Import Package (PROD)

```sql
-- Connect as SYSDBA
sqlplus / as sysdba

-- Set container
ALTER SESSION SET CONTAINER = PROD_PDB_NAME;

-- Install the package
@import_schema_pkg.sql
```

### Using Import Package

```sql
SET SERVEROUTPUT ON SIZE UNLIMITED

-- Import single schema
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA('HR', 'HR_20240115_143022_%U.dmp', 'DATAPUMP_DIR');

-- Import with schema remap
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA(
    p_schema_name      => 'HR',
    p_dump_file        => 'HR_20240115_143022_%U.dmp',
    p_directory        => 'DATAPUMP_DIR',
    p_remap_schema     => 'HR:HR_PROD',
    p_table_exists     => 'REPLACE'
);

-- Import with tablespace remap
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA(
    p_schema_name      => 'HR',
    p_dump_file        => 'HR_20240115_143022_%U.dmp',
    p_directory        => 'DATAPUMP_DIR',
    p_remap_tablespace => 'UAT_DATA:PROD_DATA',
    p_table_exists     => 'REPLACE'
);
```

## Complete Workflow Summary

End-to-end process for migrating schemas from UAT to PROD (separate hosts).

### On UAT Host

```bash
# 1. Copy export_schema_pkg.sql to UAT server
scp export_schema_pkg.sql oracle@uat-server:/home/oracle/

# 2. Connect and install package
sqlplus / as sysdba <<EOF
ALTER SESSION SET CONTAINER = UAT_PDB_NAME;
@/home/oracle/export_schema_pkg.sql
EOF

# 3. Export schemas
sqlplus / as sysdba <<EOF
ALTER SESSION SET CONTAINER = UAT_PDB_NAME;
SET SERVEROUTPUT ON SIZE UNLIMITED
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA('SCHEMA1', 'DATAPUMP_DIR');
EXEC EXPORT_SCHEMA_PKG.EXPORT_SCHEMA('SCHEMA2', 'DATAPUMP_DIR');
EOF

# 4. Transfer dump files to PROD
rsync -avz --progress /u01/app/oracle/datapump/SCHEMA*.dmp oracle@prod-server:/u01/app/oracle/datapump/
```

### On PROD Host

```bash
# 1. Copy import_schema_pkg.sql to PROD server
scp import_schema_pkg.sql oracle@prod-server:/home/oracle/

# 2. Connect and install package
sqlplus / as sysdba <<EOF
ALTER SESSION SET CONTAINER = PROD_PDB_NAME;
@/home/oracle/import_schema_pkg.sql
EOF

# 3. Import schemas
sqlplus / as sysdba <<EOF
ALTER SESSION SET CONTAINER = PROD_PDB_NAME;
SET SERVEROUTPUT ON SIZE UNLIMITED
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA('SCHEMA1', 'SCHEMA1_20240115_143022_%U.dmp', 'DATAPUMP_DIR');
EXEC IMPORT_SCHEMA_PKG.IMPORT_SCHEMA('SCHEMA2', 'SCHEMA2_20240115_150000_%U.dmp', 'DATAPUMP_DIR');
EOF
```

## Best Practices

1. **Always test in lower environment first** before running on PROD
2. **Take RMAN backup** before importing to PROD
3. **Verify disk space** on both source and target directories
4. **Use COMPRESSION=ALL** to reduce dump file size
5. **Use PARALLEL** parameter for large schemas (match CPU cores)
6. **Schedule during low-activity periods** to minimize impact
7. **Keep export logs** for audit and troubleshooting
8. **Validate dump files** before transfer: `impdp ... SQLFILE=ddl.sql`

## Files in This Repository

| File | Description |
|------|-------------|
| `README.md` | This guide |
| `export_schema_pkg.sql` | PL/SQL package for schema exports (install on UAT) |
| `import_schema_pkg.sql` | PL/SQL package for schema imports (install on PROD) |
