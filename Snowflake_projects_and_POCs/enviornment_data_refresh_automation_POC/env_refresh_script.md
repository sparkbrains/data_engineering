## Components

The solution is comprised of two main stored procedures and a set of three interconnected tasks.

### 1\. `dynamic_data_masking` Stored Procedure

This procedure is responsible for identifying and masking sensitive data in a target database.

#### **Description**

`dynamic_data_masking` scans the information schema of the target database to find columns with names suggesting they contain email addresses (e.g., containing 'EMAIL', 'CONTACT', 'MAIL'). It then updates the values in these columns to a masked format, replacing the middle part of the email with asterisks (e.g., `jo******@example.com`).

#### **Parameters**

  * `tgt_db` (STRING, optional, default: 'DEVELOPMENT\_DB'): The name of the database to perform the masking on.
  * `dry_run` (BOOLEAN, optional, default: FALSE): If set to `TRUE`, the procedure will only report on the columns and rows it would have masked without actually performing any updates.

#### **SQL Definition**

```sql
CREATE OR REPLACE PROCEDURE UTILITY_DB.PUBLIC.dynamic_data_masking(
    tgt_db STRING DEFAULT 'DEVELOPMENT_DB',
    dry_run BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_command STRING;
    result_message STRING := 'PII SANITIZATION RESULTS:\n';
    cursor_result RESULTSET;
    schema_name STRING;
    table_name STRING;
    column_name STRING;
    data_type STRING;
    rows_affected INTEGER := 0;
    total_columns_processed INTEGER := 0;
    total_rows_affected INTEGER := 0;
    count_cursor RESULTSET;
    full_table_name STRING;
    where_clause STRING;
BEGIN
    result_message := result_message || 'Target Database: ' || tgt_db || '\n';

    IF (dry_run) THEN
        result_message := result_message || 'Mode: DRY RUN\n';
    ELSE
        result_message := result_message || 'Mode: LIVE EXECUTION\n';
    END IF;

    result_message := result_message || '----------------------------------------\n';

    -- Get all email-like columns
    sql_command := 'SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ';
    sql_command := sql_command || tgt_db;
    sql_command := sql_command || '.INFORMATION_SCHEMA.COLUMNS WHERE (';
    sql_command := sql_command || 'UPPER(COLUMN_NAME) LIKE ''%EMAIL%'' OR ';
    sql_command := sql_command || 'UPPER(COLUMN_NAME) LIKE ''%CONTACT%'' OR ';
    sql_command := sql_command || 'UPPER(COLUMN_NAME) LIKE ''%MAIL%'') AND ';
    sql_command := sql_command || 'DATA_TYPE IN (''VARCHAR'', ''STRING'', ''TEXT'') AND ';
    sql_command := sql_command || 'TABLE_SCHEMA NOT IN (''INFORMATION_SCHEMA'', ''ACCOUNT_USAGE'')';

    cursor_result := (EXECUTE IMMEDIATE sql_command);

    -- Process each discovered column
    FOR record IN cursor_result DO
        schema_name := record.TABLE_SCHEMA;
        table_name := record.TABLE_NAME;
        column_name := record.COLUMN_NAME;
        data_type := record.DATA_TYPE;

        result_message := result_message || 'Processing: ';
        result_message := result_message || schema_name || '.' || table_name || '.' || column_name;
        result_message := result_message || '\n';

        -- Build full table name
        full_table_name := tgt_db || '.' || schema_name || '.' || table_name;

        -- Build where clause for email validation
        where_clause := ' WHERE ' || column_name || ' IS NOT NULL AND ';
        where_clause := where_clause || column_name || ' != '''' AND ';
        where_clause := where_clause || 'REGEXP_LIKE(' || column_name || ', ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'')';

        IF (NOT dry_run) THEN
            -- Execute the actual masking
            sql_command := 'UPDATE ' || full_table_name || ' SET ';
            sql_command := sql_command || column_name || ' = REGEXP_REPLACE(';
            sql_command := sql_command || column_name || ', ''(.{2}).+(@.+)'', ''\\1****\\2'')';
            sql_command := sql_command || where_clause;

            EXECUTE IMMEDIATE sql_command;

            -- Count masked rows
            sql_command := 'SELECT COUNT(*) as ROW_COUNT FROM ' || full_table_name;
            sql_command := sql_command || ' WHERE ' || column_name || ' LIKE ''%****%''';

            count_cursor := (EXECUTE IMMEDIATE sql_command);
            FOR count_record IN count_cursor DO
                rows_affected := count_record.ROW_COUNT;
            END FOR;

            total_rows_affected := total_rows_affected + rows_affected;
            result_message := result_message || '  > Masked ' || rows_affected || ' rows\n';
        ELSE
            -- Dry run - count potential rows
            sql_command := 'SELECT COUNT(*) as ROW_COUNT FROM ' || full_table_name;
            sql_command := sql_command || where_clause;

            count_cursor := (EXECUTE IMMEDIATE sql_command);
            FOR count_record IN count_cursor DO
                rows_affected := count_record.ROW_COUNT;
            END FOR;

            total_rows_affected := total_rows_affected + rows_affected;
            result_message := result_message || '  > Would mask ' || rows_affected || ' rows\n';
        END IF;

        total_columns_processed := total_columns_processed + 1;
    END FOR;

    result_message := result_message || '----------------------------------------\n';
    result_message := result_message || 'SUMMARY:\n';
    result_message := result_message || 'Columns processed: ' || total_columns_processed || '\n';

    IF (dry_run) THEN
        result_message := result_message || 'Total rows that would be affected: ' || total_rows_affected || '\n';
        result_message := result_message || 'To execute actual masking, call with dry_run => FALSE\n';
    ELSE
        result_message := result_message || 'Total rows affected: ' || total_rows_affected || '\n';
    END IF;

    RETURN result_message;
END;
$$;
```

-----

### 2\. `refresh_dev_env` Stored Procedure

This procedure orchestrates the refresh of the development environment.

#### **Description**

`refresh_dev_env` handles the end-to-end process of updating the development database. It performs the following steps:

1.  Creates a timestamped backup of the current target development database.
2.  Drops the existing development database.
3.  Clones the source production database to create a new development database.
4.  Performs a basic validation to check if the clone was successful. If the clone fails or the validation check does not pass, it automatically restores the development database from the backup.

#### **Parameters**

  * `src_db` (STRING, optional, default: 'PRODUCTION\_DB'): The name of the source database to clone from.
  * `tgt_db` (STRING, optional, default: 'DEVELOPMENT\_DB'): The name of the target database to be refreshed.

#### **SQL Definition**

```sql
CREATE OR REPLACE PROCEDURE UTILITY_DB.PUBLIC.refresh_dev_env(
    src_db STRING DEFAULT 'PRODUCTION_DB',
    tgt_db STRING DEFAULT 'DEVELOPMENT_DB'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    clone_timestamp STRING DEFAULT TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY_MM_DD_HH24_MI_SS');
    backup_db_name STRING DEFAULT tgt_db || '_BACKUP_' || clone_timestamp;
    result_message STRING DEFAULT '';
    error_message STRING;
    table_count INTEGER; -- Declared for validation
    target_database STRING := tgt_db; -- Use variable for consistency
BEGIN
    result_message := 'Starting env refresh at ' || CURRENT_TIMESTAMP()::STRING || '\n';

    -- Creating Backup of current Development Eniornment
    BEGIN
        EXECUTE IMMEDIATE 'CREATE DATABASE ' || backup_db_name || ' CLONE ' || tgt_db;
        result_message := result_message || 'Backup created: ' || backup_db_name || '\n';
    EXCEPTION
        WHEN OTHER THEN
            error_message := 'Failed to create backup: ' || SQLERRM;
            RETURN error_message;
    END;

    -- Droping current Development Enviornment database
    BEGIN
        EXECUTE IMMEDIATE 'DROP DATABASE ' || tgt_db;
        result_message := result_message || 'Dropped development DB.\n';
    EXCEPTION
        WHEN OTHER THEN
            error_message := 'Failed to drop dev DB: ' || SQLERRM;
            RETURN error_message;
    END;

    -- Clone Production DB to Development with Updated Production Data
    BEGIN
        EXECUTE IMMEDIATE 'CREATE DATABASE ' || tgt_db || ' CLONE ' || src_db;
        result_message := result_message || 'Cloned production to development.\n';
    EXCEPTION
        WHEN OTHER THEN
            EXECUTE IMMEDIATE 'CREATE DATABASE ' || tgt_db || ' CLONE ' || backup_db_name;
            error_message := 'Clone failed. Rolled back using backup. Error: ' || SQLERRM;
            RETURN error_message;
    END;

    --  DATA VALIDATION AND ROLLBACK
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_catalog = target_database;

    IF (table_count < 12) THEN  -- hypothetical scnario where we check if the count of tables is less than 12 then we will roll back
        -- Rollback
        EXECUTE IMMEDIATE 'DROP DATABASE ' || target_database;
        EXECUTE IMMEDIATE 'CREATE DATABASE ' || target_database || ' CLONE ' || backup_db_name;
        RETURN 'Clone validation failed - insufficient tables. Restored from backup.';
    END IF;

    result_message := result_message || 'Validation passed: ' || table_count || ' tables found\n';

    RETURN result_message || 'Refresh completed at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;
```

-----

### 3\. Automation Tasks

A series of three tasks automate the entire workflow.

#### `backup_script` Task

  * **Description**: This is the root task that initiates the environment refresh process. It calls the `refresh_dev_env` procedure.

  * **Schedule**: Runs every 12 hours (at 00:00 and 12:00 UTC).

  * **SQL Definition**:

    ```sql
    CREATE OR REPLACE TASK UTILITY_DB.PUBLIC.backup_script
      WAREHOUSE = COMPUTE_WH
      SCHEDULE = 'USING CRON 0 0,12 * * * UTC'
    AS
    BEGIN
      CALL UTILITY_DB.PUBLIC.refresh_dev_env();
    END;
    ```

#### `data_masking` Task

  * **Description**: This is a child task that executes after the `backup_script` task completes successfully. It calls the `dynamic_data_masking` procedure to sanitize the newly refreshed development environment.

  * **Trigger**: Runs after `UTILITY_DB.PUBLIC.backup_script`.

  * **SQL Definition**:

    ```sql
    CREATE OR REPLACE TASK UTILITY_DB.PUBLIC.data_masking
      WAREHOUSE = COMPUTE_WH
      AFTER UTILITY_DB.PUBLIC.backup_script
    AS
    BEGIN
      CALL UTILITY_DB.PUBLIC.dynamic_data_masking();
    END;
    ```

#### `delete_old_bkups` Task

  * **Description**: This final child task runs after the `data_masking` task completes. It is responsible for housekeeping by deleting old backups of the development database, retaining only the three most recent ones.

  * **Trigger**: Runs after `UTILITY_DB.PUBLIC.data_masking`.

  * **SQL Definition**:

    ```sql
    CREATE OR REPLACE TASK UTILITY_DB.PUBLIC.delete_old_bkups
      WAREHOUSE = COMPUTE_WH
      AFTER UTILITY_DB.PUBLIC.data_masking
    AS
    BEGIN
      -- Step 1: Get list of DEV_ databases, ordered by CREATED time desc
      LET dbs ARRAY := (
        SELECT ARRAY_AGG(DATABASE_NAME)
        FROM (
          SELECT DATABASE_NAME
          FROM INFORMATION_SCHEMA.DATABASES
          WHERE DATABASE_NAME LIKE 'DEVELOPMENT_DB_BACKUP%%'
          ORDER BY CREATED DESC
          LIMIT 10 OFFSET 3 -- Keeps 3 most recent, prepares to drop up to 10 older ones
        )
      );

      -- Step 2: Loop through and drop the rest
      FOR i IN 0 TO ARRAY_SIZE(dbs) - 1 DO
        LET db_name VARCHAR := dbs[i];
        EXECUTE IMMEDIATE 'DROP DATABASE IF EXISTS "' || db_name || '"';
      END FOR;

      RETURN 'BACKUP* databases except the 3 most recent have been dropped.';
    END;
    ```

-----

## Automation Workflow

The automation process is a chain of tasks that execute in a specific order:

1.  **`backup_script`**: Kicks off the process on a schedule. It creates a backup of the development environment and then refreshes it with data from the production environment.
2.  **`data_masking`**: Once the environment is successfully refreshed, this task automatically runs to find and mask sensitive data.
3.  **`delete_old_bkups`**: After the data has been masked, this task cleans up any backups that are older than the three most recent ones.

This creates a seamless, automated pipeline for maintaining a secure and up-to-date development environment.

-----

## Setup and Usage

### Deployment

To deploy this solution, execute the entire SQL script in your Snowflake environment. This will create the `UTILITY_DB` and the necessary procedures and tasks.

### Activating and Deactivating the Automation

The automation is controlled by the state of the tasks.

  * **To activate the automated workflow**, resume the tasks in the reverse order of their execution:

    ```sql
    ALTER TASK UTILITY_DB.PUBLIC.delete_old_bkups RESUME;
    ALTER TASK UTILITY_DB.PUBLIC.data_masking RESUME;
    ALTER TASK UTILITY_DB.PUBLIC.backup_script RESUME;
    ```

  * **To deactivate the automated workflow**, suspend the root task:

    ```sql
    ALTER TASK UTILITY_DB.PUBLIC.backup_script SUSPEND;
    ```

    *(Note: Suspending the root task is sufficient to stop the entire chain.)*

### Manual Execution

You can also run the procedures manually for on-demand refreshes or testing.

  * **To run a dry run of the data masking:**

    ```sql
    CALL UTILITY_DB.PUBLIC.dynamic_data_masking('DEVELOPMENT_DB', TRUE);
    ```

  * **To manually refresh the development environment:**

    ```sql
    CALL UTILITY_DB.PUBLIC.refresh_dev_env('PRODUCTION_DB', 'DEVELOPMENT_DB');
    ```
