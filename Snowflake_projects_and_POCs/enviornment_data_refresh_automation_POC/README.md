# Environment Refresh and Data Masking Automation for Snowflake

This document provides a comprehensive overview of the automated environment refresh and data masking solution for Snowflake. The system is designed to periodically refresh a development environment from a production source, ensuring that the development database contains up-to-date data while also protecting sensitive information through dynamic data masking.

[Watch the demonstration video here](https://youtu.be/sayayWlLY3M)

[You can refere script here](https://github.com/sparkbrains/data_engineering/blob/main/Snowflake_projects_and_POCs/enviornment_data_refresh_automation_POC/env_refresh_script.md)
-----

### Component Structure
```
PRODUCTION_DB
├── schemas
    ├── tables

DEVELOPMENT_DB
├── schemas
    ├── tables

UTILITY_DB (Control Database)
├── dynamic_data_masking() - PII sanitization procedure
├── refresh_dev_env() - Environment refresh procedure
└── Task Orchestration:
    ├── backup_script (Parent Task - Every 12 hours)
    ├── data_masking (Child Task - After backup_script)
    └── delete_old_bkups (Child Task - After data_masking)
```
Of course, here is a detailed README file summarizing the project.

-----

## Core Components

The system is built around two stored procedures and three automated tasks that manage the workflow.

### Stored Procedures

1.  **`dynamic_data_masking(tgt_db, dry_run)`**:

      * **What it does**: This is the heart of the data sanitization process. It scans the specified target database (`tgt_db`) for any columns that look like they contain email addresses (e.g., column names with `EMAIL`, `CONTACT`, `MAIL`).
      * **How it works**: For each identified column, it runs an `UPDATE` statement to mask the email addresses (e.g., `test.user@email.com` becomes `te****@email.com`). It includes a `dry_run` mode to report what *would* be changed without actually modifying any data.

2.  **`refresh_dev_env(src_db, tgt_db)`**:

      * **What it does**: This procedure manages the entire database refresh lifecycle.
      * **How it works**:
        1.  **Backup**: It first creates a full clone of the current development database as a timestamped backup (e.g., `DEVELOPMENT_DB_BACKUP_2025_07_08_163000`).
        2.  **Drop & Clone**: It then drops the old development database and creates a fresh clone from the production source.
        3.  **Validation & Rollback**: It performs a quick check to ensure the clone was successful (e.g., by counting tables). If the clone fails or seems incomplete, it automatically restores the environment from the backup it just created.

### Automation Tasks

The process is automated by a dependency chain of three tasks:

1.  **`backup_script` (Root Task)**:

      * **Schedule**: Runs automatically every 12 hours.
      * **Action**: Calls the `refresh_dev_env` procedure to kick off the entire refresh process.

2.  **`data_masking` (Child Task)**:

      * **Trigger**: Runs immediately after `backup_script` completes successfully.
      * **Action**: Calls the `dynamic_data_masking` procedure to sanitize the newly cloned development database.

3.  **`delete_old_bkups` (Child Task)**:

      * **Trigger**: Runs immediately after `data_masking` completes successfully.
      * **Action**: Performs housekeeping by deleting old database backups, ensuring only the **three most recent backups** are retained. This helps manage storage and keep the environment clean.

-----

## Automation Workflow ⚙️

The system operates in a seamless, ordered flow:

1.  **Refresh Initiated**: Every 12 hours, the `backup_script` task starts, cloning production data into the development environment.
2.  **Data Sanitized**: Once the new data is in place, the `data_masking` task automatically runs to find and mask all sensitive email data.
3.  **Cleanup Performed**: Finally, the `delete_old_bkups` task executes, removing any backups that are older than the last three, ensuring resources are managed efficiently.

-----

### Deployment

To set up the system, simply run the provided SQL script. This will create the `UTILITY_DB`, the two stored procedures, and the three tasks.

### Activating the Automation

To turn the automation on, you must **resume** the tasks. It's best practice to resume them in the reverse order of execution:

```sql
-- Activate the automation
ALTER TASK UTILITY_DB.PUBLIC.delete_old_bkups RESUME;
ALTER TASK UTILITY_DB.PUBLIC.data_masking RESUME;
ALTER TASK UTILITY_DB.PUBLIC.backup_script RESUME;
```

### Deactivating the Automation

To pause the entire workflow, you only need to **suspend** the root task (`backup_script`), which will prevent the entire chain from running.

```sql
-- Deactivate the automation
ALTER TASK UTILITY_DB.PUBLIC.backup_script SUSPEND;
```
