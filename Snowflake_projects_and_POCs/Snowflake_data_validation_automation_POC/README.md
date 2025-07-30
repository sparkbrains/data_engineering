# Snowflake Data Ingestion & Validation Pipeline with Auto Email Alert

This project is a **Proof of Concept (POC)** for building a **fully automated data ingestion and validation pipeline** using **Snowflake**, with the ability to **detect data quality issues** and **send alert emails** based on configurable thresholds.

---

## 🔧 Project Overview

This POC demonstrates an **end-to-end ETL automation workflow** that includes:

* **Automatic ingestion** of CSV files using Snowflake's Pipe and Stage.
* **Validation and processing** of records through a Python-based stored procedure.
* **Segregation of valid and invalid records** into separate tables.
* **Email alert system** for high error rates.
* **Task scheduler** that automatically runs the pipeline when new data is available.

---

## 🚀 Key Features Implemented

### 1. **Auto Ingestion with Pipe & Stage**

```sql
CREATE OR REPLACE STAGE source_stage;

CREATE OR REPLACE PIPE source_pipe
AUTO_INGEST = TRUE
AS
COPY INTO source_table
FROM @source_stage
FILE_FORMAT = my_csv_format
ON_ERROR=CONTINUE;
```
---

### 2. **Validation and Transformation via Python Stored Procedure**

```sql

CREATE OR REPLACE PROCEDURE PROCESS_DATA_WITH_EMAIL_ALERT_2()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session):
    from snowflake.snowpark.functions import col, when, lit, try_cast, contains, current_timestamp, count
    from snowflake.snowpark.types import IntegerType, DateType
    
    try:
        # Read source data
        # source_df = session.table("source_table")
        source_df = session.table("source_table_stm")
        source_df = source_df.cache_result()

        source_count = source_df.count()
        
        # Create validation conditions
        id_valid = try_cast(col("id"), IntegerType()).is_not_null()
        date_valid = try_cast(col("created_date"), DateType()).is_not_null()
        email_valid = (contains(col("email"), lit("@")) & 
                      contains(col("email"), lit(".")) &
                      col("email").is_not_null() &
                      (col("email") != ""))
        
        # Process valid records
        valid_records = source_df.filter(
            id_valid & date_valid & email_valid
        ).select(
            try_cast(col("id"), IntegerType()).alias("id"),
            col("name"),
            col("email"),
            try_cast(col("created_date"), DateType()).alias("created_date")
        )
        
        # Write valid records
        valid_records.write.mode("append").save_as_table("target_table")
        valid_count = valid_records.count()
        
        # Process invalid records
        invalid_records = source_df.filter(
            ~id_valid | ~date_valid | ~email_valid
        ).select(
            col("id").alias("original_id"),
            col("name").alias("original_name"),
            col("email").alias("original_email"),
            col("created_date").alias("original_date"),
            
            when(~id_valid, lit("Invalid ID format"))
            .when(~date_valid, lit("Invalid date format"))
            .when(~email_valid, lit("Invalid email format"))
            .otherwise(lit("Multiple validation errors"))
            .alias("error_reason"),
            
            when(~id_valid & ~date_valid & ~email_valid, 
                 lit("ID: Cannot convert to integer, Date: Invalid format, Email: Missing @ symbol"))
            .when(~id_valid & ~date_valid, 
                  lit("ID: Cannot convert to integer, Date: Invalid format"))
            .when(~id_valid & ~email_valid, 
                  lit("ID: Cannot convert to integer, Email: Missing @ symbol"))
            .when(~date_valid & ~email_valid, 
                  lit("Date: Invalid format, Email: Missing @ symbol"))
            .when(~id_valid, lit("ID: Cannot convert to integer"))
            .when(~date_valid, lit("Date: Invalid format"))
            .when(~email_valid, lit("Email: Missing @ symbol or invalid format"))
            .otherwise(lit("Unknown validation error"))
            .alias("error_details"),
            
            current_timestamp().alias("error_timestamp")
        )
        
        # Write invalid records
        invalid_records.write.mode("append").save_as_table("error_records")
        error_count = invalid_records.count()
        
        total_processed = valid_count + error_count
        
        # Calculate error percentage
        error_percentage = 0
        if total_processed > 0:
            error_percentage = (error_count / total_processed) * 100
        
        # Get current total count of target table
        total_target_count_result = session.sql("SELECT COUNT(*) FROM target_table").collect()
        total_target_count = total_target_count_result[0][0] if total_target_count_result else 0
        
        # Populate the summary table with exact numbers only
        summary_data = session.create_dataframe([[
            total_target_count,
            source_count,
            valid_count,
            error_count
        ]], schema=[
            "TOTAL_COUNT_OF_TARGET_TABLE",
            "SOURCE_COUNT", 
            "VALID_COUNT",
            "INVALID_RECORDS"
        ])
        
        summary_data.write.mode("append").save_as_table("processing_summary")
        
        # Build result message
        result_message = (f"Data Processing Complete!\\n"
                         f"Source Records: {source_count}\\n"
                         f"Total Processed: {total_processed}\\n"
                         f"Successful Records: {valid_count}\\n"
                         f"Error Records: {error_count}\\n"
                         f"Error Percentage: {error_percentage:.2f}%")
        
        # Check if error percentage exceeds threshold
        ERROR_THRESHOLD = 20.0
        
        if error_percentage > ERROR_THRESHOLD and total_processed > 0:
            try:
                # Prepare detailed alert message
                alert_subject = f"HIGH ERROR RATE ALERT - {error_percentage:.2f}% Errors"
                
                alert_body = f'''HIGH ERROR RATE DETECTED - IMMEDIATE ACTION REQUIRED!

Processing Summary:
===================
- Source Records Found: {source_count}
- Records Processed: {total_processed}
- Successful Records: {valid_count}
- Error Records: {error_count}
- Error Percentage: {error_percentage:.2f}%

Threshold Analysis:
==================
- Configured Threshold: {ERROR_THRESHOLD}%
- Exceeded By: {error_percentage - ERROR_THRESHOLD:.2f} percentage points
- Status: ALERT TRIGGERED

Error Details:
=============
- Invalid ID Records: Check records where ID cannot be converted to integer
- Invalid Date Records: Check records with malformed date formats
- Invalid Email Records: Check records with missing or malformed email addresses

Processed at: {current_timestamp()}

Generated by: Snowflake Data Processing Pipeline'''
                
                # Send email alert using Snowflake's native email function
                # Escape single quotes in the message
                escaped_alert_body = alert_body.replace("'", "''")
                escaped_alert_subject = alert_subject.replace("'", "''")
                
                session.sql(f"""
                    CALL SYSTEM$SEND_EMAIL(
                        'data_alerts',
                        'your_email@gmail.com',
                        '{escaped_alert_subject}',
                        '{escaped_alert_body}'
                    )
                """).collect()
                
                result_message += f"\\nEMAIL ALERT SENT: Error percentage ({error_percentage:.2f}%) exceeded threshold ({ERROR_THRESHOLD}%)"
                
            except Exception as email_error:
                result_message += f"\\nEMAIL ALERT FAILED: {str(email_error)}"
                print(f"Email sending failed: {email_error}")
        
        # Add summary to result
        if error_percentage <= ERROR_THRESHOLD:
            result_message += f"\\nError rate within acceptable limits ({error_percentage:.2f}% <= {ERROR_THRESHOLD}%)"
        
        return result_message
        
    except Exception as e:
        error_message = f"CRITICAL ERROR in data processing: {str(e)}"
        
        # Send critical error alert
        try:
            session.sql(f"""
                CALL SYSTEM$SEND_EMAIL(
                    'data_alerts',
                    'your_email@gmail.com',
                    'CRITICAL ERROR - Data Processing Failed',
                    'Data processing failed with error: {str(e)}\\n\\nPlease investigate immediately.'
                )
            """).collect()
        except:
            pass
            
        return error_message
$$;

call PROCESS_DATA_WITH_EMAIL_ALERT_2();

-- OR WE CAN SCHEDULE IT ON PERIODIC BASIS
CREATE OR REPLACE TASK my_src_stream_task_2
  WAREHOUSE = COMPUTE_WH
  WHEN
    SYSTEM$STREAM_HAS_DATA('source_table_stm')
AS
CALL PROCESS_DATA_WITH_EMAIL_ALERT_2();
```

* Reads records from `source_table_stm`.
* Performs validation on (for demonstrational purposes only):

  * `id` → Should be an integer.
  * `created_date` → Should be a valid date.
  * `email` → Should contain `@` and `.` symbols.
* Segregates data:

  * **Valid records** → Stored in `target_table`.
  * **Invalid records** → Stored in `error_records` with descriptive `error_reason` and `error_details`.

---

### 3. **Dynamic Summary Logging**

```sql
CREATE OR REPLACE TABLE processing_summary (
    total_count_of_target_table NUMBER,
    source_count NUMBER,
    valid_count NUMBER,
    invalid_records NUMBER
);
```

* Inserts summary metrics into `processing_summary` table:

  * Total records in target table
  * Source records
  * Valid records
  * Invalid records

---

### 4. **Email Alerts for High Error Rate**

* If the **error rate exceeds 20%**, the system triggers an email alert using Snowflake's native `SYSTEM$SEND_EMAIL()` function.
* Includes a detailed summary and categorized error explanation.

```sql
                escaped_alert_body = alert_body.replace("'", "''")
                escaped_alert_subject = alert_subject.replace("'", "''")
                
                session.sql(f"""
                    CALL SYSTEM$SEND_EMAIL(
                        'data_alerts',
                        'bambihabole00@gmail.com',
                        '{escaped_alert_subject}',
                        '{escaped_alert_body}'
                    )
                """).collect()
                
                result_message += f"\\nEMAIL ALERT SENT: Error percentage ({error_percentage:.2f}%) exceeded threshold ({ERROR_THRESHOLD}%)"
                
            except Exception as email_error:
                result_message += f"\\nEMAIL ALERT FAILED: {str(email_error)}"
                print(f"Email sending failed: {email_error}")
        
        # Add summary to result
        if error_percentage <= ERROR_THRESHOLD:
            result_message += f"\\nError rate within acceptable limits ({error_percentage:.2f}% <= {ERROR_THRESHOLD}%)"
        
        return result_message
        
    except Exception as e:
        error_message = f"CRITICAL ERROR in data processing: {str(e)}"
        
        # Send critical error alert
        try:
            session.sql(f"""
                CALL SYSTEM$SEND_EMAIL(
                    'data_alerts',
                    'your_email@gmail.com',
                    'CRITICAL ERROR - Data Processing Failed',
                    'Data processing failed with error: {str(e)}\\n\\nPlease investigate immediately.'
                )
            """).collect()
)
```

---

### 5. **Task Automation via Snowflake Task Scheduler**

```sql
CREATE OR REPLACE TASK my_src_stream_task_2
  WHEN SYSTEM$STREAM_HAS_DATA('source_table_stm')
AS
CALL PROCESS_DATA_WITH_EMAIL_ALERT_2();
```

* Monitors the **stream on source\_table\_stm**.
* Automatically **calls the stored procedure** when new data is detected.

---

## 📁 Table Summary

| Table Name           | Purpose                                        |
| -------------------- | ---------------------------------------------- |
| `source_table`       | Auto-ingested raw data                         |
| `source_table_stm`   | Stream on source\_table (for tracking changes) |
| `target_table`       | Stores valid/cleaned records                   |
| `error_records`      | Stores rejected records with reasons           |
| `processing_summary` | Logs summary stats per run                     |

---

## 📬 Email Alert Sample

**Subject:** `HIGH ERROR RATE ALERT - 42.86% Errors`

**Body:**

```
HIGH ERROR RATE DETECTED - IMMEDIATE ACTION REQUIRED!

Processing Summary:
===================
- Source Records Found: 70
- Records Processed: 70
- Successful Records: 40
- Error Records: 30
- Error Percentage: 42.86%

Threshold Analysis:
===================
- Configured Threshold: 20%
- Exceeded By: 22.86 percentage points
- Status: ALERT TRIGGERED

Error Details:
=============
- Invalid ID Records: Check records where ID cannot be converted to integer
- Invalid Date Records: Check records with malformed date formats
- Invalid Email Records: Check records with missing or malformed email addresses
```

---

## 💡 Why This Automation Matters

✅ **Early Detection of Data Issues**
Bad data can silently corrupt analytics. This pipeline detects and reports anomalies *in real-time*.

✅ **Self-Healing Pipeline**
Valid records continue processing without manual intervention. Invalids are quarantined for review.

✅ **Auditable & Transparent**
Summary logs and error tables provide a detailed audit trail.

✅ **Reduced Human Error**
Manual checking of ingested data is eliminated.

✅ **Scalable Architecture**
This framework can easily extend to multiple sources, formats, or downstream targets.
