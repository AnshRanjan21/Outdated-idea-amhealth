# Here's the updated script with the robust 3-level alert system added,
# preserving your existing logic and extending it to cover alerts at 6 and 24 hours.

from datetime import datetime, timedelta
import mysql.connector
import smtplib
import time
import csv
import os
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Configuration
SENDER_EMAIL = "anuj2804j@gmail.com"
SENDER_PASSWORD = "fsue ayla orye xlht"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
RECIPIENT_EMAIL = "enter email"

DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = '@123'
DB_NAME = 'pipelineLogs'
DB_TABLE = 'pipelineRuns'
TRACKER_TABLE = 'pipelineFailureTracker'

SCAN_INTERVAL = 60  # seconds
STATE_FILE = 'pipeline_state.json'

report_dir = os.path.join(os.path.dirname(__file__), 'report')
os.makedirs(report_dir, exist_ok=True)


# Persistent state (to resume from last scan)
def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                content = f.read().strip()
                if not content:
                    return None
                state = json.loads(content)
                return datetime.strptime(state.get("last_processed_time"), '%Y-%m-%d %H:%M:%S')
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Warning: Failed to load state file: {e}")
    return None

def save_state(last_processed_time):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_processed_time": last_processed_time.strftime('%Y-%m-%d %H:%M:%S')}, f)
    except Exception as e:
        print(f"Error writing to state file: {e}")


def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )


def ensure_tracker_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TRACKER_TABLE} (
            `Pipeline name` VARCHAR(100) PRIMARY KEY,
            `Last status` VARCHAR(20),
            `Run start` DATETIME,
            `Run end` DATETIME,
            `Error` TEXT,
            `Run ID` VARCHAR(100),
            `Alert sent` BOOLEAN DEFAULT FALSE,
            `Alert 6hr sent` BOOLEAN DEFAULT FALSE,
            `Alert 24hr sent` BOOLEAN DEFAULT FALSE
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()


def update_tracker_and_get_alert_failures(last_processed_time):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if last_processed_time:
        cursor.execute(f"SELECT * FROM {DB_TABLE} WHERE `Run start` > %s ORDER BY `Run start` ASC", (last_processed_time,))
    else:
        cursor.execute(f"SELECT * FROM {DB_TABLE} ORDER BY `Run start` ASC")

    pipeline_entries = cursor.fetchall()
    updated_failures = []
    max_processed_time = last_processed_time

    for entry in pipeline_entries:
        pipeline_name = entry['Pipeline name']
        status = entry['Status']
        run_id = entry['Run ID']
        run_start = entry['Run start']
        run_end = entry['Run end']
        error = entry['Error']

        if not max_processed_time or run_start > max_processed_time:
            max_processed_time = run_start

        cursor.execute(f"SELECT * FROM {TRACKER_TABLE} WHERE `Pipeline name` = %s", (pipeline_name,))
        result = cursor.fetchone()

        if status == 'Failed':
            if result is None:
                cursor.execute(f"""
                    INSERT INTO {TRACKER_TABLE}
                    (`Pipeline name`, `Last status`, `Run start`, `Run end`, `Error`, `Run ID`, `Alert sent`, `Alert 6hr sent`, `Alert 24hr sent`)
                    VALUES (%s, %s, %s, %s, %s, %s, FALSE, FALSE, FALSE)
                """, (pipeline_name, status, run_start, run_end, error, run_id))
                updated_failures.append(entry)

            elif run_id != result['Run ID'] or result['Last status'] != 'Failed':
                cursor.execute(f"""
                    UPDATE {TRACKER_TABLE}
                    SET `Last status` = %s, `Run start` = %s, `Run end` = %s,
                        `Error` = %s, `Run ID` = %s,
                        `Alert sent` = FALSE, `Alert 6hr sent` = FALSE, `Alert 24hr sent` = FALSE
                    WHERE `Pipeline name` = %s
                """, (status, run_start, run_end, error, run_id, pipeline_name))
                updated_failures.append(entry)

        elif status == 'Succeeded':
            cursor.execute(f"DELETE FROM {TRACKER_TABLE} WHERE `Pipeline name` = %s", (pipeline_name,))

    conn.commit()
    cursor.close()
    conn.close()
    return updated_failures, max_processed_time


def send_alert_email(failures, alert_type="Initial"):
    if not failures:
        return

    subject = f"Pipeline Failure Alert - {alert_type}"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL

    html_content = f"""
    <html><body>
    <p><b>{alert_type} Alert: The following pipelines are in failed state:</b></p>
    <table border="1" cellpadding="5" cellspacing="0">
        <tr>
            <th>Pipeline Name</th><th>Run Start</th><th>Run End</th><th>Error</th><th>Run ID</th>
        </tr>
    """
    for failure in failures:
        html_content += f"""
        <tr>
            <td>{failure['Pipeline name']}</td>
            <td>{failure['Run start']}</td>
            <td>{failure['Run end']}</td>
            <td>{failure['Error']}</td>
            <td>{failure['Run ID']}</td>
        </tr>
        """
    html_content += "</table></body></html>"
    msg.attach(MIMEText(html_content, 'html'))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())


def send_email_alerts():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    now = datetime.now()

    # Initial Alerts
    cursor.execute(f"""
        SELECT * FROM {TRACKER_TABLE}
        WHERE `Last status` = 'Failed' AND `Alert sent` = FALSE
    """)
    new_failures = cursor.fetchall()
    send_alert_email(new_failures, "Initial")
    for failure in new_failures:
        cursor.execute(f"""
            UPDATE {TRACKER_TABLE} SET `Alert sent` = TRUE
            WHERE `Pipeline name` = %s
        """, (failure['Pipeline name'],))

    # 6-Hour Alerts
    cursor.execute(f"""
        SELECT * FROM {TRACKER_TABLE}
        WHERE `Last status` = 'Failed'
          AND `Alert sent` = TRUE
          AND `Alert 6hr sent` = FALSE
          AND TIMESTAMPDIFF(HOUR, `Run start`, %s) >= 6 
          AND TIMESTAMPDIFF(HOUR, `Run start`, %s) < 24
          
    """, (now,now))
    six_hour_failures = cursor.fetchall()
    send_alert_email(six_hour_failures, "6-Hour Follow-up")
    for failure in six_hour_failures:
        cursor.execute(f"""
            UPDATE {TRACKER_TABLE} SET `Alert 6hr sent` = TRUE
            WHERE `Pipeline name` = %s
        """, (failure['Pipeline name'],))

    # 24-Hour Alerts
    cursor.execute(f"""
        SELECT * FROM {TRACKER_TABLE}
        WHERE `Last status` = 'Failed'
          AND `Alert 6hr sent` = TRUE
          AND `Alert 24hr sent` = FALSE
          AND TIMESTAMPDIFF(HOUR, `Run start`, %s) >= 24
    """, (now,))
    twentyfour_failures = cursor.fetchall()
    send_alert_email(twentyfour_failures, "24-Hour Follow-up")
    for failure in twentyfour_failures:
        cursor.execute(f"""
            UPDATE {TRACKER_TABLE} SET `Alert 24hr sent` = TRUE
            WHERE `Pipeline name` = %s
        """, (failure['Pipeline name'],))

    conn.commit()
    cursor.close()
    conn.close()


def export_failures_to_csv():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {TRACKER_TABLE} WHERE `Last status` = 'Failed'")
    failures = cursor.fetchall()

    if not failures:
        print("No failed records to export.")
        return

    filename = os.path.join(report_dir, f"failed_pipelines_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=failures[0].keys())
        writer.writeheader()
        writer.writerows(failures)

    print(f"CSV report generated at: {filename}")


def main():
    print("Pipeline Monitor Started. Press Ctrl+C to stop.")
    ensure_tracker_table()
    last_processed_time = load_state()

    try:
        while True:
            print("\n[Scan Started]", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            updated_failures, new_processed_time = update_tracker_and_get_alert_failures(last_processed_time)
            if updated_failures:
                print(f"New or updated failures found: {len(updated_failures)}")
            else:
                print("No new or updated failures.")

            send_email_alerts()

            if new_processed_time:
                last_processed_time = new_processed_time
                save_state(last_processed_time)

            for remaining in range(SCAN_INTERVAL, 0, -1):
                print(f"Next scan in: {remaining} seconds", end='\r')
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nTermination requested. Generating final report...")
        export_failures_to_csv()
        print("Application terminated.")


if __name__ == '__main__':
    main()
