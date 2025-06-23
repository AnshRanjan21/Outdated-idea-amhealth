from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage
import streamlit as st
import pandas as pd
import mysql.connector
import os
import plotly.express as px
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime, timedelta

# ============================= CONFIG =============================

DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'Hexaware@123'  # Raw password
DB_NAME = 'pipelineLogs'
DB_TABLE = 'pipelineRuns'
TRACKER_TABLE = 'pipelineFailureTracker'

# ============================= HELPERS =============================

def get_sqlalchemy_engine():
    safe_password = urllib.parse.quote_plus(DB_PASSWORD)
    return create_engine(
        f"mysql+pymysql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# ============================= INIT =============================

load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")
os.environ["GOOGLE_API_KEY"] = api_key

model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")

# ============================= TAB 1 =============================

def tab1_active_failures():
    st.title("🚨 Active Pipeline Failures")
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {TRACKER_TABLE}")
        failures = cursor.fetchall()
        cursor.close()
        conn.close()

        if not failures:
            st.success("No active pipeline failures.")
            return  # Prevent using 'df' below if no data

        df = pd.DataFrame(failures)
        st.warning(f"Found {len(df)} failed pipeline(s).")
        st.dataframe(df, use_container_width=True)

        pipeline_names = df['Pipeline name'].unique().tolist()
        selected_pipeline = st.selectbox("Select a pipeline to troubleshoot", pipeline_names)

        if selected_pipeline:
            selected_error = df[df['Pipeline name'] == selected_pipeline]['Error'].values[0]

            if st.button("Troubleshoot"):
                with st.spinner("Analyzing the error..."):
                    try:
                        response = model.invoke([
                            SystemMessage(content="You are an Azure Data Factory troubleshooting expert."),
                            HumanMessage(content=f"Given the following error message from an ADF pipeline named '{selected_pipeline}', provide step-by-step debugging suggestions:\n\n{selected_error}")
                        ])
                        st.subheader("Suggested Debugging Steps:")
                        st.markdown(response.content)
                    except Exception as e:
                        st.error(f"LLM error: {e}")

    except mysql.connector.Error as err:
        st.error(f"Database error: {err}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")

# ============================= TAB 2 =============================


# def depricated_tab2_dashboard():
#     st.title("📊 Pipeline Dashboard")

#     try:
#         engine = get_sqlalchemy_engine()

#         # ---------------- View Mode ----------------
#         view_mode = st.radio(
#             "📍 Select View Mode:",
#             ["Latest pipeline status (KPI)", "All runs in date range"],
#             horizontal=True
#         )

#         # ---------------- Date Range Picker ----------------
#         st.markdown("### 📅 Filter Runs by Date Range")
#         start_date = st.date_input("Start date", datetime.now().date() - timedelta(days=1))
#         end_date = st.date_input("End date", datetime.now().date())

#         if start_date > end_date:
#             st.warning("⚠️ Start date must be before end date.")
#             return

#         start_datetime = datetime.combine(start_date, datetime.min.time())
#         end_datetime = datetime.combine(end_date, datetime.max.time())

#         # ---------------- KPI Section ----------------
#         if view_mode == "Latest pipeline status (KPI)":
#             query_kpi = f"""
#                 SELECT t1.`Pipeline name`, t1.`Status`
#                 FROM {DB_TABLE} t1
#                 JOIN (
#                     SELECT `Pipeline name`, MAX(`Run start`) as latest_run
#                     FROM {DB_TABLE}
#                     GROUP BY `Pipeline name`
#                 ) t2
#                 ON t1.`Pipeline name` = t2.`Pipeline name` AND t1.`Run start` = t2.latest_run
#             """
#             df_kpi = pd.read_sql(query_kpi, con=engine)
#         else:
#             query_kpi = f"""
#                 SELECT `Status`
#                 FROM {DB_TABLE}
#                 WHERE `Run start` BETWEEN %s AND %s
#             """
#             df_kpi = pd.read_sql(query_kpi, con=engine, params=[start_datetime, end_datetime])

#         if df_kpi.empty:
#             st.info("No data available for KPIs.")
#             return

#         df_kpi['Status'] = df_kpi['Status'].str.strip().str.capitalize()
#         num_success = (df_kpi['Status'] == 'Success').sum()
#         num_failed = (df_kpi['Status'] == 'Failed').sum()
#         col1, col2 = st.columns(2)
#         col1.metric("✅ Successful", num_success)
#         col2.metric("❌ Failed", num_failed)

#         # ---------------- Donut Chart ----------------
#         query_donut = f"""
#             SELECT `Status`, COUNT(*) as Count
#             FROM {DB_TABLE}
#             WHERE `Run start` BETWEEN %s AND %s
#             GROUP BY `Status`
#         """
#         df_donut = pd.read_sql(query_donut, con=engine, params=[start_datetime, end_datetime])

#         if df_donut.empty:
#             st.info("No pipeline runs in the selected date range.")
#         else:
#             df_donut['Status'] = df_donut['Status'].str.strip().str.capitalize()
#             fig_donut = px.pie(
#                 df_donut,
#                 names='Status',
#                 values='Count',
#                 hole=0.5,
#                 title=f"Run Outcome Distribution ({start_date} to {end_date})",
#                 color='Status',
#                 color_discrete_map={"Success": "#28a745", "Failed": "#dc3545"}
#             )
#             st.plotly_chart(fig_donut, use_container_width=True)

#         # ---------------- Trend Line Chart ----------------
#         st.markdown("### 📈 Daily Pipeline Run Trend")

#         query_trend = f"""
#             SELECT DATE(`Run start`) as RunDate, `Status`, COUNT(*) as Count
#             FROM {DB_TABLE}
#             WHERE `Run start` BETWEEN %s AND %s
#             GROUP BY RunDate, `Status`
#             ORDER BY RunDate
#         """
#         df_trend = pd.read_sql(query_trend, con=engine, params=[start_datetime, end_datetime])

#         if df_trend.empty:
#             st.info("No data to display trend.")
#         else:
#             df_trend['Status'] = df_trend['Status'].str.strip().str.capitalize()
#             fig_trend = px.line(
#                 df_trend,
#                 x="RunDate",
#                 y="Count",
#                 color="Status",
#                 title="Daily Success/Failure Trend",
#                 markers=True
#             )
#             st.plotly_chart(fig_trend, use_container_width=True)

#     except Exception as e:
#         st.error(f"Error: {e}")

def failure_kpis():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Total active failures
        cursor.execute(f"SELECT COUNT(*) as count FROM {TRACKER_TABLE}")
        active_failures = cursor.fetchone()['count']

        # Failures over 6 hours but not yet 24
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {TRACKER_TABLE}
            WHERE `Alert 6hr sent` = 1 AND `Alert 24hr sent` = 0
        """)
        over_6h = cursor.fetchone()['count']

        # Failures over 24 hours
        cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {TRACKER_TABLE}
            WHERE `Alert 24hr sent` = 1
        """)
        over_24h = cursor.fetchone()['count']

        cursor.close()
        conn.close()

        # Display KPIs
        col1, col2, col3 = st.columns(3)
        col1.metric("Active Failures", active_failures)
        col2.metric("Failures over 6 Hours", over_6h)
        col3.metric("Failures over 24 Hours", over_24h)

    except Exception as e:
        st.error(f"Error fetching KPIs: {e}")

def render_pipeline_trend_line():
    st.markdown("### 📈 Pipeline Run Trend (Success vs Failure)")

    try:
        engine = get_sqlalchemy_engine()
        past_30_days = datetime.now() - timedelta(days=30)

        query = f"""
            SELECT DATE(`Run start`) AS RunDate, 
                   `Status`, 
                   COUNT(*) AS Count
            FROM {DB_TABLE}
            WHERE `Run start` >= %s
            GROUP BY RunDate, `Status`
            ORDER BY RunDate
        """

        df = pd.read_sql(query, con=engine, params=[past_30_days])  # ✅ use list

        if df.empty:
            st.info("No pipeline runs found in the past 30 days.")
            return

        df['Status'] = df['Status'].str.strip().str.capitalize()

        fig = px.line(
            df,
            x="RunDate",
            y="Count",
            color="Status",
            markers=True,
            title="Daily Pipeline Run Status Trend (Last 30 Days)"
        )
        fig.update_layout(xaxis_title="Date", yaxis_title="Number of Runs")

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading trend line chart: {e}")

def render_top_5_failed_pipelines():
    st.markdown("Top 5 Pipelines by Failure Count (All-Time)")

    try:
        engine = get_sqlalchemy_engine()

        query = f"""
            SELECT `Pipeline name`, COUNT(*) AS Failures
            FROM {DB_TABLE}
            WHERE `Status` = 'Failed'
            GROUP BY `Pipeline name`
            ORDER BY Failures DESC
            LIMIT 5
        """

        df = pd.read_sql(query, con=engine)

        if df.empty:
            st.info("No failure records found.")
            return

        fig = px.bar(
            df,
            x='Pipeline name',
            y='Failures',
            title='Top 5 Pipelines with Most Failures (All-Time)',
            text='Failures',
            color='Failures',
            color_continuous_scale='oranges'
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(xaxis_title="Pipeline", yaxis_title="Failure Count")

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading top 5 failure bar chart: {e}")

def render_failure_success_donut():
    st.markdown("### Success vs Failure Distribution")

    try:
        engine = get_sqlalchemy_engine()

        query = f"""
            SELECT `Status`, COUNT(*) AS Count
            FROM {DB_TABLE}
            GROUP BY `Status`
        """

        df = pd.read_sql(query, con=engine)

        if df.empty:
            st.info("No pipeline run data available.")
            return

        # Clean and format
        df['Status'] = df['Status'].str.strip().str.capitalize()

        fig = px.pie(
            df,
            names='Status',
            values='Count',
            hole=0.5,  # donut chart
            title='Pipeline Status Distribution',
            color='Status',
            color_discrete_map={"Success": "#28a745", "Failed": "#dc3545"}
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading donut chart: {e}")


def tab2_dashboard():
    failure_kpis()
    render_pipeline_trend_line()
    col1, col2 = st.columns(2)
    with col1:
        render_top_5_failed_pipelines()
    with col2:
        render_failure_success_donut()


# ============================= TAB 3 =============================

def tab3_history():
    st.title("Pipeline Run History")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Fetch distinct pipeline names
        cursor.execute(f"SELECT DISTINCT `Pipeline name` FROM {DB_TABLE}")
        pipeline_names = [row[0] for row in cursor.fetchall()]
        cursor.close()

        if not pipeline_names:
            st.info("No pipelines found.")
            return

        # Dropdown to select pipeline
        selected_pipeline = st.selectbox("Select a pipeline", sorted(pipeline_names))

        if selected_pipeline:
            query = f"""
                SELECT * FROM {DB_TABLE}
                WHERE `Pipeline name` = %s
                ORDER BY `Run start` DESC
                LIMIT 200
            """
            df = pd.read_sql(query, conn, params=[selected_pipeline])
            conn.close()

            if df.empty:
                st.info("No run logs available for this pipeline.")
                return

            # Highlight failed rows
            def highlight_failures(row):
                if str(row['Status']).strip().lower() == 'failed':
                    return ['background-color: #ffe6e6'] * len(row)
                return [''] * len(row)

            st.subheader(f"Last 200 runs for: `{selected_pipeline}`")
            styled_df = df.style.apply(highlight_failures, axis=1)
            st.dataframe(styled_df, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {e}")



# ============================== MAIN ==============================

def main():
    st.set_page_config(page_title="ADF Logs Viewer", layout="wide")
    st.title("Azure Data Factory Toolkit")

    tab1, tab2, tab3 = st.tabs(["Active Failures", "Dashboard", "Pipeline history"])
    with tab1:
        tab1_active_failures()
    with tab2:
        tab2_dashboard()
    with tab3:
        tab3_history()

if __name__ == "__main__":
    main()