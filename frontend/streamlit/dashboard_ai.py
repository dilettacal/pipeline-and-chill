#!/usr/bin/env python3
"""
ChillFlow Observatory - AI-Powered Streamlit Dashboard
A lightweight alternative to the complex Prometheus/Grafana stack with LLM integration.
"""

import os
import time

import anthropic
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg
import requests
import streamlit as st
from openai import OpenAI

# Page config
st.set_page_config(
    page_title="ChillFlow Observatory",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for dark theme
st.markdown(
    """
<style>
    .main {
        background-color: #0e1117;
    }
    .metric-card {
        background-color: #262730;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #3a3a3a;
    }
    .log-entry {
        background-color: #1e1e1e;
        padding: 0.5rem;
        margin: 0.25rem 0;
        border-radius: 0.25rem;
        font-family: monospace;
        font-size: 0.8rem;
    }
    .log-error { border-left: 4px solid #ff4444; }
    .log-warning { border-left: 4px solid #ffaa00; }
    .log-info { border-left: 4px solid #00aaff; }
    .ai-response {
        background-color: #1a1a2e;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #00d4aa;
        margin: 1rem 0;
    }
</style>
""",
    unsafe_allow_html=True,
)

# Title
st.title("🌊 ChillFlow Observatory")
st.markdown("**AI-Powered Real-time monitoring for your data pipeline**")

# AI Configuration
st.sidebar.title("🤖 AI Assistant")
ai_provider = st.sidebar.selectbox("AI Provider", ["OpenAI", "Anthropic", "Disabled"])
ai_enabled = ai_provider != "Disabled"

if ai_enabled:
    if ai_provider == "OpenAI":
        openai_key = st.sidebar.text_input("OpenAI API Key", type="password")
        if openai_key:
            os.environ["OPENAI_API_KEY"] = openai_key
    elif ai_provider == "Anthropic":
        anthropic_key = st.sidebar.text_input("Anthropic API Key", type="password")
        if anthropic_key:
            os.environ["ANTHROPIC_API_KEY"] = anthropic_key

# Main content
col1, col2, col3, col4 = st.columns(4)


# Metrics from the metrics endpoint
@st.cache_data(ttl=5)
def get_metrics_data():
    """Get metrics from the local metrics endpoint."""
    try:
        response = requests.get("http://localhost:8000/metrics", timeout=2)
        if response.status_code == 200:
            return response.text
        return None
    except:
        return None


@st.cache_data(ttl=10)
def get_database_stats():
    """Get database statistics."""
    try:
        with psycopg.connect("postgresql://dev:dev@localhost:5432/chillflow") as conn:
            with conn.cursor() as cur:
                # Get trip counts
                cur.execute("SELECT COUNT(*) FROM stg.complete_trip")
                total_trips = cur.fetchone()[0]

                # Get recent trips (last hour)
                cur.execute(
                    """
                    SELECT COUNT(*) FROM stg.complete_trip
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                """
                )
                recent_trips = cur.fetchone()[0]

                # Get revenue stats
                cur.execute(
                    """
                    SELECT
                        COUNT(*) as trips,
                        SUM(total_amount) as total_revenue,
                        AVG(total_amount) as avg_fare
                    FROM stg.complete_trip
                    WHERE total_amount IS NOT NULL
                """
                )
                revenue_stats = cur.fetchone()

                return {
                    "total_trips": total_trips,
                    "recent_trips": recent_trips,
                    "total_revenue": revenue_stats[1] or 0,
                    "avg_fare": revenue_stats[2] or 0,
                    "trips_with_revenue": revenue_stats[0] or 0,
                }
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


# AI Functions
def query_ai(prompt, context=""):
    """Query AI with context about the system."""
    if not ai_enabled:
        return "AI is disabled. Please configure an API key in the sidebar."

    system_prompt = f"""
    You are an AI assistant for ChillFlow, a real-time data pipeline for NYC taxi data.

    Current System Context:
    {context}

    You can help with:
    - Analyzing metrics and performance
    - Suggesting optimizations
    - Explaining anomalies
    - Providing insights about the data pipeline

    Be concise and helpful. Focus on actionable insights.
    """

    try:
        if ai_provider == "OpenAI" and os.getenv("OPENAI_API_KEY"):
            client = OpenAI()
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=500,
            )
            return response.choices[0].message.content

        elif ai_provider == "Anthropic" and os.getenv("ANTHROPIC_API_KEY"):
            client = anthropic.Anthropic()
            response = client.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=500,
                messages=[{"role": "user", "content": f"{system_prompt}\n\nUser: {prompt}"}],
            )
            return response.content[0].text

    except Exception as e:
        return f"AI query failed: {e}"

    return "Please configure an API key to use AI features."


# Parse metrics data
def parse_metrics(metrics_text):
    """Parse Prometheus metrics text into a dictionary."""
    if not metrics_text:
        return {}

    metrics = {}
    for line in metrics_text.split("\n"):
        if line.startswith("#") or not line.strip():
            continue

        if " " in line:
            parts = line.split(" ")
            if len(parts) >= 2:
                metric_name = parts[0]
                try:
                    value = float(parts[1])
                    metrics[metric_name] = value
                except ValueError:
                    continue

    return metrics


# Get data
metrics_text = get_metrics_data()
metrics = parse_metrics(metrics_text)
db_stats = get_database_stats()

# Display metrics cards
with col1:
    st.metric(
        label="📊 Total Trips",
        value=f"{db_stats['total_trips']:,}" if db_stats else "N/A",
        delta=(
            f"+{db_stats['recent_trips']}" if db_stats and db_stats["recent_trips"] > 0 else None
        ),
    )

with col2:
    st.metric(
        label="💰 Total Revenue",
        value=f"${db_stats['total_revenue']:,.2f}" if db_stats else "N/A",
        delta=f"${db_stats['avg_fare']:.2f} avg" if db_stats else None,
    )

with col3:
    # Get trip events from metrics
    trip_events = 0
    for key, value in metrics.items():
        if "trip_events_total" in key:
            trip_events += value

    st.metric(
        label="📡 Events Processed",
        value=f"{int(trip_events):,}" if trip_events > 0 else "0",
        delta="Live" if trip_events > 0 else "No data",
    )

with col4:
    # Get processing latency
    latency = metrics.get("processing_latency_seconds_sum", 0)
    st.metric(
        label="⏱️ Processing Time",
        value=f"{latency:.2f}s" if latency > 0 else "N/A",
        delta="Total" if latency > 0 else None,
    )

# Tabs for different views
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📈 Metrics", "📊 Database", "🤖 AI Assistant", "📝 Logs", "🔧 System"]
)

with tab1:
    st.header("📈 Real-time Metrics")

    if metrics:
        # Create a DataFrame for easier visualization
        metrics_df = pd.DataFrame(
            [
                {"Metric": "Trip Events", "Value": trip_events},
                {
                    "Metric": "Completed Trips",
                    "Value": metrics.get("trips_completed_total", 0),
                },
                {
                    "Metric": "Failed Trips",
                    "Value": metrics.get("trips_failed_total", 0),
                },
                {"Metric": "Batch Size", "Value": metrics.get("batch_size_sum", 0)},
                {
                    "Metric": "Redis Operations",
                    "Value": metrics.get("redis_operations_total", 0),
                },
                {"Metric": "DQ Failures", "Value": metrics.get("dq_failures_total", 0)},
            ]
        )

        # Bar chart
        fig = px.bar(
            metrics_df,
            x="Metric",
            y="Value",
            title="Current Metrics",
            color="Value",
            color_continuous_scale="viridis",
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Raw metrics
        st.subheader("Raw Metrics Data")
        st.code(metrics_text[:1000] + "..." if len(metrics_text) > 1000 else metrics_text)
    else:
        st.warning("⚠️ No metrics data available. Make sure the metrics server is running.")
        st.info("💡 Run: `make observability metrics` to start the metrics server")

with tab2:
    st.header("📊 Database Statistics")

    if db_stats:
        # Revenue chart
        revenue_data = {
            "Metric": [
                "Total Trips",
                "Recent Trips (1h)",
                "Total Revenue",
                "Avg Fare",
                "Trips with Revenue",
            ],
            "Value": [
                db_stats["total_trips"],
                db_stats["recent_trips"],
                db_stats["total_revenue"],
                db_stats["avg_fare"],
                db_stats["trips_with_revenue"],
            ],
        }

        df = pd.DataFrame(revenue_data)
        st.dataframe(df, use_container_width=True)

        # Simple pie chart for trip distribution
        if db_stats["total_trips"] > 0:
            recent_pct = (db_stats["recent_trips"] / db_stats["total_trips"]) * 100
            fig = go.Figure(
                data=[
                    go.Pie(
                        labels=["Recent (1h)", "Older"],
                        values=[
                            db_stats["recent_trips"],
                            db_stats["total_trips"] - db_stats["recent_trips"],
                        ],
                        hole=0.3,
                    )
                ]
            )
            fig.update_layout(
                title="Trip Distribution",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="white",
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.error("❌ Could not connect to database")

with tab3:
    st.header("🤖 AI Assistant")

    if ai_enabled:
        # Create context for AI
        context = f"""
        System Status:
        - Total Trips: {db_stats['total_trips'] if db_stats else 'Unknown'}
        - Recent Trips (1h): {db_stats['recent_trips'] if db_stats else 'Unknown'}
        - Total Revenue: ${db_stats['total_revenue'] if db_stats else 'Unknown'}
        - Events Processed: {trip_events}
        - Processing Latency: {latency}s
        """

        # Chat interface
        if "messages" not in st.session_state:
            st.session_state.messages = []

        # Display chat history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        # Chat input
        if prompt := st.chat_input("Ask me about your pipeline..."):
            # Add user message
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            # Get AI response
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    response = query_ai(prompt, context)
                    st.markdown(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})

        # Quick actions
        st.subheader("Quick Actions")
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            if st.button("🔍 Analyze Performance"):
                prompt = "Analyze the current performance metrics and suggest optimizations."
                response = query_ai(prompt, context)
                st.markdown(f"<div class='ai-response'>{response}</div>", unsafe_allow_html=True)

        with col_b:
            if st.button("⚠️ Check for Issues"):
                prompt = "Check for any potential issues or anomalies in the system."
                response = query_ai(prompt, context)
                st.markdown(f"<div class='ai-response'>{response}</div>", unsafe_allow_html=True)

        with col_c:
            if st.button("📈 Generate Insights"):
                prompt = "Generate insights about the data pipeline performance and trends."
                response = query_ai(prompt, context)
                st.markdown(f"<div class='ai-response'>{response}</div>", unsafe_allow_html=True)

    else:
        st.info("🤖 Configure an AI provider in the sidebar to enable AI features.")
        st.markdown(
            """
        **AI Features Available:**
        - 📊 **Performance Analysis**: Get AI insights on your metrics
        - 🔍 **Anomaly Detection**: Identify potential issues
        - 💡 **Optimization Suggestions**: Get recommendations for improvements
        - 💬 **Natural Language Queries**: Ask questions about your data
        """
        )

with tab4:
    st.header("📝 Live Logs")

    st.info(
        "💡 This is a simplified log view. For full log aggregation, use the Prometheus/Grafana stack."
    )

    # Simulate some log entries
    log_entries = [
        {
            "timestamp": "2025-10-25 20:12:00",
            "level": "INFO",
            "message": "Trip event producer initialized",
            "service": "trip-event-producer",
        },
        {
            "timestamp": "2025-10-25 20:12:01",
            "level": "INFO",
            "message": "Processing 100 trips",
            "service": "trip-event-producer",
        },
        {
            "timestamp": "2025-10-25 20:12:02",
            "level": "INFO",
            "message": "Generated 500 events",
            "service": "trip-event-producer",
        },
        {
            "timestamp": "2025-10-25 20:12:03",
            "level": "INFO",
            "message": "Events sent to Kafka successfully",
            "service": "trip-event-producer",
        },
        {
            "timestamp": "2025-10-25 20:12:04",
            "level": "WARNING",
            "message": "High memory usage detected",
            "service": "system",
        },
        {
            "timestamp": "2025-10-25 20:12:05",
            "level": "ERROR",
            "message": "Failed to connect to Redis",
            "service": "trip-assembler",
        },
    ]

    # Filter by level
    log_level = st.selectbox("Filter by level", ["ALL", "ERROR", "WARNING", "INFO"])

    filtered_logs = log_entries
    if log_level != "ALL":
        filtered_logs = [log for log in log_entries if log["level"] == log_level]

    for log in filtered_logs:
        level_class = f"log-{log['level'].lower()}"
        st.markdown(
            f"""
        <div class="log-entry {level_class}">
            <strong>{log['timestamp']}</strong> [{log['level']}] {log['service']}: {log['message']}
        </div>
        """,
            unsafe_allow_html=True,
        )

with tab5:
    st.header("🔧 System Status")

    # Check services
    services = {
        "Metrics Server": "http://localhost:8000/metrics",
        "Database": "postgresql://dev:dev@localhost:5432/chillflow",
        "Kafka": "localhost:9092",
        "Redis": "localhost:6379",
    }

    status_col1, status_col2 = st.columns(2)

    with status_col1:
        st.subheader("Service Status")
        for service, endpoint in services.items():
            if service == "Metrics Server":
                try:
                    response = requests.get(endpoint, timeout=2)
                    status = "✅ Online" if response.status_code == 200 else "❌ Offline"
                except:
                    status = "❌ Offline"
            elif service == "Database":
                try:
                    with psycopg.connect(endpoint) as conn:
                        status = "✅ Online"
                except:
                    status = "❌ Offline"
            else:
                status = "🟡 Unknown"

            st.write(f"**{service}**: {status}")

    with status_col2:
        st.subheader("Quick Actions")
        if st.button("🔄 Refresh All Data"):
            st.cache_data.clear()
            st.rerun()

        if st.button("📊 Generate Test Data"):
            st.info("Run: `make stream produce` to generate test events")

        if st.button("🗄️ Check Database"):
            st.info("Run: `make db shell` to connect to database")

# Auto-refresh
auto_refresh = st.sidebar.checkbox("Auto-refresh (5min)", value=True)
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 60, 600, 300)

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
st.markdown("🌊 **ChillFlow Observatory** - AI-Powered monitoring for your data pipeline")
st.markdown("💡 **Tip**: Use `make observability metrics` to start the metrics server")
