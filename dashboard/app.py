"""
Real-time GNSS Monitoring Dashboard
Displays live GNSS data, anomalies, and visualizations
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import folium_static
import os
import glob
import time
from datetime import datetime, timedelta
import yaml
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.anomaly_analyzer import AnomalyAnalyzer

from kafka import KafkaConsumer
import json

@st.cache_resource
def get_kafka_consumer():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_PROCESSED_TOPIC", "processed_gnss")
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=10000
    )

def read_kafka_data(max_messages=100):
    consumer = get_kafka_consumer()
    data = []

    try:
        msgs = consumer.poll(timeout_ms=2000)

        for tp, messages in msgs.items():
            for msg in messages:
                data.append(msg.value)

    except Exception as e:
        st.error(f"Kafka read error: {e}")

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    
    if 'producer_timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['producer_timestamp'], errors='coerce')

    # Clean
    for col in ["lat", "lon", "snr", "num_sats"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["lat", "lon"])

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    return df.tail(100)

# Page configuration
st.set_page_config(
    page_title="GNSS Anomaly Detection",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .critical { color: #ff4b4b; }
    .warning { color: #ffa500; }
    .ok { color: #00cc00; }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=2)
def load_config(config_path=None):
    """Load configuration file"""
    config_path = config_path or os.getenv("APP_CONFIG", "config/settings.yaml")
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        st.error(f"Configuration file not found: {config_path}")
        return None


def save_anomalies(df, file_path='results/anomaly_history.csv'):
    """Save detected anomalies to a persistent CSV file."""
    if df.empty or 'anomaly_detected' not in df.columns:
        return
        
    anomalies = df[df['anomaly_detected'] == True]
    if anomalies.empty:
        return
        
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    if os.path.exists(file_path):
        try:
            existing = pd.read_csv(file_path)
            combined = pd.concat([existing, anomalies])
            if 'timestamp' in combined.columns and 'satellite_id' in combined.columns:
                combined = combined.drop_duplicates(subset=['timestamp', 'satellite_id'])
            combined.to_csv(file_path, index=False)
        except Exception:
            anomalies.to_csv(file_path, index=False)
    else:
        anomalies.to_csv(file_path, index=False)


def generate_sample_data(num_points=100):
    """Generate sample data for demonstration"""
    import numpy as np
    
    timestamps = pd.date_range(
        start=datetime.now() - timedelta(minutes=10),
        end=datetime.now(),
        periods=num_points
    )
    
    current_time_offset = (datetime.now().timestamp() % 86400) * 0.00005
    current_lat = -33.865 + current_time_offset
    current_lon = 151.209 + current_time_offset
    
    data = []
    for i, ts in enumerate(timestamps):
        # Simulate normal movement
        current_lat += 0.00005
        current_lon += 0.00005
        
        # Decide timestamp-level anomalies
        rand_val = np.random.random()
        num_sats = np.random.randint(8, 14)
        is_sat_loss = False
        is_pos_drift = False
        
        if rand_val < 0.03:
            # 3% chance of SATELLITE_LOSS
            num_sats = np.random.randint(2, 4)
            is_sat_loss = True
        elif rand_val < 0.06:
            # 3% chance of POSITION_DRIFT
            current_lat += 0.005  # sudden jump
            current_lon -= 0.005
            is_pos_drift = True

        # Simulate multiple satellites
        for sat in range(1, num_sats + 1):
            snr = np.random.normal(35, 10)
            is_signal_drop = False
            anomaly = False
            anomaly_type = "NONE"
            
            if is_sat_loss:
                anomaly = True
                anomaly_type = "SATELLITE_LOSS"
            elif is_pos_drift:
                anomaly = True
                anomaly_type = "POSITION_DRIFT"
            elif np.random.random() < 0.05:  # 5% chance of single satellite SIGNAL_DROP
                snr = np.random.uniform(5, 15)
                is_signal_drop = True
                anomaly = True
                anomaly_type = "SIGNAL_DROP"
            
            data.append({
                'timestamp': ts,
                'lat': current_lat + np.random.uniform(-0.00001, 0.00001),
                'lon': current_lon + np.random.uniform(-0.00001, 0.00001),
                'satellite_id': f'G{sat:02d}',
                'snr': max(0, snr),
                'num_sats': num_sats,
                'hdop': np.random.uniform(0.8, 2.0) if not is_sat_loss else np.random.uniform(4.0, 8.0),
                'anomaly_detected': anomaly,
                'anomaly_type': anomaly_type,
                'signal_drop': is_signal_drop,
                'satellite_loss': is_sat_loss,
                'position_drift': is_pos_drift
            })
    
    return pd.DataFrame(data)


def create_snr_timeline(df):
    """Create SNR timeline chart"""
    if df.empty:
        return go.Figure()
    
    df = df.copy()
    df['snr'] = pd.to_numeric(df['snr'], errors='coerce')
    df['num_sats'] = pd.to_numeric(df['num_sats'], errors='coerce')
    df = df.dropna(subset=['snr', 'timestamp'])
    
    fig = go.Figure()
    
    # Ensure satellite_id is string
    df['satellite_id'] = df['satellite_id'].astype(str)
    
    # Group by satellite
    for sat_id in df['satellite_id'].unique()[:10]:
        sat_data = df[df['satellite_id'] == sat_id].sort_values('timestamp')
        
        fig.add_trace(go.Scatter(
            x=sat_data['timestamp'],
            y=sat_data['snr'],
            mode='lines+markers',
            name=str(sat_id),   # ✅ FIX HERE
            line=dict(width=2),
            marker=dict(size=4)
        ))
    
    fig.add_hline(
        y=20,
        line_dash="dash",
        line_color="red",
        annotation_text="SNR Threshold"
    )
    
    fig.update_layout(
        title="Signal Strength (SNR) Over Time",
        xaxis_title="Time",
        yaxis_title="SNR (dB)",
        hovermode='x unified',
        height=400,
        showlegend=True
    )
    
    return fig

def create_satellite_count_chart(df):
    """Create satellite count chart"""
    if df.empty:
        return go.Figure()
    
    # Aggregate by timestamp
    sat_count = df.groupby('timestamp')['num_sats'].first().reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=sat_count['timestamp'],
        y=sat_count['num_sats'],
        mode='lines+markers',
        name='Satellites',
        fill='tozeroy',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=6)
    ))
    
    # Add minimum threshold line
    fig.add_hline(
        y=5,
        line_dash="dash",
        line_color="red",
        annotation_text="Minimum Required"
    )
    
    fig.update_layout(
        title="Number of Satellites in View",
        xaxis_title="Time",
        yaxis_title="Number of Satellites",
        height=300,
        hovermode='x unified'
    )
    
    return fig


def create_position_map(df):
    """Create map with position markers"""

    # ✅ Step 1: Check columns FIRST
    if df.empty or 'lat' not in df.columns or 'lon' not in df.columns:
        return folium.Map(location=[-33.865, 151.209], zoom_start=15)

    # ✅ Step 2: Convert to numeric (VERY IMPORTANT)
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    df['snr'] = pd.to_numeric(df.get('snr', 0), errors='coerce')
    df['num_sats'] = pd.to_numeric(df.get('num_sats', 0), errors='coerce')

    # ✅ Step 3: Remove invalid rows
    df = df.dropna(subset=["lat", "lon"])

    if df.empty:
        return folium.Map(location=[-33.865, 151.209], zoom_start=15)

    # ✅ Step 4: Sort safely
    if 'timestamp' in df.columns:
        df = df.sort_values('timestamp')

    latest = df.iloc[-1]

    # ✅ Step 5: Create map
    m = folium.Map(
        location=[latest['lat'], latest['lon']],
        zoom_start=15,
        tiles='OpenStreetMap'
    )

    # ✅ Step 6: Recent points
    recent_df = df.tail(50)

    for _, row in recent_df.iterrows():
        if pd.notna(row['lat']) and pd.notna(row['lon']):

            color = 'red' if row.get('anomaly_detected', False) else 'blue'

            snr_val = row['snr'] if pd.notna(row['snr']) else 0

            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=3,
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.6,
                popup=f"Time: {row.get('timestamp','N/A')}<br>SNR: {snr_val:.1f} dB"
            ).add_to(m)

    # ✅ Step 7: Current marker
    snr_val = latest['snr'] if pd.notna(latest['snr']) else 0
    sats_val = latest['num_sats'] if pd.notna(latest['num_sats']) else 0

    folium.Marker(
        location=[latest['lat'], latest['lon']],
        popup=f"Current Position<br>SNR: {snr_val:.1f} dB<br>Sats: {sats_val}",
        icon=folium.Icon(color='green', icon='info-sign')
    ).add_to(m)

    return m


def create_anomaly_distribution(df):
    """Create anomaly distribution pie chart"""
    if df.empty or 'anomaly_type' not in df.columns:
        return go.Figure()
    
    anomaly_counts = df[df['anomaly_detected'] == True]['anomaly_type'].value_counts()
    
    fig = go.Figure(data=[go.Pie(
        labels=anomaly_counts.index,
        values=anomaly_counts.values,
        hole=0.3
    )])
    
    fig.update_layout(
        title="Anomaly Distribution",
        height=300
    )
    
    return fig


def main():
    """Main dashboard application"""
    
    # Header
    st.title("🛰️ GNSS Real-Time Anomaly Detection Dashboard")
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Load config
        config = load_config()
        
        if config:
            st.success("✓ Configuration loaded")
            
            # Display thresholds
            st.subheader("Detection Thresholds")
            st.metric("SNR Threshold", f"{config['anomaly_detection']['snr_threshold']} dB")
            st.metric("Min Satellites", config['anomaly_detection']['min_satellites'])
            st.metric("Drift Threshold", f"{config['anomaly_detection']['drift_threshold']}°")
        else:
            st.error("✗ Configuration not loaded")
        
        st.markdown("---")
        
        # Refresh controls
        st.subheader("Display Options")
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_interval = st.slider("Refresh interval (s)", 1, 10, 2)
        
        default_sample = os.getenv("DASHBOARD_USE_SAMPLE_DATA", "true").lower() == "true"
        use_sample_data = st.checkbox("Use sample data", value=default_sample, 
                                     help="Use generated sample data for demonstration")
        
        st.markdown("---")
        st.caption("GNSS Anomaly Detection System v1.0")
    
    # Load data
    if use_sample_data:
        df = generate_sample_data(200)
        st.info("📊 Displaying sample data for demonstration")
    else:
        new_df = read_kafka_data()
        
        # Maintain history in session state to show a continuous path
        if 'kafka_history' not in st.session_state:
            st.session_state.kafka_history = pd.DataFrame()
            
        if not new_df.empty:
            st.session_state.kafka_history = pd.concat([
                st.session_state.kafka_history, 
                new_df
            ]).drop_duplicates(subset=['timestamp', 'satellite_id']).tail(1000)
            
            # Save anomalies to persistent storage
            save_anomalies(new_df)
            
        df = st.session_state.kafka_history
        
        if df.empty:
            st.warning("⚠️ No anomaly data found. Start the producer and consumer first.")
            st.stop()
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_records = len(df)
        st.metric("📡 Total Records", f"{total_records:,}")
    
    with col2:
        if not df.empty and 'anomaly_detected' in df.columns:
            anomaly_count = df['anomaly_detected'].sum()
            anomaly_pct = (anomaly_count / len(df) * 100) if len(df) > 0 else 0
        else:
            anomaly_count = 0
            anomaly_pct = 0
        
        st.metric("⚠️ Anomalies Detected", f"{int(anomaly_count)}", 
                 delta=f"{anomaly_pct:.1f}%")
    
    with col3:
        if not df.empty and 'num_sats' in df.columns:
            avg_sats = df['num_sats'].mean()
        else:
            avg_sats = 0
        st.metric("🛰️ Avg Satellites", f"{avg_sats:.1f}")
    
    with col4:
        if not df.empty and 'snr' in df.columns:
            avg_snr = df['snr'].mean()
            status_class = "ok" if avg_snr >= 30 else ("warning" if avg_snr >= 20 else "critical")
        else:
            avg_snr = 0
            status_class = "ok"
        
        st.markdown(f'<div class="metric-card"><h3 class="{status_class}">📶 Avg SNR</h3>'
                   f'<h2 class="{status_class}">{avg_snr:.1f} dB</h2></div>',
                   unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🗺️ Position Map", 
                                       "⚠️ Anomalies", "📈 Statistics"])
    
    with tab1:
        # SNR Timeline
        st.subheader("Signal Strength Timeline")
        snr_fig = create_snr_timeline(df)
        st.plotly_chart(snr_fig, use_container_width=True)
        
        # Satellite count
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Satellite Count")
            sat_fig = create_satellite_count_chart(df)
            st.plotly_chart(sat_fig, use_container_width=True)
        
        with col2:
            st.subheader("Anomaly Distribution")
            anomaly_fig = create_anomaly_distribution(df)
            st.plotly_chart(anomaly_fig, use_container_width=True)
    
    with tab2:
        st.subheader("🗺️ Position Tracking")
        position_map = create_position_map(df)
        folium_static(position_map, width=1200, height=600)
        
        if not df.empty:
            # Position statistics
            col1, col2, col3 = st.columns(3)
            
            latest = df.sort_values('timestamp').iloc[-1]
            
            with col1:
                st.metric("Latitude", f"{latest['lat']:.6f}°")
            with col2:
                st.metric("Longitude", f"{latest['lon']:.6f}°")
            with col3:
                if 'hdop' in latest and pd.notna(latest['hdop']):
                    st.metric("HDOP", f"{latest['hdop']:.2f}")
    
    with tab3:
        st.subheader("⚠️ Recent Anomalies")
        
        if not df.empty and 'anomaly_detected' in df.columns:
            anomalies = df[df['anomaly_detected'] == True].sort_values('timestamp', ascending=False)
            
            if not anomalies.empty:
                # Display table
                display_cols = ['timestamp', 'satellite_id', 'snr', 'num_sats', 
                              'anomaly_type', 'lat', 'lon']
                available_cols = [col for col in display_cols if col in anomalies.columns]
                
                st.dataframe(
                    anomalies[available_cols].head(50),
                    use_container_width=True,
                    height=250
                )
                
                # ----------------- NEW ANALYSIS SECTION -----------------
                st.markdown("---")
                st.subheader("🔍 Anomaly Prevention Guide")
                
                # Let user select an anomaly type to read about
                anomaly_types = ["SIGNAL_DROP", "SATELLITE_LOSS", "POSITION_DRIFT"]
                
                selected_type = st.selectbox(
                    "Select an Anomaly Type to view its prevention strategy:", 
                    anomaly_types,
                    key="prevention_selectbox"
                )
                
                analysis = AnomalyAnalyzer.analyze(selected_type)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.error("🚨 Root Cause Analysis")
                    st.markdown(analysis['reason'])
                with col2:
                    st.success("✅ How to Tackle & Prevent")
                    st.markdown(analysis['tackle'])
                
                st.markdown("---")
                # ---------------------------------------------------------
                
                # Download button
                csv = anomalies.to_csv(index=False)
                st.download_button(
                    label="📥 Download Anomalies CSV",
                    data=csv,
                    file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.success("✅ No anomalies detected!")
        else:
            st.info("No anomaly data available")
    
    with tab4:
        st.subheader("📈 Statistical Analysis")
        
        if not df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**SNR Statistics**")
                if 'snr' in df.columns:
                    st.write(df['snr'].describe())
            
            with col2:
                st.markdown("**Satellite Statistics**")
                if 'num_sats' in df.columns:
                    st.write(df['num_sats'].describe())
            
            # Hourly anomaly trend
            if 'timestamp' in df.columns and 'anomaly_detected' in df.columns:
                df['hour'] = pd.to_datetime(df['timestamp']).dt.floor('h')
                hourly = df.groupby('hour')['anomaly_detected'].sum().reset_index()
                
                fig = px.bar(
                    hourly,
                    x='hour',
                    y='anomaly_detected',
                    title="Anomalies by Hour",
                    labels={'anomaly_detected': 'Number of Anomalies', 'hour': 'Time'}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
