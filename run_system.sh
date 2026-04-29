#!/bin/bash

echo "🛰️ GNSS Anomaly Detection System Startup"
echo "=========================================="

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Run: python3 -m venv venv"
    exit 1
fi

source venv/bin/activate

# Function to check if port is in use
check_port() {
    lsof -i :$1 >/dev/null 2>&1
    return $?
}

# Function to start service in background
start_service() {
    echo "📦 Starting $1..."
    $2 &
    echo $! > /tmp/gnss_$3.pid
    sleep $4
}

# Cleanup function
cleanup() {
    echo ""
    echo "🛑 Shutting down services..."
    
    # Kill all background processes
    for pid_file in /tmp/gnss_*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat $pid_file)
            kill $pid 2>/dev/null
            rm $pid_file
        fi
    done
    
    # Stop Kafka and Zookeeper
    kafka-server-stop
    zookeeper-server-stop
    
    exit 0
}

trap cleanup EXIT INT TERM

# Check if Kafka is already running
if check_port 9092; then
    echo "⚠️  Kafka appears to be running already"
else
    # Start Zookeeper
    start_service "Zookeeper" "zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties" "zookeeper" 5
    
    # Start Kafka
    start_service "Kafka" "kafka-server-start /opt/homebrew/etc/kafka/server.properties" "kafka" 10
    
    # Create topic
    echo "📝 Creating Kafka topic..."
    kafka-topics --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic gnss_topic 2>/dev/null
fi

# Start Spark Consumer
start_service "Spark Consumer" "python consumer/spark_streaming.py" "spark" 5

# Start Producer
start_service "Producer" "python producer/kafka_producer.py --source test --delay 2" "producer" 3

# Start Dashboard
echo "📊 Starting Dashboard..."
echo "Dashboard will open at http://localhost:8501"
streamlit run dashboard/app.py

# Wait for all processes
wait