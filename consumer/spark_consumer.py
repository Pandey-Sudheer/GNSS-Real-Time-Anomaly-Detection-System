"""
Spark Streaming Consumer for GNSS Anomaly Detection
Processes Kafka stream and detects:
- Signal drops (SNR < threshold)
- Position drift
- Satellite loss
"""

import os
import sys
import yaml
import logging
import math
from datetime import datetime
from typing import Dict, List
from collections import deque

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, stddev,
    when, lit, udf, struct, to_timestamp, sqrt, pow, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)
import pyspark.sql.functions as F

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GNSSAnomalyDetector:
    """Real-time GNSS anomaly detection using Spark Streaming"""
    
    def __init__(self, config_path: str = 'config/settings.yaml'):
        """
        Initialize Spark streaming application
        
        Args:
            config_path: Path to configuration file
        """
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.kafka_config = self.config['kafka']
        self.spark_config = self.config['spark']
        self.anomaly_config = self.config['anomaly_detection']
        self.storage_config = self.config['storage']
        self.ntrip_config = self.config.get('ntrip', {})
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        
        # Define schema for GNSS data
        self.schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("altitude", DoubleType(), True),
            StructField("fix_quality", IntegerType(), True),
            StructField("num_sats", IntegerType(), True),
            StructField("hdop", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("heading", DoubleType(), True),
            StructField("satellite_id", StringType(), True),
            StructField("elevation", DoubleType(), True),
            StructField("azimuth", DoubleType(), True),
            StructField("snr", DoubleType(), True),
            StructField("producer_timestamp", StringType(), True)
        ])
        
        # Thresholds
        self.snr_threshold = self.anomaly_config['snr_threshold']
        self.drift_threshold = self.anomaly_config['drift_threshold']
        self.min_satellites = self.anomaly_config['min_satellites']
        self.window_size = self.anomaly_config['moving_avg_window']
        self.reference_lat = self.ntrip_config.get('latitude')
        self.reference_lon = self.ntrip_config.get('longitude')
        
        # Output directory
        self.output_dir = self.storage_config['output_dir']
        os.makedirs(self.output_dir, exist_ok=True)
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        
        # Get Kafka package version compatible with Spark
        spark_version = "3.5"
        scala_version = "2.12"
        kafka_version = "3.5.1"
        
        packages = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}.0"
        
        spark = (SparkSession.builder
                .appName(self.spark_config['app_name'])
                .master(self.spark_config['master'])
                .config("spark.jars.packages", packages)
                .config("spark.sql.streaming.checkpointLocation", 
                       self.spark_config['checkpoint_dir'])
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.streaming.stopGracefullyOnShutdown", "true")
                .getOrCreate())
        
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✓ Spark session created")
        return spark
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        
        df = (self.spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", 
                     self.kafka_config['bootstrap_servers'])
              .option("subscribe", self.kafka_config['topic'])
              .option("startingOffsets", "latest")
              .option("failOnDataLoss", "false")
              .load())
        
        # Parse JSON from Kafka value
        parsed_df = (df
                    .selectExpr("CAST(value AS STRING) as json_str")
                    .select(from_json(col("json_str"), self.schema).alias("data"))
                    .select("data.*"))
        
        # Convert timestamp string to timestamp type
        parsed_df = parsed_df.withColumn(
            "timestamp",
            coalesce(
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col("producer_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
                to_timestamp(col("producer_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            )
        )
        
        logger.info("✓ Kafka stream configured")
        return parsed_df
    
    def detect_signal_drops(self, df):
        """
        Detect signal drops (SNR < threshold)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with signal drop flag
        """
        return df.withColumn(
            "signal_drop",
            when(col("snr") < self.snr_threshold, lit(True))
            .otherwise(lit(False))
        )
    
    def detect_satellite_loss(self, df):
        """
        Detect satellite loss (num_sats < minimum)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with satellite loss flag
        """
        return df.withColumn(
            "satellite_loss",
            when(col("num_sats") < self.min_satellites, lit(True))
            .otherwise(lit(False))
        )
    
    def detect_position_drift(self, df):
        """
        Detect position drift from a configured reference position.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with drift detection
        """
        if self.reference_lat is None or self.reference_lon is None:
            logger.warning(
                "Reference coordinates are not configured; position drift will stay disabled."
            )
            return (df
                .withColumn("lat_diff", lit(None).cast("double"))
                .withColumn("lon_diff", lit(None).cast("double"))
                .withColumn("drift_distance_m", lit(None).cast("double"))
                .withColumn("position_drift", lit(False)))

        lat_ref = float(self.reference_lat)
        lon_ref = float(self.reference_lon)
        meters_per_degree = 111320.0
        lon_scale = meters_per_degree * max(abs(math.cos(math.radians(lat_ref))), 0.000001)

        return (df
            .withColumn("lat_diff", F.abs(col("lat") - lit(lat_ref)))
            .withColumn("lon_diff", F.abs(col("lon") - lit(lon_ref)))
            .withColumn(
                "drift_distance_m",
                sqrt(
                    pow(col("lat_diff") * lit(meters_per_degree), 2) +
                    pow(col("lon_diff") * lit(lon_scale), 2)
                )
            )
            .withColumn(
                "position_drift",
                when(
                    col("lat").isNotNull() &
                    col("lon").isNotNull() &
                    (
                        (col("lat_diff") > lit(self.drift_threshold)) |
                        (col("lon_diff") > lit(self.drift_threshold))
                    ),
                    lit(True)
                ).otherwise(lit(False))
            ))
    
    def aggregate_anomalies(self, df):
        """
        Aggregate and flag anomalies
        
        Args:
            df: Input DataFrame with anomaly flags
            
        Returns:
            DataFrame with anomaly summary
        """
        df_with_anomaly = df.withColumn(
            "anomaly_detected",
            when(
                (col("signal_drop") == True) |
                (col("satellite_loss") == True) |
                (col("position_drift") == True),
                lit(True)
            ).otherwise(lit(False))
        )
        
        # Add anomaly type
        df_with_type = df_with_anomaly.withColumn(
            "anomaly_type",
            F.concat_ws(", ",
                when(col("signal_drop") == True, lit("SIGNAL_DROP")),
                when(col("satellite_loss") == True, lit("SATELLITE_LOSS")),
                when(col("position_drift") == True, lit("POSITION_DRIFT"))
            )
        )
        
        df_with_type = df_with_type.withColumn(
            "anomaly_type",
            when(col("anomaly_type") == "", lit("NONE")).otherwise(col("anomaly_type"))
        )
        
        return df_with_type
    
    def process_stream(self):
        """Main stream processing pipeline"""
        
        logger.info("Starting GNSS anomaly detection stream...")
        
        # Read from Kafka
        input_df = self.read_kafka_stream()
        
        # ✅ Filter bad data
        input_df = input_df.filter(
            (col("lat").isNotNull()) &
            (col("lon").isNotNull()) &
            (col("snr").isNotNull()) &
            (col("num_sats").isNotNull())
        )       
        
        # Apply anomaly detection
        df = self.detect_signal_drops(input_df)
        df = self.detect_satellite_loss(df)
        df = self.detect_position_drift(df)
        df = self.aggregate_anomalies(df)
        
        # --------------------------------------------------
        # ✅ NEW: SEND DATA BACK TO KAFKA (IMPORTANT 🔥)
        # --------------------------------------------------
        kafka_query = (df
            .selectExpr("to_json(struct(*)) AS value")
            .writeStream
            .outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers'])
            .option("topic", self.kafka_config.get("processed_topic", "processed_gnss"))
            .option("checkpointLocation", "results/checkpoints/kafka_out")
            .start())

        logger.info(
            "✓ Streaming to Kafka topic: %s",
            self.kafka_config.get("processed_topic", "processed_gnss")
        )
        
        # --------------------------------------------------
        # (Optional) Console output for debugging
        # --------------------------------------------------
        console_query = (df
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 5)
            .start())
        
        # --------------------------------------------------
        # ❌ REMOVE CSV BLOCK (NOT NEEDED ANYMORE)
        # --------------------------------------------------
        # DELETE THIS FROM YOUR CODE:
        # .format("csv") ❌
        
        # --------------------------------------------------
        # Statistics (optional)
        # --------------------------------------------------
        stats_df = (df
            .withWatermark("timestamp", "10 seconds")
            .groupBy(
                window(col("timestamp"), "30 seconds", "10 seconds"),
                col("anomaly_type")
            )
            .agg(
                count("*").alias("count"),
                avg("snr").alias("avg_snr"),
                avg("num_sats").alias("avg_num_sats")
            ))
        
        stats_query = (stats_df
            .writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .start())
        
        logger.info("✓ Stream processing started")
        
        try:
            console_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\nStopping stream processing...")
            console_query.stop()
            kafka_query.stop()
            stats_query.stop()
            self.spark.stop()
            logger.info("✓ Stream processing stopped")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='GNSS Anomaly Detection with Spark Streaming'
    )
    parser.add_argument('--config', default='config/settings.yaml',
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    try:
        detector = GNSSAnomalyDetector(config_path=args.config)
        detector.process_stream()
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
