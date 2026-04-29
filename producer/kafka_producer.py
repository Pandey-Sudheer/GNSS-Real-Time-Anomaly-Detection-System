"""
Kafka Producer for GNSS NMEA Data including direct NTRIP streaming
Reads NMEA messages (TCP, File, Test, NTRIP) and publishes JSON to Kafka
"""

import json
import time
import socket
import logging
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import yaml
import sys
import os
import base64
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.nmea_parser import NMEAParser
from utils.rtcm_decoder import RTCMDecoder

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("GNSSProducer")


# ------------------------------------------------------------------------------
#   GNSS Kafka Producer Class
# ------------------------------------------------------------------------------

class GNSSKafkaProducer:
    """Producer that reads NMEA data and publishes to Kafka"""

    def __init__(self, config_path: str = 'config/settings.yaml'):
        """Initialize Kafka producer"""

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Kafka settings
        self.kafka_config = self.config['kafka']
        self.bootstrap_servers = self.kafka_config['bootstrap_servers']
        self.topic = self.kafka_config['topic']
        self.ntrip_config = self.config.get('ntrip', {})

        # Initialize Kafka producer
        self.producer = None
        self._init_producer()

        # Initialize NMEA parser
        self.parser = NMEAParser()

        # Stats
        self.messages_sent = 0
        self.errors = 0
        self.start_time = datetime.now()


    # --------------------------------------------------------------------------
    # Initialize Kafka Producer
    # --------------------------------------------------------------------------

    def _init_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                compression_type='gzip',
                linger_ms=50,
                max_in_flight_requests_per_connection=1,
            )
            logger.info(f"✓ Connected to Kafka at {self.bootstrap_servers}")

        except KafkaError as e:
            logger.error(f"Kafka init failed: {e}")
            raise

    # --------------------------------------------------------------------------
    # Send Message to Kafka
    # --------------------------------------------------------------------------

    def send_message(self, message: dict):
        """Send JSON to Kafka"""

        try:
            message['producer_timestamp'] = datetime.utcnow().isoformat() + 'Z'

            future = self.producer.send(self.topic, value=message)
            future.get(timeout=10)

            self.messages_sent += 1

            if self.messages_sent % 10 == 0:
                logger.info(
                    f"Sent {self.messages_sent} messages | "
                    f"Sat: {message.get('satellite_id', 'N/A')} | "
                    f"SNR: {message.get('snr', 0):.1f}"
                )

        except Exception as e:
            self.errors += 1
            logger.error(f"Send error: {e}")

    # --------------------------------------------------------------------------
    # Process Stream Dispatcher
    # --------------------------------------------------------------------------

    def process_nmea_stream(self, source_type, host, port, filepath, delay,
                             caster=None, caster_port=None, mountpoint=None,
                             user=None, password=None):

        if source_type == 'tcp':
            self._process_tcp_stream(host, port)

        elif source_type == 'file':
            self._process_file_stream(filepath, delay)

        elif source_type == 'test':
            self._generate_test_stream(delay)

        elif source_type == 'ntrip':
            self._process_ntrip_stream(
                caster=caster,
                port=caster_port,
                mountpoint=mountpoint,
                username=user,
                password=password
            )

        else:
            raise ValueError("Unknown source type.")

    # --------------------------------------------------------------------------
    # TCP Stream (RTKNAVI/STRSVR)
    # --------------------------------------------------------------------------

    def _process_tcp_stream(self, host, port):
        logger.info(f"Connecting to TCP {host}:{port}")

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.settimeout(5)

            logger.info("✓ TCP connected")

            buffer = ""
            last_gga = time.time()

            while True:
                data = sock.recv(1024).decode("utf-8", errors="ignore")
                if not data:
                    break

                buffer += data

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()

                    if not line.startswith("$"):
                        continue

                    self.parser.parse_message(line)

                    if 'GGA' in line:
                        if time.time() - last_gga >= 1:
                            for msg in self.parser.to_json_messages():
                                if msg.get('snr') is not None:
                                    self.send_message(msg)
                            last_gga = time.time()

        except Exception as e:
            logger.error(f"TCP error: {e}")

        finally:
            sock.close()
            self.close()

    # --------------------------------------------------------------------------
    # FILE Stream (sample_nmea.txt)
    # --------------------------------------------------------------------------

    def _process_file_stream(self, filepath, delay):
        logger.info(f"Reading file: {filepath}")

        try:
            with open(filepath, 'r') as f:
                last_send = time.time()

                for line in f:
                    line = line.strip()
                    if not line.startswith("$"):
                        continue

                    self.parser.parse_message(line)

                    if ("GGA" in line) or ("GSV" in line):
                        if time.time() - last_send >= delay:

                            for msg in self.parser.to_json_messages():
                                if msg.get('snr') is not None:
                                    self.send_message(msg)

                            last_send = time.time()

                        time.sleep(delay)

        except Exception as e:
            logger.error(f"File error: {e}")

        finally:
            self.close()

    # --------------------------------------------------------------------------
    # NTRIP Live Stream (REAL AUSCORS)
    # --------------------------------------------------------------------------

    def _process_ntrip_stream(self, caster, port, mountpoint, username, password):
        import base64

        logger.info(f"🔗 Connecting to NTRIP caster: {caster}:{port}/{mountpoint}")

        auth = base64.b64encode(f"{username}:{password}".encode()).decode()

        request = (
            f"GET /{mountpoint} HTTP/1.1\r\n"
            f"Host: {caster}\r\n"
            f"Ntrip-Version: Ntrip/2.0\r\n"
            f"User-Agent: GNSS-RTCM-Client\r\n"
            f"Authorization: Basic {auth}\r\n"
            f"Connection: close\r\n\r\n"
        )

        sock = None

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((caster, port))
            sock.send(request.encode())

            logger.info("✓ Connected to NTRIP — streaming RTCM with correct framing")

            # Attach decoder to live socket
            self.rtcm_decoder = RTCMDecoder(
                sock,
                latitude=self.ntrip_config.get("latitude"),
                longitude=self.ntrip_config.get("longitude")
            )

            while True:
                try:
                    messages = self.rtcm_decoder.read_next()

                    for msg in messages:
                        # Now messages *do* have snr
                        if msg.get("snr") is not None:
                            self.send_message(msg)

                except KeyboardInterrupt:
                    logger.info("🛑 CTRL-C received — stopping NTRIP stream...")
                    break

        except Exception as e:
            logger.error(f"NTRIP RTCM error: {e}")

        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass
            logger.info("NTRIP closed.")
            self.close()

    # --------------------------------------------------------------------------
    # Synthetic Test Stream
    # --------------------------------------------------------------------------

    def _generate_test_stream(self, delay):
        """Synthetic GNSS stream"""
        logger.info("Generating test stream...")
        import random

        current_lat = -33.865
        current_lon = 151.209

        sats = [f"G{i:02d}" for i in range(1, 33)]

        try:
            while True:
                timestamp = datetime.utcnow().isoformat() + 'Z'
                count = random.randint(8, 12)
                
                # Introduce anomalies
                rand_val = random.random()
                if rand_val < 0.05:
                    count = random.randint(2, 4) # SATELLITE_LOSS
                elif rand_val < 0.10:
                    current_lat += 0.01 # POSITION_DRIFT (10km jump)
                    current_lon -= 0.01

                visible = random.sample(sats, count)
                
                # Move slightly every tick to simulate travel
                current_lat += random.uniform(0.00005, 0.00015)
                current_lon += random.uniform(0.00005, 0.00015)

                for sat_id in visible:
                    snr = random.uniform(20, 48)
                    if random.random() < 0.05:
                        snr = random.uniform(5, 15) # SIGNAL_DROP

                    msg = {
                        'timestamp': timestamp,
                        'lat': current_lat + random.uniform(-0.00001, 0.00001),
                        'lon': current_lon + random.uniform(-0.00001, 0.00001),
                        'altitude': 50 + random.uniform(-2, 2),
                        'fix_quality': 1,
                        'num_sats': count,
                        'hdop': random.uniform(0.8, 2.0) if count > 4 else random.uniform(4.0, 8.0),
                        'speed': 0.0,
                        'heading': random.uniform(0, 360),
                        'satellite_id': sat_id,
                        'elevation': random.uniform(10, 80),
                        'azimuth': random.uniform(0, 360),
                        'snr': snr,
                    }

                    self.send_message(msg)

                time.sleep(delay)

        except KeyboardInterrupt:
            pass

        finally:
            self.close()
    
    

    # --------------------------------------------------------------------------
    # Close & Stats
    # --------------------------------------------------------------------------

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()

        duration = (datetime.now() - self.start_time).total_seconds()
        rate = self.messages_sent / duration if duration > 0 else 0

        logger.info("\n========== PRODUCER STATS ==========")
        logger.info(f"Messages sent: {self.messages_sent}")
        logger.info(f"Errors: {self.errors}")
        logger.info(f"Duration: {duration:.1f} sec")
        logger.info(f"Rate: {rate:.2f} msg/sec")
        logger.info("====================================\n")


# ------------------------------------------------------------------------------
#   Main Entry Point
# ------------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="GNSS Kafka Producer")

    parser.add_argument("--config", default="config/settings.yaml")

    parser.add_argument(
        "--source",
        choices=["tcp", "file", "test", "ntrip"],
        default="test",
        help="Source: tcp | file | test | ntrip"
    )

    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--file", help="Path to NMEA file")
    parser.add_argument("--delay", type=float, default=1.0)

    # ---- NTRIP arguments ----
    parser.add_argument("--caster", default=None)
    parser.add_argument("--caster-port", type=int, default=None)
    parser.add_argument("--mountpoint", default=None)
    parser.add_argument("--ntrip-user", default=None)
    parser.add_argument("--ntrip-pass", default=None)

    args = parser.parse_args()

    # Load default NTRIP from config if missing
    cfg = yaml.safe_load(open(args.config))

    caster = args.caster or cfg['ntrip']['caster']
    caster_port = args.caster_port or cfg['ntrip']['port']
    mountpoint = args.mountpoint or cfg['ntrip']['mountpoint']
    ntrip_user = args.ntrip_user or cfg['ntrip']['username']
    ntrip_pass = args.ntrip_pass or cfg['ntrip']['password']

    producer = GNSSKafkaProducer(config_path=args.config)

    producer.process_nmea_stream(
        source_type=args.source,
        host=args.host,
        port=args.port,
        filepath=args.file,
        delay=args.delay,
        caster=caster,
        caster_port=caster_port,
        mountpoint=mountpoint,
        user=ntrip_user,
        password=ntrip_pass
    )
    


if __name__ == "__main__":
    main()
