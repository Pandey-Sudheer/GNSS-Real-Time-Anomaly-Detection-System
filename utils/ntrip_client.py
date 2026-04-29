"""
NTRIP Client for AUSCORS Streams
Connects to NTRIP caster and receives RTCM correction data
"""

import socket
import base64
import logging
from typing import Optional, Callable
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NTRIPClient:
    """
    NTRIP (Networked Transport of RTCM via Internet Protocol) Client
    Connects to GNSS correction services like AUSCORS
    """
    
    def __init__(self, 
                 caster: str,
                 port: int,
                 mountpoint: str,
                 username: str,
                 password: str,
                 latitude: float = 0.0,
                 longitude: float = 0.0):
        """
        Initialize NTRIP client
        
        Args:
            caster: NTRIP caster hostname (e.g., gnss.ga.gov.au)
            port: Port number (usually 2101)
            mountpoint: Mountpoint name (e.g., MOBS00AUS0)
            username: NTRIP username
            password: NTRIP password
            latitude: Receiver latitude (for approximate position)
            longitude: Receiver longitude
        """
        self.caster = caster
        self.port = port
        self.mountpoint = mountpoint
        self.username = username
        self.password = password
        self.latitude = latitude
        self.longitude = longitude
        
        self.socket: Optional[socket.socket] = None
        self.connected = False
        
    def connect(self) -> bool:
        """
        Connect to NTRIP caster
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(f"Connecting to {self.caster}:{self.port}/{self.mountpoint}")
            
            # Create socket
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10)
            
            # Connect to caster
            self.socket.connect((self.caster, self.port))
            
            # Build HTTP request
            auth_string = f"{self.username}:{self.password}"
            auth_bytes = auth_string.encode('ascii')
            auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
            
            # NTRIP v1.0 request format
            request = (
                f"GET /{self.mountpoint} HTTP/1.0\r\n"
                f"User-Agent: NTRIP PythonClient/1.0\r\n"
                f"Authorization: Basic {auth_b64}\r\n"
                f"Accept: */*\r\n"
                f"Connection: close\r\n"
                "\r\n"
            )
            
            # Send request
            self.socket.send(request.encode('utf-8'))
            
            # Read response
            response = b""
            while b"\r\n\r\n" not in response:
                chunk = self.socket.recv(1024)
                if not chunk:
                    break
                response += chunk
            
            # Parse response
            response_str = response.decode('utf-8', errors='ignore')
            
            if "200 OK" in response_str or "ICY 200 OK" in response_str:
                logger.info("✓ Connected to NTRIP caster successfully")
                self.connected = True
                return True
            else:
                logger.error(f"Connection failed: {response_str}")
                return False
                
        except socket.timeout:
            logger.error("Connection timeout")
            return False
        except socket.error as e:
            logger.error(f"Socket error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
    
    def read_data(self, callback: Callable[[bytes], None], timeout: Optional[float] = None):
        """
        Read RTCM data stream and call callback with each chunk
        
        Args:
            callback: Function to call with each data chunk
            timeout: Optional timeout for read operations
        """
        if not self.connected or not self.socket:
            logger.error("Not connected to NTRIP caster")
            return
        
        try:
            if timeout:
                self.socket.settimeout(timeout)
            else:
                self.socket.settimeout(None)
            
            logger.info("Starting to read RTCM data stream...")
            
            while self.connected:
                try:
                    # Read data chunk
                    data = self.socket.recv(4096)
                    
                    if not data:
                        logger.warning("No data received, connection closed")
                        break
                    
                    # Call callback with data
                    callback(data)
                    
                except socket.timeout:
                    logger.debug("Read timeout, continuing...")
                    continue
                except Exception as e:
                    logger.error(f"Error reading data: {e}")
                    break
                    
        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
        finally:
            self.disconnect()
    
    def disconnect(self):
        """Close connection to NTRIP caster"""
        if self.socket:
            try:
                self.socket.close()
                logger.info("Disconnected from NTRIP caster")
            except:
                pass
        self.connected = False
        self.socket = None
    
    def send_gga(self, gga_message: str):
        """
        Send GGA message to caster (required by some mountpoints)
        
        Args:
            gga_message: NMEA GGA sentence
        """
        if self.connected and self.socket:
            try:
                self.socket.send((gga_message + "\r\n").encode('utf-8'))
            except Exception as e:
                logger.error(f"Failed to send GGA: {e}")
    
    def get_sourcetable(self) -> Optional[str]:
        """
        Request sourcetable from NTRIP caster
        
        Returns:
            Sourcetable as string or None
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.caster, self.port))
            
            auth_string = f"{self.username}:{self.password}"
            auth_b64 = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
            
            request = (
                f"GET / HTTP/1.0\r\n"
                f"User-Agent: NTRIP PythonClient/1.0\r\n"
                f"Authorization: Basic {auth_b64}\r\n"
                "\r\n"
            )
            
            sock.send(request.encode('utf-8'))
            
            response = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
            
            sock.close()
            
            return response.decode('utf-8', errors='ignore')
            
        except Exception as e:
            logger.error(f"Failed to get sourcetable: {e}")
            return None


# Example usage
if __name__ == "__main__":
    import sys
    
    # Example configuration (replace with your credentials)
    config = {
        'caster': 'gnss.ga.gov.au',
        'port': 2101,
        'mountpoint': 'MOBS00AUS0',
        'username': 'your_username',
        'password': 'your_password',
        'latitude': -33.865,
        'longitude': 151.209
    }
    
    # Create client
    client = NTRIPClient(**config)
    
    # Test connection
    if client.connect():
        print("✓ Connection successful!")
        
        # Define callback to handle data
        def handle_rtcm_data(data: bytes):
            print(f"Received {len(data)} bytes of RTCM data")
            # Here you would decode RTCM and convert to NMEA
            # For now, just print data length
        
        # Read data for 10 seconds
        try:
            start_time = time.time()
            while time.time() - start_time < 10:
                chunk = client.socket.recv(4096)
                if chunk:
                    handle_rtcm_data(chunk)
        except KeyboardInterrupt:
            pass
        finally:
            client.disconnect()
    else:
        print("✗ Connection failed")
        sys.exit(1)
