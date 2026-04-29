"""
NMEA Parser for GNSS Data
Parses GGA, GSV, GSA, and RMC messages into structured JSON
"""

import pynmea2
import json
from datetime import datetime
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NMEAParser:
    """Parser for NMEA 0183 messages"""
    
    def __init__(self):
        self.current_data = {
            'timestamp': None,
            'lat': None,
            'lon': None,
            'altitude': None,
            'fix_quality': None,
            'num_sats': None,
            'hdop': None,
            'speed': None,
            'heading': None,
            'satellites': []
        }
        
    def parse_message(self, nmea_string: str) -> Optional[Dict]:
        """
        Parse a single NMEA message and update current data
        
        Args:
            nmea_string: Raw NMEA sentence
            
        Returns:
            Updated data dictionary or None if parsing fails
        """
        try:
            # Remove any whitespace
            nmea_string = nmea_string.strip()
            
            # Parse the NMEA message
            msg = pynmea2.parse(nmea_string)
            
            # Handle different message types
            if isinstance(msg, pynmea2.types.talker.GGA):
                self._parse_gga(msg)
            elif isinstance(msg, pynmea2.types.talker.GSV):
                self._parse_gsv(msg)
            elif isinstance(msg, pynmea2.types.talker.GSA):
                self._parse_gsa(msg)
            elif isinstance(msg, pynmea2.types.talker.RMC):
                self._parse_rmc(msg)
            
            return self.get_current_data()
            
        except pynmea2.ParseError as e:
            logger.warning(f"Failed to parse NMEA: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing NMEA: {e}")
            return None
    
    def _parse_gga(self, msg: pynmea2.types.talker.GGA):
        """Parse GGA message - Position fix data"""
        if msg.timestamp:
            # Combine with current date
            now = datetime.utcnow()
            self.current_data['timestamp'] = datetime.combine(
                now.date(), 
                msg.timestamp
            ).isoformat() + 'Z'
        
        if msg.latitude and msg.longitude:
            self.current_data['lat'] = msg.latitude
            self.current_data['lon'] = msg.longitude
        
        if msg.altitude:
            self.current_data['altitude'] = float(msg.altitude)
        
        if msg.gps_qual:
            self.current_data['fix_quality'] = int(msg.gps_qual)
        
        if msg.num_sats:
            self.current_data['num_sats'] = int(msg.num_sats)
        
        if msg.horizontal_dil:
            self.current_data['hdop'] = float(msg.horizontal_dil)
    
    def _parse_gsv(self, msg: pynmea2.types.talker.GSV):
        """Parse GSV message - Satellites in view"""
        # GSV messages contain SNR data for satellites
        # Multiple GSV messages make up complete satellite list
        
        # Clear satellites list on first message
        if msg.msg_num == '1':
            self.current_data['satellites'] = []
        
        # Extract satellite data from this message
        for i in range(1, 5):  # Up to 4 satellites per GSV message
            try:
                sat_prn = getattr(msg, f'sv_prn_num_{i}', None)
                elevation = getattr(msg, f'elevation_deg_{i}', None)
                azimuth = getattr(msg, f'azimuth_{i}', None)
                snr = getattr(msg, f'snr_{i}', None)
                
                if sat_prn:
                    sat_data = {
                        'satellite_id': sat_prn,
                        'elevation': float(elevation) if elevation else None,
                        'azimuth': float(azimuth) if azimuth else None,
                        'snr': float(snr) if snr else 0.0
                    }
                    self.current_data['satellites'].append(sat_data)
            except (AttributeError, ValueError):
                continue
    
    def _parse_gsa(self, msg: pynmea2.types.talker.GSA):
        """Parse GSA message - DOP and active satellites"""
        # GSA provides PDOP, HDOP, VDOP
        if msg.hdop:
            self.current_data['hdop'] = float(msg.hdop)
    
    def _parse_rmc(self, msg: pynmea2.types.talker.RMC):
        """Parse RMC message - Recommended minimum"""
        if msg.timestamp and msg.datestamp:
            # RMC has both date and time
            dt = datetime.combine(msg.datestamp, msg.timestamp)
            self.current_data['timestamp'] = dt.isoformat() + 'Z'
        
        if msg.spd_over_grnd:
            # Speed in knots, convert to m/s
            self.current_data['speed'] = float(msg.spd_over_grnd) * 0.514444
        
        if msg.true_course:
            self.current_data['heading'] = float(msg.true_course)
    
    def get_current_data(self) -> Dict:
        """Get current accumulated data"""
        return self.current_data.copy()
    
    def to_json_messages(self) -> List[Dict]:
        """
        Convert current data into JSON messages per satellite
        
        Returns:
            List of JSON objects, one per satellite with SNR data
        """
        messages = []
        
        base_data = {
            'timestamp': self.current_data['timestamp'] or datetime.utcnow().isoformat() + 'Z',
            'lat': self.current_data['lat'],
            'lon': self.current_data['lon'],
            'altitude': self.current_data['altitude'],
            'fix_quality': self.current_data['fix_quality'],
            'num_sats': self.current_data['num_sats'],
            'hdop': self.current_data['hdop'],
            'speed': self.current_data['speed'],
            'heading': self.current_data['heading']
        }
        
        # Create one message per satellite with SNR data
        if self.current_data['satellites']:
            for sat in self.current_data['satellites']:
                msg = base_data.copy()
                msg.update({
                    'satellite_id': sat['satellite_id'],
                    'elevation': sat['elevation'],
                    'azimuth': sat['azimuth'],
                    'snr': sat['snr']
                })
                messages.append(msg)
        else:
            # If no satellite data, send one message with base data
            messages.append(base_data)
        
        return messages
    
    def reset(self):
        """Reset parser state"""
        self.__init__()


def parse_nmea_file(filepath: str) -> List[Dict]:
    """
    Parse NMEA file and return list of JSON messages
    
    Args:
        filepath: Path to NMEA file
        
    Returns:
        List of parsed JSON messages
    """
    parser = NMEAParser()
    all_messages = []
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('$'):
                parser.parse_message(line)
                
                # After parsing GGA (position update), generate messages
                if 'GGA' in line:
                    messages = parser.to_json_messages()
                    all_messages.extend(messages)
    
    return all_messages


# Example usage and testing
if __name__ == "__main__":
    # Test with sample NMEA messages
    sample_messages = [
        "$GPGGA,092750.000,5321.6802,N,00630.3372,W,1,8,1.03,61.7,M,55.2,M,,*76",
        "$GPGSV,3,1,11,03,03,111,00,04,15,270,00,06,01,010,00,13,06,292,00*74",
        "$GPGSV,3,2,11,14,25,170,00,16,57,208,39,18,67,296,40,19,40,246,00*74",
        "$GPGSV,3,3,11,22,42,067,42,24,14,311,43,27,05,244,00,,,,*4D",
        "$GPRMC,092750.000,A,5321.6802,N,00630.3372,W,0.02,31.66,280511,,,A*43"
    ]
    
    parser = NMEAParser()
    
    for msg in sample_messages:
        print(f"\nParsing: {msg}")
        result = parser.parse_message(msg)
    
    # Generate JSON messages
    json_messages = parser.to_json_messages()
    print(f"\n\nGenerated {len(json_messages)} JSON messages:")
    for msg in json_messages:
        print(json.dumps(msg, indent=2))
