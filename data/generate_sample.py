"""
Generate sample NMEA data for testing
"""

import random
from datetime import datetime, timedelta
import math

def generate_nmea_gga(lat, lon, time, num_sats=10, hdop=1.0, alt=50.0):
    """Generate NMEA GGA sentence"""
    
    # Convert decimal to NMEA format (DDMM.MMMM)
    lat_deg = int(abs(lat))
    lat_min = (abs(lat) - lat_deg) * 60
    lat_nmea = f"{lat_deg:02d}{lat_min:07.4f}"
    lat_dir = 'N' if lat >= 0 else 'S'
    
    lon_deg = int(abs(lon))
    lon_min = (abs(lon) - lon_deg) * 60
    lon_nmea = f"{lon_deg:03d}{lon_min:07.4f}"
    lon_dir = 'E' if lon >= 0 else 'W'
    
    time_str = time.strftime("%H%M%S.000")
    
    sentence = (f"$GPGGA,{time_str},{lat_nmea},{lat_dir},{lon_nmea},{lon_dir},"
               f"1,{num_sats:02d},{hdop:.1f},{alt:.1f},M,0.0,M,,")
    
    # Calculate checksum
    checksum = 0
    for char in sentence[1:]:
        checksum ^= ord(char)
    
    return f"{sentence}*{checksum:02X}"


def generate_nmea_gsv(satellites, msg_num, total_msgs):
    """Generate NMEA GSV sentence"""
    
    start_idx = (msg_num - 1) * 4
    end_idx = min(start_idx + 4, len(satellites))
    sats_in_msg = satellites[start_idx:end_idx]
    
    sentence = f"$GPGSV,{total_msgs},{msg_num},{len(satellites):02d}"
    
    for sat in sats_in_msg:
        sentence += f",{sat['prn']:02d},{sat['elev']:02d},{sat['azim']:03d},{sat['snr']:02d}"
    
    # Pad if less than 4 satellites
    while len(sats_in_msg) < 4:
        sentence += ",,,,"
        sats_in_msg.append({})
    
    # Calculate checksum
    checksum = 0
    for char in sentence[1:]:
        checksum ^= ord(char)
    
    return f"{sentence}*{checksum:02X}"


def generate_nmea_rmc(lat, lon, time, speed=0.0, heading=0.0):
    """Generate NMEA RMC sentence"""
    
    lat_deg = int(abs(lat))
    lat_min = (abs(lat) - lat_deg) * 60
    lat_nmea = f"{lat_deg:02d}{lat_min:07.4f}"
    lat_dir = 'N' if lat >= 0 else 'S'
    
    lon_deg = int(abs(lon))
    lon_min = (abs(lon) - lon_deg) * 60
    lon_nmea = f"{lon_deg:03d}{lon_min:07.4f}"
    lon_dir = 'E' if lon >= 0 else 'W'
    
    time_str = time.strftime("%H%M%S.000")
    date_str = time.strftime("%d%m%y")
    
    # Speed in knots
    speed_kts = speed / 0.514444
    
    sentence = (f"$GPRMC,{time_str},A,{lat_nmea},{lat_dir},{lon_nmea},{lon_dir},"
               f"{speed_kts:.2f},{heading:.1f},{date_str},,,A")
    
    # Calculate checksum
    checksum = 0
    for char in sentence[1:]:
        checksum ^= ord(char)
    
    return f"{sentence}*{checksum:02X}"


def main():
    """Generate sample NMEA file"""
    
    # Starting position (Sydney, Australia)
    current_lat = -33.865143
    current_lon = 151.209900
    
    # Generate 1000 position updates (about 30 minutes at 2 sec intervals)
    start_time = datetime.utcnow()
    
    output_file = "sample_nmea.txt"
    
    with open(output_file, 'w') as f:
        for i in range(1000):
            current_time = start_time + timedelta(seconds=i*2)
            
            # Add true movement (random walk in a general direction)
            current_lat += random.uniform(0.0001, 0.0003)
            current_lon += random.uniform(0.0001, 0.0003)
            
            lat = current_lat + random.uniform(-0.00005, 0.00005)
            lon = current_lon + random.uniform(-0.00005, 0.00005)
            
            # Random number of satellites (8-14)
            num_sats = random.randint(8, 14)
            
            # Generate satellites
            satellites = []
            for j in range(num_sats):
                # Occasionally create low SNR satellite
                if random.random() < 0.1:
                    snr = random.randint(10, 20)
                else:
                    snr = random.randint(30, 50)
                
                satellites.append({
                    'prn': j + 1,
                    'elev': random.randint(10, 80),
                    'azim': random.randint(0, 359),
                    'snr': snr
                })
            
            # Write GGA
            f.write(generate_nmea_gga(lat, lon, current_time, num_sats) + '\n')
            
            # Write GSV (multiple messages for all satellites)
            total_gsv = math.ceil(num_sats / 4)
            for msg_num in range(1, total_gsv + 1):
                f.write(generate_nmea_gsv(satellites, msg_num, total_gsv) + '\n')
            
            # Write RMC
            f.write(generate_nmea_rmc(lat, lon, current_time) + '\n')
    
    print(f"✓ Generated {output_file} with 1000 position updates")


if __name__ == "__main__":
    main()