# RTKLIB Setup Guide for macOS M2 Pro

## Option 1: Using STRSVR (GUI Application)

### Download RTKLIB
Since RTKLIB doesn't have official M2 builds, use one of these options:

1. **RTKLib Demo5 (Windows via Wine)**
   - Install Wine: `brew install wine-stable`
   - Download RTKLIB Demo5
   - Run: `wine rtklaunch.exe`

2. **Build from Source**
```bash
   git clone https://github.com/tomojitakasu/RTKLIB.git
   cd RTKLIB/app/consapp/strsvr/gcc
   make
```

### Configure STRSVR

1. **Input Stream (NTRIP Client)**
   - Type: `NTRIP Client`
   - Address: `gnss.ga.gov.au:2101`
   - Mountpoint: `MOBS00AUS0` (or your choice)
   - User: Your AUSCORS username
   - Password: Your AUSCORS password

2. **Output Stream**
   - Type: `TCP Server`
   - Port: `9000`
   - Or: `File` → Save to `gnss_data.rtcm`

### Configure RTKNAVI

1. **Input Stream**
   - Rover: `NTRIP Client` (same as above)
   - Base Station: Not needed for single-point positioning

2. **Output**
   - Solution 1: `TCP Server` Port `9000`
   - Format: `NMEA`

## Option 2: Python NTRIP Client (Recommended for M2)

Use the provided `utils/ntrip_client.py`:
```python
from utils.ntrip_client import NTRIPClient

client = NTRIPClient(
    caster='gnss.ga.gov.au',
    port=2101,
    mountpoint='MOBS00AUS0',
    username='your_username',
    password='your_password'
)

if client.connect():
    # Process RTCM data
    def handle_data(data):
        # Decode RTCM and convert to NMEA
        pass
    
    client.read_data(handle_data)
```

## Option 3: Using BKG NtripClient
```bash
# Download BKG NtripClient
wget http://igs.bkg.bund.de/ntrip/download/ntripclient_1.5.0.tar.gz
tar -xzf ntripclient_1.5.0.tar.gz
cd ntripclient

# Build
make

# Run
./ntripclient -s gnss.ga.gov.au -r 2101 -m MOBS00AUS0 \
  -u your_username -p your_password -n
```

## Verifying Connection

Test AUSCORS connection:
```bash
# Using curl
curl -v -u username:password http://gnss.ga.gov.au:2101/MOBS00AUS0

# Should return RTCM data stream
```

## Available AUSCORS Mount Points

Common mount points:
- `MOBS00AUS0` - Mount Stromlo, ACT
- `SYDN00AUS0` - Sydney, NSW
- `ALIC00AUS0` - Alice Springs, NT
- `DARW00AUS0` - Darwin, NT

Check full list: http://gnss.ga.gov.au/auscors