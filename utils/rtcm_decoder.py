"""
RTCM MSM decoder tailored for AUSCORS + pyrtcm output.
Decodes:
- satellite_id (PRN)
- carrier-to-noise ratio from DF408_xx
"""

from pyrtcm import RTCMReader
from datetime import datetime
import io


class SocketStream(io.RawIOBase):
    def __init__(self, sock):
        self.sock = sock

    def read(self, n=1):
        try:
            return self.sock.recv(n)
        except Exception:
            return b""


class RTCMDecoder:
    def __init__(self, sock, latitude=None, longitude=None):
        self.stream = SocketStream(sock)
        self.reader = RTCMReader(self.stream)
        self.latitude = latitude
        self.longitude = longitude

    def _safe_int(self, value):
        """Convert to int safely."""
        try:
            return int(value)
        except:
            return None

    def _safe_float(self, value):
        """Convert to float safely."""
        try:
            return float(value)
        except:
            return None

    def _msgtype(self, parsed):
        if hasattr(parsed, "msgtype"):
            return parsed.msgtype
        if hasattr(parsed, "identity"):
            try:
                return int(parsed.identity)
            except:
                return None
        return None

    def read_next(self):
        messages = []

        try:
            raw, parsed = self.reader.read()
            if parsed is None:
                return messages

            msgtype = self._msgtype(parsed)
            if msgtype is None:
                return messages

            print("RTCM message received:", msgtype)

            # MSM messages
            if msgtype in (
                1071, 1072, 1073, 1074, 1075, 1076, 1077,
                1081, 1082, 1083, 1084, 1085, 1086, 1087,
                1091, 1092, 1093, 1094, 1095, 1096, 1097,
                1111, 1112, 1113, 1114, 1115, 1116, 1117,
                1121, 1122, 1123, 1124, 1125, 1126, 1127
            ):

                nsat = self._safe_int(getattr(parsed, "NSat", 0))
                ncell = self._safe_int(getattr(parsed, "NCell", 0))
                constellation = getattr(parsed, "identity", "MSM")

                if nsat is None or ncell is None:
                    return messages

                # Iterate over every cell
                for cell_idx in range(1, ncell + 1):

                    # For this pyrtcm MSM output, CELLPRN already contains the
                    # actual satellite PRN for the cell.
                    cellprn_attr = f"CELLPRN_{cell_idx:02d}"
                    prn = self._safe_int(getattr(parsed, cellprn_attr, None))
                    if prn is None:
                        continue

                    if prn < 1:
                        continue

                    # DF408 is the live MSM carrier-to-noise ratio (C/N0) in
                    # the AUSCORS stream, and maps cleanly to the dashboard SNR.
                    snr_attr = f"DF408_{cell_idx:02d}"
                    snr = self._safe_float(getattr(parsed, snr_attr, None))
                    if snr is None:
                        continue

                    messages.append({
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "lat": self.latitude,
                        "lon": self.longitude,
                        "altitude": None,
                        "fix_quality": 1,
                        "num_sats": nsat,
                        "hdop": None,
                        "speed": 0.0,
                        "heading": None,
                        "constellation": constellation,
                        "satellite_id": prn,
                        "elevation": None,
                        "azimuth": None,
                        "snr": snr,
                        "pseudorange": None,
                        "carrier_phase": None,
                        "doppler": None,
                        "lock_time": None,
                    })

                return messages

        except KeyboardInterrupt:
            raise
        except Exception as e:
            print("RTCM decode error:", e)

        return messages
