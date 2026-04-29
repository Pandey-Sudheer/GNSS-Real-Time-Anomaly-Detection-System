"""
Anomaly Analyzer Module
Provides expert-system based root cause analysis and mitigation strategies for GNSS anomalies.
"""

from typing import Dict, Any

class AnomalyAnalyzer:
    """Analyzes GNSS anomalies to provide reasons and tackling strategies."""

    @staticmethod
    def analyze(anomaly_type: str, row_data: Dict[str, Any] = None) -> Dict[str, str]:
        """
        Analyze a specific anomaly type and optionally its surrounding data.
        
        Args:
            anomaly_type: Type of anomaly (SIGNAL_DROP, SATELLITE_LOSS, POSITION_DRIFT, etc.)
            row_data: Optional dictionary containing additional metrics (snr, num_sats, etc.)
                      that can be used for more fine-grained analysis.
            
        Returns:
            A dictionary containing 'reason' and 'tackle' recommendations.
        """
        
        # Default fallback
        result = {
            "reason": "Unknown anomaly type detected.",
            "tackle": "Verify receiver status and monitor for persistent issues."
        }
        
        if anomaly_type == "SIGNAL_DROP":
            result["reason"] = (
                "**Likely Root Cause**: Multipath interference, atmospheric disturbances, or hardware degradation.\n"
                "The Signal-to-Noise Ratio (SNR) fell below the acceptable threshold. This usually occurs when the GNSS "
                "signal bounces off surrounding buildings or water bodies before reaching the receiver, degrading signal quality."
            )
            result["tackle"] = (
                "**Recommended Mitigations**:\n"
                "1. **Environment**: Ensure the antenna has a clear, unobstructed 360-degree view of the sky.\n"
                "2. **Hardware**: Inspect the antenna cable for water ingress or physical damage.\n"
                "3. **Upgrades**: Consider using a Choke-Ring Antenna to physically block multipath ground reflections."
            )
            
            # Use contextual data if available
            if row_data and row_data.get('snr') is not None:
                if float(row_data['snr']) < 10:
                    result["reason"] += " *Note: The SNR is severely low (<10 dB), indicating almost complete blockage or severe hardware failure.*"

        elif anomaly_type == "SATELLITE_LOSS":
            result["reason"] = (
                "**Likely Root Cause**: Physical obstruction or localized RF jamming.\n"
                "The number of visible satellites dropped below the minimum required to maintain a reliable fix. "
                "This typically happens when moving into an urban canyon, tunnel, heavy tree canopy, or due to intentional jamming."
            )
            result["tackle"] = (
                "**Recommended Mitigations**:\n"
                "1. **Relocation**: If stationary, move the receiver away from tall structures or overhanging trees.\n"
                "2. **Sensor Fusion**: For mobile platforms, integrate an Inertial Measurement Unit (IMU) to provide dead-reckoning during temporary outages.\n"
                "3. **Multi-Constellation**: Ensure your receiver is configured to track multiple constellations (e.g., GPS, Galileo, GLONASS, BeiDou) simultaneously to maximize satellite visibility."
            )

        elif anomaly_type == "POSITION_DRIFT":
            result["reason"] = (
                "**Likely Root Cause**: Loss of RTK corrections, severe multipath, or GNSS spoofing.\n"
                "The calculated position suddenly shifted by an impossible or unexpected amount. If you are relying on high-precision GNSS, "
                "this often means the correction stream (NTRIP/RTK) was temporarily lost, causing the receiver to fall back to a less accurate standard fix."
            )
            result["tackle"] = (
                "**Recommended Mitigations**:\n"
                "1. **Connectivity**: Check the internet connection to the NTRIP caster and verify your mountpoint credentials.\n"
                "2. **Anti-Spoofing**: Enable anti-spoofing algorithms on your receiver. Cross-reference GNSS velocity with vehicle wheel odometry.\n"
                "3. **Filtering**: Tune the Kalman filter on the receiver to reject sudden, physically impossible jumps in position."
            )

        return result
