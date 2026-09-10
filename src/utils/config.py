import os
import yaml

DEFAULT_CONFIG_PATH = os.environ.get("CONFIG_PATH", "/opt/configs/config.yaml")
if not os.path.exists(DEFAULT_CONFIG_PATH):
    # Fallback to local path if running outside Docker
    local_path = os.path.join(os.path.dirname(__file__), "..", "..", "configs", "config.yaml")
    if os.path.exists(local_path):
        DEFAULT_CONFIG_PATH = os.path.abspath(local_path)

def load_config(config_path=None):
    path = config_path or DEFAULT_CONFIG_PATH
    if os.path.exists(path):
        with open(path, "r") as f:
            return yaml.safe_load(f)
    return {
        "hdfs": {
            "namenode_rpc": "hdfs://namenode:9000",
            "base_dir": "hdfs://namenode:9000/wildfire",
            "curated_dir": "hdfs://namenode:9000/wildfire/curated",
            "features_dir": "hdfs://namenode:9000/wildfire/features",
            "raw_sample_dir": "hdfs://namenode:9000/wildfire/raw/sample",
            "checkpoints_fire": "hdfs://namenode:9000/wildfire/checkpoints/firms_fire_events",
            "checkpoints_environmental": "hdfs://namenode:9000/wildfire/checkpoints/environmental_data"
        },
        "kafka": {
            "bootstrap_internal": "kafka:9092",
            "bootstrap_external": "localhost:9094",
            "topics": {
                "fire_events": "firms_fire_events",
                "environmental_data": "environmental_data",
                "anomaly_alerts": "anomaly_alerts",
                "risk_predictions": "risk_predictions"
            }
        },
        "storage": {
            "local_nasa_dir": "/data/nasa-wildfire-data",
            "reports_profile_dir": "/opt/reports/data_profile",
            "reports_quality_dir": "/opt/reports/data_quality"
        }
    }
