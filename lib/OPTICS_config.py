"""
kafka_config.py
"""

# =================================================================================
# ============================== USEFUL PARAMETERS ================================
# =================================================================================

### OPTICS PARAMETERS
MIN_SAMPLES = 3
MAX_EPS = 1500

CFG_THRESHOLD_LON_MAX = 1000000000
CFG_THRESHOLD_LON_MIN = -1000000000
CFG_THRESHOLD_LAT_MAX = 1000000000
CFG_THRESHOLD_LAT_MIN = -1000000000

# speed in knots
CFG_THRESHOLD_SPEED_MAX = 1
CFG_THRESHOLD_SPEED_MIN = 0

### Number of initial records for model Prediction
CFG_INIT_POINTS = 10000

### Features needed
COLUMN_NAMES = ['timestamp', 'mmsi' ,'lon', 'lat', 'heading', 'speed', 'course']

CSV_FILE_FOR_READ = "./data/unipi_ais_dynamic_brest_3months_ws_v4.csv"
CSV_FILE_FOR_READ_anchs = "../../DataStories-UniPi/EvolvingClusters_PackageToCommit/data/csv/ports_hcmr_buffer.csv"

