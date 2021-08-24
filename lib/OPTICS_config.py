"""
kafka_config.py
"""

import os
from kafka_config_c_p_v01 import CFG_CSV_OUTPUT_DIR

### This is the topic output to which FLPtool writes (with partition key 'mmsi')
CFG_TOPIC_NAME_OUT_OPTICS = 'optics_clusters'
CFG_TOPIC_NAME_OUT_ANCHS = 'anchs_clusters'
CFG_TOPIC_NAME_OUT_ANCHS_INTERSECT = 'anchs_intersect'

### OPTICS PARAMETERS
CFG_MIN_SAMPLES = 3
CFG_MAX_EPS = 1500

# speed in knots
CFG_THRESHOLD_SPEED_MAX = 1
CFG_THRESHOLD_SPEED_MIN = 0

### Number of initial records for model Prediction
CFG_INIT_POINTS = 10000
CSV_FILE_FOR_READ_anchs = os.path.join(CFG_CSV_OUTPUT_DIR, 'ports_hcmr_buffer.csv')
