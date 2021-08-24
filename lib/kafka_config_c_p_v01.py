"""
cfg.py
--- SET PARAMETERS FOR KAFKA SERVER & CONSUMER & PRODUCER ---
"""
from numpy import array
import os, sys

### Folder where Kafka & ZooKeeper exist
CFG_BASEPATH = os.path.dirname(__file__)
CFG_CSV_OUTPUT_DIR = os.path.join(CFG_BASEPATH, '..', 'data', 'csv')

# CFG_KAFKA_FOLDER = os.path.join(CFG_BASEPATH, 'kafka_2.12-2.5.0')
CFG_KAFKA_FOLDER = os.path.join(CFG_BASEPATH, 'kafka_2.13-2.8.0')
CFG_KAFKA_BIN_FOLDER = os.path.join(CFG_KAFKA_FOLDER, 'bin')
CFG_KAFKA_CFG_FOLDER = os.path.join(CFG_KAFKA_FOLDER, 'config')


### Database Credentials and Query for Kafka Producer to fetch Data
CFG_HOST_ADDRESS = "HOST IP ADDRESS"
CFG_HOST_PORT    = "DATABASE PORT"
CFG_DB_NAME      = "DATABASE SCHEMA NAME"
CFG_DB_USERNAME  = "DATABASE USERNAME"
CFG_DB_PASSWORD  = "DATABASE PASSWORD"
CFG_DB_QUERY     = "DATABASE QUERY"


### Kafka Topic Suffix
CFG_DATASET_NAME = 'unipi_ais_antenna'
# --------------------------------------------------------------------------------------------------------------------------------------------------------



#################################################################################
############################## PRODUCER PARAMETERS ##############################
#################################################################################

### Dataset special dtypes
# -----------------------------------------------------------------------------------
CFG_PRODUCER_DTYPE = {'mmsi':int, 'ts':int}               # UniPi AIS Antenna Dataset
# -----------------------------------------------------------------------------------


### Dataset Essential Features
# -----------------------------------------------------------------------------------
CFG_PRODUCER_KEY = 'mmsi'                                 # UniPi AIS Antenna Dataset
CFG_PRODUCER_TIMESTAMP_NAME = 'ts'                        # UniPi AIS Antenna Dataset
CFG_CONSUMER_SPEED_NAME = 'speed'                         # UniPi AIS Antenna Dataset
CFG_CONSUMER_COORDINATE_NAMES = ['lon', 'lat']            # UniPi AIS Antenna Dataset
# -----------------------------------------------------------------------------------


### Dataset Temporal Unit
# -------------------------------------------------------------
CFG_PRODUCER_TIMESTAMP_UNIT = 'ms'      # UniPi Antenna Dataset
# -------------------------------------------------------------


### Kafka Output Topic(s) Suffix (useful for multiple concurrent instances -- can be ```None```)
# ---------------------------------------------------------------
CFG_TOPIC_SUFFIX = '_unipi_antenna'       # UniPi Antenna Dataset
# ---------------------------------------------------------------

### Topic Name which Kafka Producer writes & Kafka Consumer reads
CFG_TOPIC_NAME = 'datacsv{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Data Throughput Factor (i.e. Fast-Forward)
CFG_TOPIC_FF = 512



#################################################################################
############################## CONSUMER PARAMETERS ##############################
#################################################################################

### File where Kafka Consumer writes
# -------------------------------------------------------------------------------------------------------------
CFG_WRITE_FILE = os.path.join(CFG_CSV_OUTPUT_DIR, 'MessagesKafka{0}.csv'.format(CFG_TOPIC_SUFFIX))

### Topic Name which Kafka Consumer writes the Evolving Clusters at each Temporal Instance
CFG_EC_RESULTS_TOPIC_NAME = 'ecdresults{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Anchorages at each Temporal Instance
CFG_AC_RESULTS_TOPIC_NAME = 'acresults{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Moving Objects' Timeslice
CFG_ALIGNMENT_RESULTS_TOPIC_NAME = 'alignedata_ec{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Creeping Objects' Timeslice
CFG_AC_ALIGNMENT_RESULTS_TOPIC_NAME = 'alignedata_ac{0}'.format(CFG_TOPIC_SUFFIX)

### Topic Name which Kafka Consumer writes the Objects' Pool (buffer)
CFG_BUFFER_DATA_TOPIC_NAME = 'buffer{0}'.format(CFG_TOPIC_SUFFIX)


### Topic num of partitions (must be an integer)
CFG_TOPIC_PARTITIONS = 1

### Num of consumers (must be an integer)
CFG_NUM_CONSUMERS = 1

### Num of consumers equal to Num of partitions
CFG_CONSUMERS_EQUAL_TO_PARTITIONS = 'yes' if CFG_TOPIC_PARTITIONS == CFG_NUM_CONSUMERS else 'no' #'yes' or 'no'
# -------------------------------------------------------------------------------------------------------------


### Buffer/Timeslice Settings
# --------------------------------------------------------------------------------------------------------------------------------------------------------
CFG_BUFFER_COLUMN_NAMES = ['mmsi', 'ts', 'lon', 'lat', 'speed', 'heading']         # UniPi AIS Antenna Dataset
# --------------------------------------------------------------------------------------------------------------------------------------------------------



### Dataset Non-essential Features
CFG_BUFFER_OTHER_FEATURES = sorted(list(set(CFG_BUFFER_COLUMN_NAMES) - (set([CFG_PRODUCER_KEY]) | set([CFG_CONSUMER_SPEED_NAME]) | set([CFG_PRODUCER_TIMESTAMP_NAME]) | set(CFG_CONSUMER_COORDINATE_NAMES))))

### Number of necessary records for temporal alignment
CFG_INIT_POINTS = 2

### Data-Point Alignment Interval (seconds)
# -------------------------------------------------------------------------------------
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 30     # 30 seconds = 0.5 minutes
CFG_DESIRED_ALIGNMENT_RATE_SEC = 60     # 1 minute
# CFG_DESIRED_ALIGNMENT_RATE_SEC = 120    # 2 minutes
# -------------------------------------------------------------------------------------


### Maximum Speed Threshold (Vessels -- m/s)
# -------------------------------------------------------------------------------------------------------------
CFG_THRESHOLD_MAX_SPEED = 25.7222222  # 50 knots = 92.6 km/h = 25.72 meters / second --- BREST/SARONIKOS
# -------------------------------------------------------------------------------------------------------------


### EVOLVING CLUSTERS PARAMETERS
# -------------------------------------------------------
CFG_ALIGNMENT_MODE = 'extra'        # {'inter', 'extra'}
# -------------------------------------------------------
# Brest/Saronikos Default Parameters
# -------------------------------------------------------
CFG_EC_CARDINALITY_THRESHOLD = 3    # number of objects
CFG_EC_TEMPORAL_THRESHOLD = 2       # in minutes
CFG_EC_DISTANCE_THRESHOLD = 1500    # in meters
# -------------------------------------------------------


### DATA OUTPUT PARAMETERS
# ------------------------------------------------------------------------
CFG_SAVE_TO_FILE  = True           # **True**: Output Data to CSV File
CFG_SAVE_TO_TOPIC = True           # **True**: Output Data to Kafka Topic
# ------------------------------------------------------------------------
