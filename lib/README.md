# Auxiliary Code

  * ```OPTICS_config.py```
    
    Contains parameters related to the OPTICS algorithm for (offline) Anchorage Discovery (AD).

    * ```CFG_THRESHOLD_LON_{MIN,MAX}```: Longtude range (for spatial outlier removal; default {-1000000000, 1000000000})
    * ```CFG_THRESHOLD_LAT_{MIN,MAX}```: Latitude range (for spatial outlier removal; default {-1000000000, 1000000000})

    * ```CSV_FILE_FOR_READ```: Path for the input CSV file.

    * ```COLUMN_NAMES```: Feature list of input file.

    * ```CFG_MIN_SAMPLES```: The number of samples in a neighborhood for a point to be considered as a core point, required by OPTICS (default value: 3 objects). 
   
    * ```CFG_MAX_EPS```: The maximum distance between two samples for one to be considered as in the neighborhood of the other, required by OPTICS (default value: 1500 meters).

    * ```CFG_THRESHOLD_SPEED_{MIN, MAX}```: Speed range for a stationary object (default: {0, 1}).

    * ```CFG_INIT_POINTS```: Minimum required number of data points for Anchorage Discovery.

    * ```CSV_FILE_FOR_READ_anchs```: Path for the ports' locations CSV file (for AD cluster filtering)

  * ```helper.py```

    Contains code for helper functions (e.g. temporal aligment, data output, etc.)

  * ```kafka_config_c_p_v01.py```

    Contains parameters related to the MaSEC framework.

    * ```CFG_KAFKA_FOLDER```: Path for the Kafka/Zookeeper binaries' location (default: kafka_2.13-2.8.0")
    
    * ```CFG_HOST_ADDRESS```: The IP address of the Host node.

    * ```CFG_HOST_PORT```: The Port of the Host node.
  
    * ```CFG_DB_NAME```: Database schema name

    * ```CFG_DB_USERNAME```: Database username
    
    * ```CFG_DB_PASSWORD```: Database password
    
    * ```CFG_DB_QUERY```: SQL query for accessing the streaming data 
    
    * ```CFG_PRODUCER_DTYPE```: Dataset special dtypes
   
    * ```CFG_PRODUCER_KEY```: The column name referring to the locations' object identifier (default: "mmsi")

    * ```CFG_PRODUCER_TIMESTAMP_NAME```: The column name referring to the locations' timestamp (default: "ts")
    
    * ```CFG_CONSUMER_SPEED_NAME```: The column name referring to the locations' recorded speed (default: "speed")

    * ```CFG_CONSUMER_COORDINATE_NAMES```: The column names referring to the locations' (ordered - by imporance) coordinates (default: ["lon", "lat"])

    * ```CFG_PRODUCER_TIMESTAMP_UNIT```: The unit of the temporal information (default: "ms")
    
    * ```CFG_BUFFER_COLUMN_NAMES```: Dataset columns to be maintained on the buffer.
    
    * ```CFG_DESIRED_ALIGNMENT_RATE_SEC```: Data-Point Alignment Interval (seconds)

    * ```CFG_THRESHOLD_MAX_SPEED```: Maximum Speed Threshold (Vessels -- m/s; default: 25.7222222)
    
    * ```CFG_EC_CARDINALITY_THRESHOLD```: Minimum object cardinality for an Evolving Cluster (default: 3 objects)
    
    * ```CFG_EC_TEMPORAL_THRESHOLD```: Minimum allowed duration for an Evolving Cluster (default: 2 minutes)
    
    * ```CFG_EC_DISTANCE_THRESHOLD```: Maximum allowed distance for an Evolving Cluster (default: 1500 meters)

    * ```CFG_SAVE_TO_FILE```: Output data to CSV file (default: True)
    
    * ```CFG_SAVE_TO_TOPIC```: Output data to Kafka Topic (default: False)
    

  * ```kafka_fun_aux.py```
    Functions related to Kafka/Zookeeper instantiation and Kafka Producer.

  * ```kafka_update_buffer_v03.py```
    Methods related to Objects' Buffer maintenance and Evolving Clusters discovery.

---

## Contributors
Andreas Tritsarolis, Yannis Kontoulis, Nikos Pelekis, Yannis Theodoridis; Data Science Lab., University of Piraeus
