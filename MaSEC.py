"""
kafka_multiprocess_v04.py
"""
import sys, os
import csv, json
import pandas as pd
import numpy as np
from time import sleep

from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata



# IMPORT SCRIPT HELPER FUNCTIONS & CONFIGURATION PARAMETERS
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))
from kafka_config_c_p_v01 import CFG_WRITE_FILE, CFG_TOPIC_NAME, CFG_EC_RESULTS_TOPIC_NAME, CFG_ALIGNMENT_RESULTS_TOPIC_NAME,\
								 CFG_TOPIC_PARTITIONS, CFG_NUM_CONSUMERS, CFG_CONSUMERS_EQUAL_TO_PARTITIONS, CFG_BUFFER_COLUMN_NAMES,\
								 CFG_DESIRED_ALIGNMENT_RATE_SEC, CFG_ALIGNMENT_MODE, CFG_SAVE_TO_TOPIC, CFG_PRODUCER_KEY,\
								 CFG_PRODUCER_TIMESTAMP_NAME, CFG_PRODUCER_TIMESTAMP_UNIT, CFG_CONSUMER_COORDINATE_NAMES,\
							     CFG_BUFFER_OTHER_FEATURES, CFG_CONSUMER_SPEED_NAME, CFG_AC_RESULTS_TOPIC_NAME,\
								 CFG_AC_ALIGNMENT_RESULTS_TOPIC_NAME, CFG_BUFFER_DATA_TOPIC_NAME

from helper import get_rounded_timestamp, get_aligned_location, adjust_buffers, data_output, ec_convex_hulls, send_to_kafka_topic
from kafka_update_buffer_v03 import update_buffer, discover_evolving_clusters
from kafka_fun_aux import LoadingBar, StartServer, KafkaTopics, KProducer

# PARALLELIZING MODULES
# Consider importing Ray (https://github.com/ray-project/ray) for Process Parallelization
import subprocess
import threading, logging, time
import multiprocessing



def init_log_output(consumer_num):
	if CFG_NUM_CONSUMERS == "None" or consumer_num == 0:
		if os.path.isfile(CFG_WRITE_FILE):
			print(CFG_WRITE_FILE, 'File Already Exists... Deleting...')
			os.remove(CFG_WRITE_FILE)


def init_KConsumer(consumer_num):
	if CFG_NUM_CONSUMERS == "None" or CFG_CONSUMERS_EQUAL_TO_PARTITIONS == 'no' or CFG_TOPIC_PARTITIONS != CFG_NUM_CONSUMERS:
		"""Consumer - Reads from all topics"""
		consumer = KafkaConsumer(CFG_TOPIC_NAME, bootstrap_servers='localhost:9092', group_id='MaSEC_Consumer',
                     			 auto_offset_reset='latest', enable_auto_commit=False, max_poll_interval_ms=10000)
	
	elif CFG_CONSUMERS_EQUAL_TO_PARTITIONS == 'yes' and CFG_TOPIC_PARTITIONS == CFG_NUM_CONSUMERS:
		"""Consumer k reads from the k partition - Assign each k consumer to the k partition """
		consumer = KafkaConsumer(bootstrap_servers='localhost:9092', group_id='MaSEC_Consumer',
                     			 auto_offset_reset='latest', enable_auto_commit=False, max_poll_interval_ms=10000)
		# consumer.assign([TopicPartition(topic=CFG_TOPIC_NAME, partition=consumer_num)])
		consumer.subscribe(topics=(CFG_TOPIC_NAME,))
	
	else:
		print('Check Configuration Parameters for #Consumers')

	return consumer


def init_KProducer():
	if CFG_SAVE_TO_TOPIC:
		savedata_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	else:
		savedata_producer = None

	return savedata_producer


def KConsumer(consumer_num, CFG_TOPIC_PARTITIONS):
	"""
		Start Consumer
	"""
	LoadingBar(15, desc="Starting Kafka Consumer ...")


	# ====================	INITIALIZING AUXILIARY FILES	====================
	init_log_output(consumer_num)

	# ====================	INSTANTIATE A KAFKA CONSUMER	====================
	consumer = init_KConsumer(consumer_num)

	# ============	INSTANTIATE A KAFKA PRODUCER (FOR DATA OUTPUT)	============
	savedata_producer = init_KProducer()


	# ========================================	NOW THE FUN BEGINS	========================================
	with open(CFG_WRITE_FILE, 'a') as fw2:
		fwriter = csv.writer(fw2)
		fwriter.writerow(['ts', 'message'])
		print('CSV File Writer Initialized...')


		# 0.	INITIALIZE THE BUFFERS
		object_pool = pd.DataFrame(columns=CFG_BUFFER_COLUMN_NAMES)	# create dataframe which keeps all the messages			
		pending_time = None

		stream_active_patterns, stream_closed_patterns = [pd.DataFrame(), pd.DataFrame()], [pd.DataFrame(), pd.DataFrame()]
		stream_active_anchs,    stream_closed_anchs    = [pd.DataFrame(), pd.DataFrame()], [pd.DataFrame(), pd.DataFrame()]


		# 1.	LISTEN TO DATASTREAM
		while True:
			message_batch = consumer.poll()

			# Commit Offsets (At-Most-Once Behaviour)
			# consumer.commit_async()		

			for topic_partition, partition_batch in message_batch.items():
				for message in partition_batch:
					# Print Incoming Message (and Test for Consistency)
					print('Incoming Message')
					print ("c{0}:t{1}:p{2}:o{3}: key={4} value={5}".format(consumer_num, message.topic, message.partition, message.offset, message.key, message.value))
					
					# Decode Message
					msg = json.loads(message.value.decode('utf-8'))
					fwriter.writerow([message.timestamp, msg])

					'''
						* Get the Current Datapoint's Timestamp
						* Get the Pending Timestamp (if not initialized)
					'''
					
					# Kafka Message Timestamp is in MilliSeconds 
					if pending_time is None:
						pending_time = get_rounded_timestamp(message.timestamp, base=CFG_DESIRED_ALIGNMENT_RATE_SEC, mode=CFG_ALIGNMENT_MODE, unit='ms') 

					print ("\nCurrent Timestamp: {0} ({1})\n".format(message.timestamp, pd.to_datetime(message.timestamp, unit='ms')))		
					print ('\nPending Timestamp (EC): {0} ({1})\n'.format(pending_time, pd.to_datetime(pending_time, unit='s')))

					'''
					If the time is right:
						* Discover evolving clusters up to ```curr_time```
						* Save (or Append) the timeslice to the ```kafka_aligned_data_*.csv``` file
						* Save the Discovered Evolving Clusters 
					'''
					if message.timestamp // 10**3 > pending_time:
						# Completing Missing Information
						# Fill NaN values with median for each object
						object_pool.loc[:, CFG_CONSUMER_SPEED_NAME] = object_pool[CFG_CONSUMER_SPEED_NAME].fillna(object_pool.groupby(CFG_PRODUCER_KEY)[CFG_CONSUMER_SPEED_NAME].transform('median'))
						
						# Create the Timeslice
						# Interpolate Points
						timeslice = object_pool.groupby(CFG_PRODUCER_KEY, group_keys=False).apply(lambda l: get_aligned_location(l, pending_time, temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, temporal_unit=CFG_PRODUCER_TIMESTAMP_UNIT, mode=CFG_ALIGNMENT_MODE))

						# Ctrate DataFrame Views
						moving_points = timeslice.loc[timeslice[CFG_CONSUMER_SPEED_NAME] > 1].copy()
						stationary_points = timeslice.loc[timeslice[CFG_CONSUMER_SPEED_NAME] <= 1].copy()

						# Discover Evolving Clusters
						stream_active_patterns, stream_closed_patterns = discover_evolving_clusters(moving_points, stream_active_patterns, stream_closed_patterns, 
																									coordinate_names=CFG_CONSUMER_COORDINATE_NAMES, temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, 
																									temporal_unit='s', o_id_name=CFG_PRODUCER_KEY, verbose=True)	
						# Discover Anchorages
						stream_active_anchs,    stream_closed_anchs    = discover_evolving_clusters(stationary_points, stream_active_anchs, stream_closed_anchs, 
																									coordinate_names=CFG_CONSUMER_COORDINATE_NAMES, temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, 
																									temporal_unit='s', o_id_name=CFG_PRODUCER_KEY, verbose=True)	
						
						# Data Output
						data_output(savedata_producer, pending_time, moving_points, stream_active_patterns, stream_closed_patterns, 
									alignment_res_topic_name=CFG_ALIGNMENT_RESULTS_TOPIC_NAME, cluster_res_topic_name=CFG_EC_RESULTS_TOPIC_NAME, tag='EC')
						
						data_output(savedata_producer, pending_time, stationary_points, stream_active_anchs, stream_closed_anchs, 
									alignment_res_topic_name=CFG_AC_ALIGNMENT_RESULTS_TOPIC_NAME, cluster_res_topic_name=CFG_AC_RESULTS_TOPIC_NAME, tag='AC')

						# Adjust Buffers and Pending Timestamp
						object_pool, pending_time, timeslice = adjust_buffers(pending_time, pending_time, object_pool.copy(), CFG_PRODUCER_TIMESTAMP_NAME)
						pending_time = get_rounded_timestamp(message.timestamp, base=CFG_DESIRED_ALIGNMENT_RATE_SEC, mode=CFG_ALIGNMENT_MODE, unit='ms') 
						
					'''
					In any case, Update the Objects' Buffer 
					'''			
					oid, ts, lon, lat = msg[CFG_PRODUCER_KEY], msg[CFG_PRODUCER_TIMESTAMP_NAME], msg[CFG_CONSUMER_COORDINATE_NAMES[0]], msg[CFG_CONSUMER_COORDINATE_NAMES[1]] # parameters for function update_buffer must be int/float
					object_pool = update_buffer(object_pool, oid, ts, lon, lat, **{k:msg[k] for k in CFG_BUFFER_OTHER_FEATURES})


				# Send Updated Buffer to **CFG_BUFFER_DATA_TOPIC_NAME** 
				send_to_kafka_topic(savedata_producer, CFG_BUFFER_DATA_TOPIC_NAME, pending_time*10**3, object_pool)


			# Commit Offsets (At-Least-Once Behaviour)
			consumer.commit_async()

			

def main():
	# StartServer() # start Zookeeper & Kafka
	# KafkaTopics() # Delete previous topic & Create new

	print('Start %d Consumers & 1 Producer with %d partitions' % (CFG_NUM_CONSUMERS, CFG_TOPIC_PARTITIONS))

	jobs = []

	job = multiprocessing.Process(target=StartServer) # 	Job #0: Start Kafka & Zookeeper
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_TOPIC_NAME,)) # 	Job #1: Delete previous kafka topic & Create new one (Simulating a DataStream via a CSV file)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_EC_RESULTS_TOPIC_NAME,)) # 	Job #2: Delete previous kafka topic & Create new one (EvolvingClusters Results Output Topic)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_AC_RESULTS_TOPIC_NAME,)) # 	Job #2: Delete previous kafka topic & Create new one (EvolvingClusters Results Output Topic)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_BUFFER_DATA_TOPIC_NAME,)) # 	Job #2: Delete previous kafka topic & Create new one (EvolvingClusters Results Output Topic)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_ALIGNMENT_RESULTS_TOPIC_NAME,)) # 	Job #3: Delete previous kafka topic & Create new one (Alignment Results Output Topic)
	jobs.append(job)

	for i in range(CFG_NUM_CONSUMERS): # Create different consumer jobs
		job = multiprocessing.Process(target=KConsumer, args=(i,CFG_TOPIC_PARTITIONS))
		jobs.append(job)

	job = multiprocessing.Process(target=KProducer) # 	Job #4: Start Producer
	jobs.append(job)


	for job in jobs: # 	Start the Threads
		job.start()

	for job in jobs: # 	Join the Threads
		job.join()

	print("Done!")


if __name__ == "__main__":
	logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
		level=logging.INFO
	)
	main()
