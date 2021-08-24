from datetime import datetime, timezone
from time import sleep
from tqdm import tqdm

import sys, os
import collections

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

import pandas as pd
import numpy as np
import psycopg2
from kafka_config_c_p_v01 import *


def LoadingBar(time_sec, desc):
	for _ in tqdm(range(time_sec), desc=desc):
		sleep(1)
	return None


def StartServer():
	"""
		Start Server
	"""
	# Single SYS Call (V2)
	os.system("{0}/zookeeper-server-start.sh {1}/zookeeper.properties & {0}/kafka-server-start.sh {1}/server.properties".format(CFG_KAFKA_BIN_FOLDER, CFG_KAFKA_CFG_FOLDER))



def KafkaTopics(topic_name):
	def fun_delete_kafka_topic(name):
		"""
			Delete Previous Kafka Topic
		"""
		client = KafkaAdminClient(bootstrap_servers="localhost:9092")
		
		print('Deleting Previous Kafka Topic(s) ...')
		if name in client.list_topics():
			print('Topic {0} Already Exists... Deleting...'.format(name))
			client.delete_topics([name])  # Delete kafka topic
		
		print("List of Topics: {0}".format(client.list_topics()))  # See list of topics


	def fun_create_topic(name):
		"""
			Create Topic
		"""
		print('Create Kafka Topic {0}...'.format(name))
		client = KafkaAdminClient(bootstrap_servers="localhost:9092")
		topic_list = []
		
		print('Create Topic with {0} Partitions and replication_factor=1'.format(CFG_TOPIC_PARTITIONS))
		topic_list.append(NewTopic(name=name, num_partitions=CFG_TOPIC_PARTITIONS, replication_factor=1))
		client.create_topics(new_topics=topic_list, validate_only=False)
		
		print("List of Topics: {0}".format(client.list_topics()))  # See list of topics
		print("Topic {0} Description:".format(name))
		print(client.describe_topics([name]))


	LoadingBar(10, desc="Creating/Cleaning Kafka Topics...")
	fun_delete_kafka_topic(topic_name)
	fun_create_topic(topic_name)



def fetch_latest_positions():
	try:
		# Connect to Database and get the latest positions
		conn = psycopg2.connect(host=CFG_HOST_ADDRESS, port=CFG_HOST_PORT, 
								dbname=CFG_DB_NAME, user=CFG_DB_USERNAME, 
								password=CFG_DB_PASSWORD)
		result = pd.read_sql_query(CFG_DB_QUERY, conn)
		conn.close()

	except Exception:
		# if it fails (for some reason) return an empty DataFrame
		result = pd.DataFrame(data=[], columns=CFG_BUFFER_COLUMN_NAMES)
	
	# Just a Simulation for Demonstration/Testing --- REMOVE BEFORE DEPLOYING
	result.loc[:, CFG_PRODUCER_TIMESTAMP_NAME] = int(datetime.now(timezone.utc).strftime("%s")) * 10**3
	return result


def producer_send(producer, key, value, timestamp):
	return producer.send(CFG_TOPIC_NAME, key=key, value=value, timestamp_ms=int(timestamp*10**3)) # send each csv row to consumer


def KProducer():
	"""
		Start Producer
	"""

	LoadingBar(40, desc="Starting Kafka (Input) Producer ...")
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	
	# Pandas CSV File Iterator Definition
	while True:
		df = fetch_latest_positions()
		df = df.loc[:, CFG_BUFFER_COLUMN_NAMES].copy()

		# 
		df.loc[:, CFG_PRODUCER_TIMESTAMP_NAME] = df.loc[:, CFG_PRODUCER_TIMESTAMP_NAME].copy().apply(lambda x: pd.to_datetime(x, unit=CFG_PRODUCER_TIMESTAMP_UNIT).timestamp())
		df.sort_values(CFG_PRODUCER_TIMESTAMP_NAME, ascending=True, inplace=True)
		df.reset_index(drop=True, inplace=True)

		# df.loc[:, 'value'] = df.apply(lambda rec: rec.to_json(orient='records', lines=True).encode('utf-8'))
		df.loc[:, 'value'] = df.apply(lambda rec: rec.to_json(orient='columns').encode('utf-8'), axis=1)
		df.loc[:, 'key'] = df.loc[:, CFG_PRODUCER_KEY].copy().apply(lambda l: str(l).encode('utf-8'))

		for row in df.itertuples():
			producer_send(producer, row.key, row.value, getattr(row, CFG_PRODUCER_TIMESTAMP_NAME))
			# sleep(0.5)
			
		sleep(5)
		
	# print('\t\t\t---- Successfully Sent Data to Kafka Topic ----')
	