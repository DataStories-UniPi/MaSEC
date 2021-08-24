import urllib
import numpy as np
import pandas as pd
import geopandas as gpd
# import psycopg2
import sys, os
import time

import json
import sys, os

sys.path.append(os.path.join(os.path.expanduser('~'), 'Documents', 'st_visualizer'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))

import geom_helper as viz_helper 
from kafka_config_c_p_v01 import CFG_CSV_OUTPUT_DIR, CFG_PRODUCER_TIMESTAMP_NAME, CFG_DATASET_NAME, CFG_BUFFER_COLUMN_NAMES



# Define Global Variables
APP_ROOT = os.path.dirname(__file__)
DATETIME_OFFSET_SEC = 2*3600 if time.daylight else 3*3600



def get_points_polygon(clusters, st, et, timeslices):
	points = timeslices.loc[(timeslices.mmsi.isin(clusters)) & (timeslices.timestamp == et)]
	res = points.groupby(pd.to_datetime(points.timestamp, unit='s'), as_index=False, group_keys=False).apply(lambda l: l.geometry.unary_union.convex_hull)
	return res



def ec_convex_hulls(ecs, location_data):
	# print('LOCATION DATA ', location_data)
	ec_hulls = ecs.apply(lambda l: get_points_polygon(l.clusters, l.st, l.et, location_data), axis=1)
	# print('HULLS ', ec_hulls)
	return gpd.GeoSeries(pd.melt(ec_hulls).dropna().value, crs='epsg:4326').buffer(0.0005)



def fetch_latest_positions(consumer_ais, tag):
	try:
		message_ais = consumer_ais.poll(max_records=1)
		message_ais = next(iter(message_ais.values()))[0]
		consumer_ais.commit_async()

	except StopIteration as si:
		# print('POLL FAILED ', si)
		message_ais = None

	try:
		df_points_ais = pd.DataFrame(json.loads(message_ais.value.decode('utf-8')))
		df_points_ais = df_points_ais.assign(moving = tag)

	except Exception as e:
		# if it fails (for some reason) return an empty DataFrame
		df_points_ais = gpd.GeoDataFrame(data=[], columns=[*CFG_BUFFER_COLUMN_NAMES, 'moving', 'geometry'], geometry='geometry', crs='epsg:4326')
	
	return df_points_ais



def fetch_latest_evolving_clusters(consumer_ec, timeslices_filepath):
	try:
		message_ec = consumer_ec.poll(max_records=1)
		message_ec = next(iter(message_ec.values()))[0]
		consumer_ec.commit_async()
	except StopIteration as si:
		# print('POLL FAILED ', si)
		message_ec = None


	try:
		timeslices = pd.read_csv(timeslices_filepath)
		timeslices = viz_helper.getGeoDataFrame_v2(timeslices, crs='epsg:4326')
		timeslices.loc[:, 'timestamp'] = pd.to_datetime(timeslices[CFG_PRODUCER_TIMESTAMP_NAME], unit='s')

		mcs = pd.DataFrame(eval(json.loads(message_ec.value.decode('utf-8'))['mcs']['active']))
		mcs.loc[:, 'st'] = pd.to_datetime(mcs.st, unit='ms')
		mcs.loc[:, 'et'] = pd.to_datetime(mcs.et, unit='ms')

		# print('MCS ACTIVE ', mcs)
		mcs.loc[:, 'geometry'] = ec_convex_hulls(mcs, timeslices)
		mcs.loc[:, 'status'] = 'Active'

	except Exception as e:
		# if it fails (for some reason) return an empty DataFrame
		mcs = gpd.GeoDataFrame(data=[], columns=['clusters', 'st', 'et', 'geometry', 'status'], geometry='geometry', crs='epsg:4326')

	finally:
		result_active = mcs


	try:
		mcs_closed = pd.DataFrame(eval(json.loads(message_ec.value.decode('utf-8'))['mcs']['closed']))
		mcs_closed.loc[:, 'st'] = pd.to_datetime(mcs_closed.st, unit='ms')
		mcs_closed.loc[:, 'et'] = pd.to_datetime(mcs_closed.et, unit='ms')

		# print('MCS CLOSED ', mcs_closed)
		mcs_closed.loc[:, 'geometry'] = ec_convex_hulls(mcs_closed, timeslices)
		mcs_closed.loc[:, 'status'] = 'Closed'
		
	except Exception as e:
		# if it fails (for some reason) return an empty DataFrame
		mcs_closed = gpd.GeoDataFrame(data=[], columns=['clusters', 'st', 'et', 'geometry', 'status'], geometry='geometry', crs='epsg:4326')

	finally:
		result_closed = mcs_closed

	result = gpd.GeoDataFrame( pd.concat([result_active, result_closed], ignore_index=True), geometry='geometry', crs='epsg:4326')
	return result.sort_values('st', ascending=False).reset_index()



def get_data(consumer_ec, consumer_ac, consumer_op):
	timeslices_ec_filepath = os.path.join(CFG_CSV_OUTPUT_DIR, f'kafka_aligned_data_extra_EC_dataset_{CFG_DATASET_NAME}.csv')
	timeslices_ac_filepath = os.path.join(CFG_CSV_OUTPUT_DIR, f'kafka_aligned_data_extra_AC_dataset_{CFG_DATASET_NAME}.csv')


	# print('==================== EVOLVING CLUSTERS ====================')
	# df_evolving_clusters = fetch_latest_evolving_clusters(consumer_ec, timeslices_ec_filepath)
	df_evolving_clusters = fetch_latest_evolving_clusters(consumer_ec, timeslices_ec_filepath)
	print('\nEVOLVING CLUSTERS (MODE 1)\n', df_evolving_clusters)
	

	# print('==================== ANCHORAGES ====================')
	# df_anchs             = fetch_latest_evolving_clusters(consumer_ac, timeslices_ac_filepath)
	df_anchs             = fetch_latest_evolving_clusters(consumer_ac, timeslices_ac_filepath)
	print('\nEVOLVING CLUSTERS (MODE 2)\n', df_anchs)
	

	# print('==================== AIS POINTS ====================')
	df_points = fetch_latest_positions(consumer_op, 'B')
	df_points = df_points.assign(timestamp = pd.to_datetime(df_points[CFG_PRODUCER_TIMESTAMP_NAME], unit='s'))

	df_points.loc[df_points.speed > 1, 'moving']  = 'Y'
	df_points.loc[df_points.speed <= 1, 'moving'] = 'N'

	df_points = viz_helper.getGeoDataFrame_v2(df_points, crs='epsg:4326')
	print('\nBUFFER POINTS\n', df_points)


	## Meteorological Discipline
	# df_points.loc[:, 'TRCMP'] = - df_points.heading
	# df_points.loc[:, 'DSCMP'] = 270 - df_points.heading
	# df_points.sort_values('timestamp', ascending=False, inplace=True)
	return df_evolving_clusters.reset_index(drop=True), df_anchs.reset_index(drop=True), df_points.reset_index(drop=True)



def track_selected(df, cluster):
	try:
		return df.loc[df.clusters.isin(cluster)].index.values.tolist()
	except IndexError:
		return df



def toggle_renderers(viz):
	for renderer in viz.renderers: 
		renderer.visible = not renderer.visible
