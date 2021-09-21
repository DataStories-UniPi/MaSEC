"""
OPTICS_kafka_multiprocess_v01.py
"""

""" OPTICStool"""

###########################################################################################################
""" Import python files """
###########################################################################################################
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))
import OPTICS_config as cfg



###########################################################################################################
""" Import libraries """
###########################################################################################################
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import csv
import json
import pandas
import time
import logging
import multiprocessing
import numpy
from kafka.admin import KafkaAdminClient, NewTopic
import os
import datetime
from sklearn.cluster import OPTICS
import pyproj
from shapely.geometry import Point
import shapely
import geopandas as gpd



###########################################################################################################
""" OPTICS """
###########################################################################################################
def fun_optics_model(data, min_samples, max_eps):
    model = OPTICS(min_samples=min_samples, max_eps=max_eps)
    model = model.fit(data[['lon', 'lat']])
    predicted_clusters = model.labels_
    # print("predictions:", predicted_clusters)
    return predicted_clusters


###########################################################################################################
""" OPTICS """
###########################################################################################################
def fun_optics_model2(min_samples, max_eps):

    startt = time.time()

    df = pandas.read_csv(cfg.CSV_FILE_FOR_READ, header=0, delimiter=',', index_col=None)
    print(df.columns)
    #print(df)
    #print(df['timestamp'])
    print(df)	
    df['id'] = df['mmsi'].copy()
    df['timestamp'] = (pandas.to_datetime(df['timestamp']).astype(int) / 10 ** 6).astype(int)

    df.sort_values(by=['id', 'timestamp'], ascending=[True, True], inplace=True); df.reset_index(drop=True, inplace=True)

    df.drop_duplicates(keep='first', inplace=True) # Drop duplicates
    df.drop_duplicates(subset=['id', 'timestamp'], inplace=True)  # Check if there are points with the same timestamp

    myProj = pyproj.Proj("+proj=utm +zone=35S, +ellps=WGS84 +datum=WGS84 +units=m +no_defs") 
    lon2, lat2 = myProj(df['lon'].values, df['lat'].values)
    df['UTMlon'] = lon2
    df['UTMlat'] = lat2

    df['dt'] = (df['timestamp'].groupby(df['id']).diff()).values  # Diff Time in seconds
    df['dlon'] = (df['UTMlon'].groupby(df['id']).diff()).values  # Diff Longitude in meters
    df['dlat'] = (df['UTMlat'].groupby(df['id']).diff()).values  # Diff Latitude in meters
    df['dist_m'] = numpy.sqrt(df.dlon.values ** 2 + df.dlat.values ** 2)  # euclidean distance in meters
    df['my_speed'] = df['dist_m'] / df['dt'] # calculate speed m/s

    CFG_THRESHOLD_SPEED_MIN = cfg.CFG_THRESHOLD_SPEED_MIN*0.514444444
    CFG_THRESHOLD_SPEED_MAX = cfg.CFG_THRESHOLD_SPEED_MAX*0.514444444
    df = df.drop(df[df['my_speed'] < CFG_THRESHOLD_SPEED_MIN].index) # Drop points with limited speed
    df = df.drop(df[df['my_speed'] > CFG_THRESHOLD_SPEED_MAX].index) # Drop points with high speed

    print("Start OPTICS....")
    model = OPTICS(min_samples=min_samples, max_eps=max_eps, n_jobs=-1)
    data = df[['UTMlon', 'UTMlat']].copy()
    #data = data.iloc[:1000].copy()
    model = model.fit(data)
    p_cl = model.labels_
    print("predictions:", p_cl)
    df['oo'] = p_cl
    df.to_csv('optics_clusters.csv')

    print("ELAPSED TIME:", time.time() - startt)

    return p_cl, df


###########################################################################################################
""" FIND POLYGONS """
###########################################################################################################
def fun_convex_hull(df):

    startt = time.time()

    df.sort_values(by=['oo'], ascending=[True], inplace=True)
    df.reset_index(drop=True, inplace=True)

    df['geometry'] = [shapely.geometry.Point(xy) for xy in zip(df['lon'], df['lat'])]

    anchs_clusters = pandas.DataFrame()
    anchs_clusters_gdf = gpd.GeoDataFrame()
    gb = df.groupby(df['oo'])
    for y in gb.groups:
        df0 = gb.get_group(y).copy()
        point_collection = shapely.geometry.MultiPoint(list(df0['geometry']))
        # point_collection.envelope
        convex_hull_polygon = point_collection.convex_hull
        anchs_clusters = anchs_clusters.append(pandas.DataFrame(data={'anchorage_id':[y],'geom':[convex_hull_polygon]}))
        anchs_clusters_gdf = anchs_clusters_gdf.append(gpd.GeoDataFrame({'anchorage_id':[y],'geometry':[convex_hull_polygon]}))

    anchs_clusters.reset_index(inplace=True)
    anchs_clusters_gdf.reset_index(inplace=True)
    anchs_clusters_gdf.crs = 'epsg:4326'
    anchs_clusters.to_csv('anchs_clusters.csv')

    print("ELAPSED TIME:", time.time() - startt)

    return anchs_clusters, anchs_clusters_gdf


###########################################################################################################
""" INTERSECT """
###########################################################################################################
def fun_intersect(anchs_clusters, anchs_clusters_gdf):

    startt = time.time()

    anchs_buffer = pandas.read_csv(cfg.CSV_FILE_FOR_READ_anchs, header=0, delimiter=';', index_col=None)
    print(anchs_buffer.columns)

    anchs_buffer_gdf = gpd.GeoDataFrame()
    anchs_buffer_gdf['cd'] = anchs_buffer['cd']
    anchs_buffer_gdf['geometry'] = [shapely.wkb.loads(xy, hex='True') for xy in anchs_buffer['var_buffer']]
    anchs_buffer_gdf.crs = 'epsg:2100'
    anchs_buffer_gdf = anchs_buffer_gdf.copy().to_crs('epsg:4326')

    anchs_intersect_gdf = anchs_buffer_gdf.copy()
    anchs_intersect_gdf['intersect_num_clusters'] = -10
    anchs_intersect_gdf['intersect_clusters_ids'] = '-'
    anchs_intersect_gdf['intersect_clusters_ids'] = anchs_intersect_gdf['intersect_clusters_ids'].astype(object)
    anchs_intersect_gdf['intersect_clusters_geom'] = anchs_intersect_gdf['intersect_clusters_ids'].copy()

    for i in range(anchs_buffer_gdf['geometry'].shape[0]):
        anchs_intersect0 = anchs_clusters_gdf['geometry'].intersects(anchs_buffer_gdf['geometry'][i])
        # print(anchs_intersect.sum().copy())
        anchs_intersect_gdf['intersect_num_clusters'][i] = anchs_intersect0.sum()
        if anchs_intersect0.sum()>0:
            anchs_intersect_gdf['intersect_clusters_ids'][i] = anchs_clusters_gdf[anchs_intersect0==True]['anchorage_id'].to_list()
            anchs_intersect_gdf['intersect_clusters_geom'][i] = anchs_clusters_gdf[anchs_intersect0==True]['geometry'].to_list()



    anchs_intersect = pandas.DataFrame(anchs_intersect_gdf)
    anchs_intersect.to_csv('anchs_intersect.csv')

    print("ELAPSED TIME:", time.time() - startt)

    return anchs_intersect, anchs_intersect_gdf


###########################################################################################################
""" MAIN """
###########################################################################################################
if __name__ == "__main__":
    print('[Stage 1 - Validity Check] Preprocessing Input AIS/VMS Data...')
    print('optics'); p_cl, df = fun_optics_model2(cfg.MIN_SAMPLES, cfg.MAX_EPS)
    #print('load csv'); df = pandas.read_csv('optics_clusters.csv', delimiter=',')

    print('[Stage 2 - Data Clustering] Clustering with OPTICS...')
    print('convex'); anchs_clusters, anchs_clusters_gdf = fun_convex_hull(df)

    print('[Stage 3 - Anchorage Discovery] Create Polygons from Convex Hulls of OPTICS\' Clusters...')
    print('intersect'); anchs_intersect, anchs_intersect_gdf = fun_intersect(anchs_clusters, anchs_clusters_gdf)
    print("END")
