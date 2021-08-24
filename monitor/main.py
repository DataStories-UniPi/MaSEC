import urllib
import time, datetime
import numpy as np
import pandas as pd
import geopandas as gpd

import bokeh
import bokeh.models as bokeh_models
import bokeh.layouts as bokeh_layouts
import bokeh.io as bokeh_io
from bokeh.driving import linear

import sys, os
sys.path.append(os.path.join(os.path.expanduser('~'), 'Documents', 'DataStories-UniPi', 'ST_Visions'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))

from st_visualizer import st_visualizer
import express as viz_express
import geom_helper as viz_helper 

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from monitor.vessel_positions_json import get_data, track_selected, toggle_renderers, APP_ROOT
from shapely import wkt, wkb

from kafka_config_c_p_v01 import CFG_TOPIC_SUFFIX, CFG_BUFFER_COLUMN_NAMES, CFG_CSV_OUTPUT_DIR



def render_items(df, st_viz_instance):
	st_viz_instance.canvas_data = df.to_crs(epsg=3857)
	st_viz_instance.canvas_data = st_viz_instance.prepare_data(st_viz_instance.canvas_data)
	st_viz_instance.source.data = st_viz_instance.canvas_data.drop(st_viz_instance.canvas_data.geometry.name, axis=1).to_dict(orient="list")


def main():
	title = 'SSTD\'21 Demo'
	
	x_range = (2603068.0, 2642460.0)
	y_range = (4533512.0, 4584541.0)

	sizing_mode='stretch_both'
	plot_width=250
	plot_height=250

	datefmt_str = "%Y-%m-%d %H:%M:%S"
	datefmt = bokeh.models.DateFormatter(format=datefmt_str)
	hfmt_str = "0.00"
	hfmt = bokeh.models.NumberFormatter(format=hfmt_str)

	consumer_ec = KafkaConsumer(f'ecdresults{CFG_TOPIC_SUFFIX}',
                     bootstrap_servers=['localhost:9092'],
                     group_id='MaSEC_VisionsConsumer_EvolvingClusters',
                     auto_offset_reset='latest',
					 max_poll_interval_ms=10000,
					 max_poll_records=1)
					 
	consumer_ac = KafkaConsumer(f'acresults{CFG_TOPIC_SUFFIX}',
                     bootstrap_servers=['localhost:9092'],
                     group_id='MaSEC_VisionsConsumer_Anchorages',
                     auto_offset_reset='latest',
					 max_poll_interval_ms=10000,
					 max_poll_records=1)

	consumer_op = KafkaConsumer(f'buffer{CFG_TOPIC_SUFFIX}',
                     bootstrap_servers=['localhost:9092'],
                     group_id='MaSEC_VisionsConsumer_ObjectPool',
                     auto_offset_reset='latest',
					 max_poll_interval_ms=10000,
					 max_poll_records=1)


	@linear()
	def update(step):
		# Flusing old Data
		st_viz.figure.title.text = 'Refreshing...'

		# Fetching New Data
		df_evolving_clusters, df_anchs, df_points = get_data(consumer_ec, consumer_ac, consumer_op)
		
		# Visualize Data
		## Hide Objects and Replace Tracked Indices behind-the-scenes (Fix Flickering)
		_ = toggle_renderers(st_viz)
		_ = toggle_renderers(st_viz2)
		_ = toggle_renderers(st_viz3)

		## Prepare Data for Visualization -- st_viz
		# If no new data are fetched, do not refresh (otherwise the view will be flushed)
		if not df_evolving_clusters.empty:
			render_items(df_evolving_clusters, st_viz)
			
		## Prepare Data for Visualization -- st_viz2
		if not df_anchs.empty:
			render_items(df_anchs, st_viz2)

		## Prepare Data for Visualization -- st_viz3
		if not df_points.empty:
			render_items(df_points, st_viz3)

		## Show Objects (Fix Flickering)
		toggle_renderers(st_viz)
		toggle_renderers(st_viz2)
		toggle_renderers(st_viz3)

		## Flush Buffers
		st_viz.canvas_data = None
		st_viz.data = None
		
		## Restore Title
		st_viz.figure.title.text = f'Discovering Moving and Stationary ECs (MCS) within Saronic Gulf @{datetime.datetime.now(datetime.timezone.utc).strftime("%d-%m-%Y %H:%M:%S")} UTC'
		


	# Design the First view of the Platform (prior to the Update)
	df_evolving_clusters = gpd.GeoDataFrame(data=[], columns=['clusters', 'st', 'et', 'geometry', 'status', 'type'], geometry='geometry', crs='epsg:4326')
	df_anchs             = gpd.GeoDataFrame(data=[], columns=['clusters', 'st', 'et', 'geometry', 'status', 'type'], geometry='geometry', crs='epsg:4326')
	df_points            = gpd.GeoDataFrame(data=[], columns=[*CFG_BUFFER_COLUMN_NAMES, 'moving', 'geometry'], geometry='geometry', crs='epsg:4326')



	# Instance #1 - Moving ECs
	st_viz = st_visualizer(limit=30000)
	st_viz.set_data(df_evolving_clusters.copy())

	basic_tools = "tap,pan,wheel_zoom,save,reset" 

	st_viz.create_canvas(x_range=x_range, y_range=y_range, 
						 title=f'Discovering Moving and Stationary ECs (MCS) within Saronic Gulf @{datetime.datetime.now(datetime.timezone.utc).strftime("%d-%m-%Y %H:%M:%S")} UTC',
						 sizing_mode=sizing_mode, plot_width=plot_width, plot_height=plot_height, 
						 height_policy='max', tools=basic_tools, output_backend='webgl')
	st_viz.add_map_tile('CARTODBPOSITRON')


	# Add (Different) Colors for Active and Closed Moving ECs
	ec_moving = bokeh_models.CDSView(source=st_viz.source, filters=[bokeh_models.GroupFilter(column_name='status', group='Active')])
	ec_stat = bokeh_models.CDSView(source=st_viz.source, filters=[bokeh_models.GroupFilter(column_name='status', group='Closed')])

	ec_moving_poly = st_viz.add_polygon(legend_label='Moving ECs (Active)', fill_color='#4292c6', line_color='#4292c6', fill_alpha=0.3, line_alpha=0.3, muted_alpha=0, nonselection_alpha=0, view=ec_moving)
	ec_stat_poly = st_viz.add_polygon(legend_label='Moving ECs (Closed)', fill_color='#9ecae1', line_color='#9ecae1', fill_alpha=0.3, line_alpha=0.3, muted_alpha=0, nonselection_alpha=0, view=ec_stat)


	tooltips = [('Cluster Start','@st{{{0}}}'.format(datefmt_str)), ('Cluster End','@et{{{0}}}'.format(datefmt_str))]
	st_viz.add_hover_tooltips(tooltips=tooltips, formatters={'@st':'datetime', '@et':'datetime'}, mode="mouse", muted_policy='ignore', renderers=[ec_moving_poly, ec_stat_poly])

	columns_ec = [
		bokeh_models.TableColumn(field="clusters", title="Clusters", sortable=False),
		bokeh_models.TableColumn(field="st", title="Start", default_sort='descending', formatter=datefmt),
		bokeh_models.TableColumn(field="et", title="End", sortable=False, formatter=datefmt),
		bokeh_models.TableColumn(field="status", title="Status", sortable=False)
	]
	data_table_ec_title = bokeh_models.Div(text=f'''<h4>Moving ECs</h4>''', height_policy='min')    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.
	data_table_ec = bokeh_models.DataTable(source=st_viz.source, columns=columns_ec, height_policy='max', width_policy='max', height=plot_height, width=plot_width)



	# Instance #2 - Stationary ECs (Anchorages)
	st_viz2 = st_visualizer(limit=30000)
	st_viz2.set_data(df_anchs.copy())
	
	st_viz2.set_figure(st_viz.figure)
	st_viz2.create_source()


	# Add (Different) Colors for Active and Closed Stationary ACs
	ac_moving = bokeh_models.CDSView(source=st_viz2.source, filters=[bokeh_models.GroupFilter(column_name='status', group='Active')])
	ac_stat = bokeh_models.CDSView(source=st_viz2.source, filters=[bokeh_models.GroupFilter(column_name='status', group='Closed')])

	ac_moving_poly = st_viz2.add_polygon(legend_label='Stationary ECs (Active)', fill_color='#439775', line_color='#439775', fill_alpha=0.3, line_alpha=0.3, muted_alpha=0, nonselection_alpha=0, view=ac_moving)
	ac_stat_poly = st_viz2.add_polygon(legend_label='Stationary ECs (Closed)', fill_color='#61D095', line_color='#61D095', fill_alpha=0.3, line_alpha=0.3, muted_alpha=0, nonselection_alpha=0, view=ac_stat)
	

	tooltips = [('Cluster Start','@st{{{0}}}'.format(datefmt_str)), ('Cluster End','@et{{{0}}}'.format(datefmt_str))]
	st_viz2.add_hover_tooltips(tooltips=tooltips, formatters={'@st':'datetime', '@et':'datetime'}, mode="mouse", muted_policy='ignore', renderers=[ac_moving_poly, ac_stat_poly])

	columns_ac = [
		bokeh_models.TableColumn(field="clusters", title="Clusters", sortable=False),
		bokeh_models.TableColumn(field="st", title="Start", default_sort='descending', formatter=datefmt),
		bokeh_models.TableColumn(field="et", title="End", sortable=False, formatter=datefmt),
		bokeh_models.TableColumn(field="status", title="Status", sortable=False)
	]
	data_table_ac_title = bokeh_models.Div(text=f'''<h4>Stationary ECs</h4>''', height_policy='min')    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.
	data_table_ac = bokeh_models.DataTable(source=st_viz2.source, columns=columns_ac, height_policy='max', width_policy='max', height=plot_height, width=plot_width)	



	# Instance #3 - Vessels' (AIS) Points
	st_viz3 = st_visualizer(limit=30000)
	st_viz3.set_data(df_points.copy())
	
	st_viz3.set_figure(st_viz2.figure)
	st_viz3.create_source()


	# Add (Different) Colors for Active and Closed Stationary ACs
	ais_stat = bokeh_models.CDSView(source=st_viz3.source, filters=[bokeh_models.GroupFilter(column_name='moving', group='N')])
	ais_moving = bokeh_models.CDSView(source=st_viz3.source, filters=[bokeh_models.GroupFilter(column_name='moving', group='Y')])

	ais_pts_stat = st_viz3.add_glyph(fill_color='orangered', line_color='orangered', size=5, alpha=0.5, fill_alpha=0.5, muted_alpha=0, nonselection_alpha=0, legend_label=f'AIS Points (Stationary)', view=ais_stat)
	ais_pts_moving = st_viz3.add_glyph(fill_color='forestgreen', line_color='forestgreen', size=5, alpha=0.5, fill_alpha=0.5, muted_alpha=0, nonselection_alpha=0, legend_label=f'AIS Points (Moving)', view=ais_moving)

	tooltips = [('Vessel ID','@mmsi'), ('Timestamp','@timestamp{{{0}}}'.format(datefmt_str)), ('Location (lon., lat.)','(@lon{{{0}}}, @lat{{{1}}})'.format(hfmt_str, hfmt_str)), ('Heading (deg.)', '@heading'), ('Moving', '@moving')]
	st_viz3.add_hover_tooltips(tooltips=tooltips, formatters={'@timestamp':'datetime'}, mode="mouse", muted_policy='ignore', renderers=[ais_pts_moving, ais_pts_stat])

	columns_ais = [
		bokeh_models.TableColumn(field="mmsi", title="MMSI", sortable=False),
		bokeh_models.TableColumn(field="timestamp", title="Timestamp", default_sort='descending', formatter=datefmt),
		bokeh_models.TableColumn(field="lon", title="Longitude", sortable=False, formatter=hfmt),
		bokeh_models.TableColumn(field="lat", title="Latitude", sortable=False, formatter=hfmt),
		bokeh_models.TableColumn(field="heading", title="Heading", sortable=False),
		bokeh_models.TableColumn(field="speed", title="Speed", sortable=False),
		bokeh_models.TableColumn(field="moving", title="Moving", sortable=False)
	]
	data_table_ais_title = bokeh_models.Div(text=f'''<h4>Vessels' Points</h4>''', height_policy='min')    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.
	data_table_ais = bokeh_models.DataTable(source=st_viz3.source, columns=columns_ais, height_policy='max', width_policy='max', height=plot_height, width=plot_width)	


	# Instance #4 - Offline Anchorages
	st_viz4 = st_visualizer(limit=30000)

	df2 = pd.read_csv(os.path.join(CFG_CSV_OUTPUT_DIR, f'anchs{CFG_TOPIC_SUFFIX}', 'anchs_clusters.csv'))
	df2.loc[:, 'geom'] = df2.loc[:, 'geom'].apply(wkt.loads)
	gdf2 = gpd.GeoDataFrame(df2, crs='epsg:4326', geometry='geom')
	gdf2 = gdf2.iloc[1:]

	st_viz4.set_data(gdf2.loc[gdf2.geom_type == 'Polygon'].copy())
	st_viz4.set_figure(st_viz3.figure)
	st_viz4.create_source()

	ais_pts = st_viz4.add_polygon(legend_label='Anchorages', fill_color='darkkhaki', line_color='darkkhaki', fill_alpha=0.35, line_alpha=0.35, muted_alpha=0, nonselection_alpha=0)


	st_viz.figure.legend.title = 'Legend'
	st_viz.figure.legend.title_text_font = 'Arial'
	st_viz.figure.legend.title_text_font_style = 'bold'

	st_viz.figure.match_aspect = True
	st_viz.figure.add_tools(bokeh_models.BoxZoomTool(match_aspect=True))

	st_viz.figure.legend.location = "top_left"
	st_viz.figure.legend.click_policy = "mute"
	st_viz.figure.toolbar.active_scroll = st_viz.figure.select_one(bokeh_models.WheelZoomTool)


	doc = bokeh_io.curdoc()

	# url = os.path.join(os.path.basename(APP_ROOT), 'static', 'logo.png')
	# app_logo = bokeh_models.Div(text=f'''<img src={url} width=73>''', height_policy='min', margin=(-5, 0, -35, 0))    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.
	# logo_height = 27
	# app_logo = bokeh_models.Div(text=f'''<a href="http://www.datastories.org/" target="_blank"> <img src={url} height={logo_height}> </a>''', height_policy='min', height=logo_height, margin=(-5, 0, -5, 0))    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.
	
	st_viz4.show_figures([[st_viz3.figure, bokeh_layouts.column([data_table_ec_title, data_table_ec, data_table_ac_title, data_table_ac, data_table_ais_title, data_table_ais])]], notebook=False, toolbar_options=dict(logo=None), sizing_mode='stretch_both', doc=doc, toolbar_location='right')
	doc.add_periodic_callback(update, 2000) #period in ms

main()