import ee
import time
import geopandas as gpd
import json
import logging
from datetime import datetime
import argparse, sys, os
import configparser
import uuid
from retry import retry
from download_drive_files import download_files_from_gdrive
import glob
import pandas as pd

current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

"""
README:
https://storage.googleapis.com/earthenginepartners-hansen/GFC-2023-v1.11/download.html
https://glad-forest-alert.appspot.com/
"""

# get user inputs from config file:
config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_file.ini")
config = configparser.ConfigParser()
config.read(config_file)
config_dict = config['inputs']

def _parse_args(args):
    parser = argparse.ArgumentParser(
        description="Batch process GLAD alerts from Google Earth Engine datasets",
        epilog="""Example of usage to process GLAD alerts:
            Download and process the GLAD alerts within your project area
                python glad_alerts.py 
            """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-rd", "--remove_duplicates", help="Removing duplicate geometries after merging?, default to True", default=True)
    
    return parser.parse_args(args)

##################################
# Provide your inputs here
##################################
LOG_FILE = config_dict['log_file']
service_account = config_dict['service_account']
key_iam = config_dict['key_iam']
temp_download = config_dict['temp_download']
google_drive_folder = config_dict['google_drive_folder']
credentials = ee.ServiceAccountCredentials(service_account, key_iam)
aoi_name = config_dict['aoi_name']
shapefile = config_dict['shapefile']
final_shp = config_dict['final_shp']
# Set the latest day alerts
max_days_retrieval = config_dict['max_days_retrieval']
year = config_dict['year']
final_path = config_dict['final_path']
# Filter the image to probable loss and confirmed loss only
band_conf = config_dict['band_conf'] + config_dict['year']
band_alert_date = config_dict['band_alert_date'] + config_dict['year']

##################################
# Provide a log to trace the process
##################################
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.getLogger().addHandler(logging.StreamHandler())
_str_decorator = "=" * 20
logging.info(f"\n{_str_decorator} BEGINNING LOG {_str_decorator}")

# Using high-volume EE endpoints to make requests simultaneously as recommended by Gorelick in this blogpost https://gorelick.medium.com/fast-er-downloads-a2abd512aa26
ee.Initialize(credentials, opt_url='https://earthengine-highvolume.googleapis.com')

# Recent GLAD alerts
recent_glad_alerts = ee.ImageCollection('projects/glad/alert/UpdResult')

def export2GCP(image, fileName, aoi, scale=30, crs='EPSG:4326'):
    """Export Earth Engine image to Google Cloud Storage.

    Args:
        image: Earth Engine image to export.
        fileName (str): Name to assign to the exported file.
        aoi: Area of interest for the export.
        scale (int, optional): Resolution in meters. Defaults to 300.
        crs (str, optional): Coordinate Reference System. Defaults to 'EPSG:4326'.
    """
    gcp_bucket_name = config_dict['gcp_bucket_name']
    gcp_bucket_prefix = config_dict['gcp_bucket_prefix']
    task = ee.batch.Export.image.toCloudStorage(
        image=image, 
        description=fileName,
        bucket=gcp_bucket_name,
        fileNamePrefix=gcp_bucket_prefix + fileName,
        region=aoi,
        scale=scale,
        crs=crs,
        maxPixels=1e13)
    
    task.start()
    while task.active():
        print(f"Waiting on (id: {task.id})")
        time.sleep(30)

def export2drive_tiff(image, i):
    # Export the alerts to Google Drive as TIFF files
    glad_alerts = image.select(band_alert_date)
    task = ee.batch.Export.image.toDrive(
        image=glad_alerts,
        description='glad_alerts_' + aoi.lower().replace(' ', '_') + '_' + str(i),
        folder=google_drive_folder,
        region=glad_alerts.geometry(),
        crs=glad_alerts.projection(),
        scale=image.projection().nominalScale(),
        maxPixels=1e13
    )
    task.start()

def export2drive_shp(image, i):
    # Export the alerts to Google Drive as SHP files
    conf = image.select(band_conf)
    alert_date = image.select(band_alert_date)
    loss_alerts = conf.addBands(alert_date)
    features = loss_alerts.reduceToVectors(
        reducer=ee.Reducer.first().setOutputs(["alert_date"]),
        geometry=loss_alerts.geometry(),
        crs=loss_alerts.projection(),
        labelProperty="conf",
        scale=image.projection().nominalScale(),
        geometryType="polygon",
        eightConnected=False,
        maxPixels=1e13
    )
    print(f"Save the glad alerts {aoi_name.lower().replace(' ', '_') + '_' + str(i)} to google drive")
    task = ee.batch.Export.table.toDrive(
        collection=features,
        description='glad_alerts_' + aoi_name.lower().replace(' ', '_') + '_' + str(i),
        folder=google_drive_folder,
        fileFormat="SHP"
    )
    task.start()
    
class GladProcessing(object):

    def __init__(self, remove_duplicates):
        self.remove_duplicates = remove_duplicates
        self.crs = 'EPSG:4326'
        
    # @retry(tries=10, delay=1, backoff=2)
    def get_request(self):
        # iterate over rows of geometry to prevent the limitation of GEE
        aoi_df = gpd.read_file(shapefile).to_json()

        try:
            aoi = ee.FeatureCollection(json.loads(aoi_df)).geometry()
            aoi_glad_alerts = recent_glad_alerts.filterBounds(aoi)
            
            def filter_loss(image):
                loss = image.select(band_conf)
                return image.updateMask(loss.gt(0))

            glad_alerts = aoi_glad_alerts.map(filter_loss)

            # Sort the glad alerts by date in ascending order and get the last days of alerts
            max_glad_alerts = glad_alerts.sort('system:time_start', False).limit(int(max_days_retrieval))
            aoi_alerts_list = max_glad_alerts.toList(max_glad_alerts.size())
            n = aoi_alerts_list.size().getInfo()

            # Looping through each alert in the list to export each one individually and save to Google Drive
            for i in range(int(max_days_retrieval)):
                event_info = ee.Image(aoi_alerts_list.get(i)).getString('system:index').getInfo()[:5]
                daily_glad = ee.Image(aoi_alerts_list.get(i)).clip(aoi)
                export2drive_shp(daily_glad, event_info)
                
        except Exception as e:
            logging.info(f"Stack trace error: {e}")

        
    def merge2shp(self, filename):
        def get_mdy(year, numday):
            """
            Since the PostgreSQL database has a datestyle format: mdy, so then this converts the same type
            Convert days of number from 0-366 to datetime format
            
                Args:
                    year: int
                    
                    numday: int
            """
            import datetime

            d0 = datetime.date(year,1,1)
            deltaT = datetime.timedelta(numday - 1)
            d = d0 + deltaT
            date = datetime.date(year, d.month, d.day)
            return date.strftime('%m-%d-%Y')

        # Create an empty GeodataFrame to hold the merged data
        merged_gdf = gpd.GeoDataFrame()
        files = [f for f in glob.glob(f"{temp_download}/*.shp")]
        
        from osgeo import gdal
        gdal.SetConfigOption('SHAPE_RESTORE_SHX', 'YES')
        try:
            for file in files:
                gdf = gpd.read_file(file)
                gdf['year'] = 2000 + int(year)
                merged_gdf = pd.concat([merged_gdf, gdf], ignore_index=True)
            
        finally:
            if self.remove_duplicates:
                # removing duplicate by the alert_date and conf
                logging.info(f"Removing possible duplicate of {filename}")
                # merged_gdf = merged_gdf.drop_duplicates('geometry')
                merged_gdf['wkt'] = merged_gdf.apply(lambda x: x.geometry.wkt, axis=1)
                merged_gdf = merged_gdf.dissolve(by="wkt")
                # # removing wkt column
                # merged_gdf.drop('wkt', axis=1, inplace=True)
                # Convert the WKT column to geometry
                # merged_gdf['geometry'] = gpd.GeoSeries.from_wkt(merged_gdf['wkt_column'])
                # merged_gdf = merged_gdf.to_crs(self.crs)

                # # Remove duplicates based on geometry
                # merged_gdf = merged_gdf.buffer(0).unary_union

                # # Convert back to GeoDataFrame
                # merged_gdf = gpd.GeoDataFrame(geometry=[merged_gdf])

                # # If you need to reset the index
                # merged_gdf.reset_index(drop=True, inplace=True)

            for idx, row in merged_gdf.iterrows():
                merged_gdf.at[idx, 'date'] = get_mdy(int(merged_gdf.at[idx, 'year']), int(merged_gdf.at[idx, 'alert_date']))
                # insert random uuid for its primary key
                merged_gdf.at[idx, 'uuid'] = str(merged_gdf.at[idx, 'alert_date']) + str(merged_gdf.at[idx, 'year']) + str(uuid.uuid4())
        out = os.path.join(final_path, filename + ".shp")
        merged_gdf.to_file(out, driver='ESRI Shapefile')
        
    def remove_file(self):
        files = [f for f in glob.glob(f"{temp_download}/*")]
        for file in files:
            logging.info(f"Removing file {file} ....")
            os.remove(file)


def main():
    args = _parse_args(sys.argv[1:])
    start = datetime.now()
    glad_gee = GladProcessing(remove_duplicates=True)

    try:
        glad_gee.get_request()
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        download_files_from_gdrive(key_iam=key_iam, drive_folder=google_drive_folder, out_path=temp_download)
        try:
            glad_gee.merge2shp(final_shp)
        finally:
            glad_gee.remove_file()

    logging.info(f"elapsed time to process the data: {datetime.now() - start}")

if __name__ == "__main__":
    main()