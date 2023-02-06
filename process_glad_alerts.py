import ee

# Initialize the Earth Engine client
ee.Initialize()

# Recent GLAD alerts
recent_glad_alerts = ee.ImageCollection('projects/glad/alert/UpdResult')
# Set the latest day alerts
max_days_alerts = 3
# Set Google Drive path output
drive_folder = 'GLAD'

# Define the area of interest (AoI)
adm_simplified = ee.FeatureCollection("users/rfirmansyah/adm/idn_adm_lv1_50k_simplified")
aoi_name = 'Jambi'
aoi = adm_simplified.filter(ee.Filter.eq('WADMPR', aoi_name)).geometry()
# Filter the recent GLAD alerts to only include those within the AoI
aoi_glad_alerts = recent_glad_alerts.filterBounds(aoi)

# Filter the image to probable loss and confirmed loss only
band_conf = 'conf23'
band_alert_date = 'alertDate23'
def filter_loss(image):
    loss = image.select(band_conf)
    return image.updateMask(loss.gt(0))

glad_alerts = aoi_glad_alerts.map(filter_loss)

# Sort the glad alerts by date in ascending order and get the last days of alerts
max_glad_alerts = glad_alerts.sort('system:time_start', False).limit(max_days_alerts)
aoi_alerts_list = max_glad_alerts.toList(max_glad_alerts.size())

# Export the alerts to Google Drive as TIFF files
def export_img2tiff_drive(image, i):
    glad_alerts = image.select(band_alert_date)
    task = ee.batch.Export.image.toDrive(
        image=glad_alerts,
        description='glad_alerts_' + aoi_name.lower().replace(' ', '_') + '_' + str(i),
        folder=drive_folder,
        region=glad_alerts.geometry(),
        crs=glad_alerts.projection(),
        scale=image.projection().nominalScale(),
        maxPixels=1e13
    )
    task.start()

# Export the alerts to Google Drive as SHP files
def export_img2shp_drive(image, i):
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
        folder=drive_folder,
        fileFormat="SHP"
    )
    task.start()

# Looping through each alert in the list to export each one individually and save to Google Drive
for i in range(max_days_alerts):
    event_info = ee.Image(aoi_alerts_list.get(i)).getString('system:index').getInfo()[:5]
    daily_glad = ee.Image(aoi_alerts_list.get(i)).clip(aoi)
    export_img2shp_drive(daily_glad, event_info)