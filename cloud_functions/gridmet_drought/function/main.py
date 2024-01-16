import datetime
import logging
import ee
import google.auth
from flask import abort, Response

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


def handle_event(event):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        event (dict): Event payload.
    """

    # Need to authenticate using instructions here: https://google-auth.readthedocs.io/en/master/reference/google.auth.html
    credentials, project_id = google.auth.default();

    ee.Initialize(credentials, project=project_id);
    run_export()


def run_export():
    """Earth Engine export function
    """
    # Define time period to export
    start_date = datetime.datetime(1980, 1, 1)
    end_date = datetime.datetime(2050, 1, 1)

    # Define input Image Collection
    in_ic_name = 'GridMET_Drought'
    in_ic_paths = ['GRIDMET/DROUGHT']
    in_ic = ee.ImageCollection(in_ic_paths[0])
    in_ic_res = ee.Number(in_ic.first().projection().nominalScale()).round().getInfo()

    # Loop over land unit types
    fc_types  = ['allotment', 'fieldoffice', 'districtoffice', 'stateoffice']

    for fc_type in fc_types:

        in_fc, in_fc_path, in_fc_id, land_unit_long, land_unit_short, tile_scale, mask, mask_path = define_parameters(fc_type)

        # Define variable from Image Collection
        var_dict = {'Long_Term_Drought_Blend': {'units': 'drought'},
                    'Short_Term_Drought_Blend': {'units': 'drought'}}
        
        # Loop over variables
        var_names = ['Long_Term_Drought_Blend', 'Short_Term_Drought_Blend']

        for var_name in var_names:

            var_type = 'Categorical'
            var_units = var_dict.get(var_name).get('units')

            out_path = f"projects/climate-engine-pro/assets/blm-database/{land_unit_short.replace('_', '').lower()}-{in_ic_name.replace('_', '').lower()}-{var_name.replace('_', '').lower()}"
        
            # Get list of all dates
            all_dates = ee.ImageCollection('GRIDMET/DROUGHT').filterDate(start_date, end_date).filterDate(start_date, end_date).aggregate_array('system:time_start').getInfo()

            # Get list of dates from collection
            coll_dates = ee.ImageCollection(out_path).aggregate_array('system:time_start').distinct().getInfo()

            # Get list of dates missing from collection
            miss_dates = sorted(set(all_dates) - set(coll_dates))

            for date in miss_dates:

                print("Running ", datetime.datetime.fromtimestamp(date/1000.0), ' for ', land_unit_short, ' ', var_name)
        
                # Parse date for ID
                date_ymd = datetime.datetime.fromtimestamp(date/1000.0).strftime('%Y%m%d')
        
                # Create dictionary of properties for image    
                properties = {'system:index': date_ymd, 'system:time_start': date, 'land_unit_long': land_unit_long, 'land_unit_short': land_unit_short, 'in_fc_path': in_fc_path,\
                            'in_fc_id': in_fc_id, 'in_ic_paths': in_ic_paths, 'in_ic_path': in_ic_paths[0], 'in_ic_name': in_ic_name, 'in_ic_res': in_ic_res, 'var_type': var_type,\
                            'var_name': var_name, 'var_units': var_units, 'tile_scale': tile_scale, 'mask': mask}
        
                if mask == True:
                    properties['mask_path'] = mask_path
        
                elif mask == False:
                    properties['mask_path'] = 'None'
        
                # Generate image to extact statistics from
                in_i = preprocess_gm_drought(in_ic_paths = in_ic_paths, var_name = properties.get('var_name'), date = date)

                # Conditionally apply mask to images
                if properties.get('mask_path') == 'None':
                    # Do not apply mask
                    in_i = in_i
                else:
                    # Apply mask
                    in_i = in_i.updateMask(ee.Image(properties.get('mask_path')))

                # Cast in_fc_path to feature collection
                in_fc = ee.FeatureCollection(properties.get('in_fc_path'))

                # Run function to get time-series statistics for input feature collection
                out_fc = img_to_pts_categorical(in_i = in_i, in_fc = in_fc, in_ic_name = in_ic_name, tile_scale = properties.get('tile_scale'))

                # Convert centroid time-series to image collection time-series
                out_i = pts_to_img_categorical(in_fc = out_fc, in_ic_name = in_ic_name)

                # Create out region for export
                out_region = out_fc.geometry().buffer(20)

                # Export the image
                export_img(out_i = out_i, out_region = out_region, out_path = out_path, properties = properties)


def export_img(out_i, out_region, out_path, properties):
    '''
    :param out_i: e.g. Image to export returned from .pts_to_img*()
    :param out_region: e.g. Feature Collection at equator returned from .img_to_pts*()
    :param out_path: e.g. path for exported GEE asset
    :param properties: e.g. {'land-unit': land_unit, 'in-fc-path': in_fc_path, "in-fc-id": in_fc_id, "in-ic-paths": in_ic_path, "var-type": var_type, "var-name": var_name}
    :return: Earth Engine image of pixels at the equator with bands for histogram bins
    '''

    # Define variables for export task
    var_name_exp = properties.get('var_name').replace('_', '').lower()
    in_ic_name_exp = properties.get('in_ic_name').replace('_', '').lower()
    land_unit_exp = properties.get('land_unit_short').replace('_', '').lower()
    out_id = properties.get('system:index')

    # Queue and start export task
    task = ee.batch.Export.image.toAsset(
        image = out_i.set(properties),
        description = f'append - {land_unit_exp} {in_ic_name_exp} {var_name_exp} - {out_id}',
        assetId = f'{out_path}/{out_id}',
        region = out_region,
        scale = 22.264,
        maxPixels = 1e13)
    task.start()


def img_to_pts_categorical(in_i, in_fc, in_ic_name, tile_scale):
    """
    :param in_i: e.g. Image for single date
    :param in_fc: e.g. ee.FeatureCollection(in_fc_path)
    :return: Earth Engine Feature Collection of points at the equator with properties for histogram bins
    """
    # Cast input image to ee.Image
    img = ee.Image(in_i)

    # Need to further pre-process drought blends to be able to extract bins consistent with drought.gov
    # There are no reducers that allow histogram bins with variable widths, so we have to put bins into categories to start
    if in_ic_name == "GridMET_Drought":
        img = img.where(img.lt(-2.0), 0)\
            .where(img.lt(-1.5).And(img.gte(-2.0)), 1)\
            .where(img.lt(-1.2).And(img.gte(-1.5)), 2)\
            .where(img.lt(-0.7).And(img.gte(-1.2)), 3)\
            .where(img.lt(-0.5).And(img.gte(-0.7)), 4)\
            .where(img.lt(0.5).And(img.gte(-0.5)), 5)\
            .where(img.lt(0.7).And(img.gte(0.5)), 6)\
            .where(img.lt(1.2).And(img.gte(0.7)), 7)\
            .where(img.lt(1.5).And(img.gte(1.2)), 8)\
            .where(img.lt(2.0).And(img.gte(1.5)), 9)\
            .where(img.gte(2.0), 10).toInt()
        classes = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10']
    # Maintain original values for USDM
    elif in_ic_name == "USDM":
        img = img
        classes = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5']
    # Maintain original values for MTBS
    elif in_ic_name == "MTBS":
        img = img
        classes = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6']

    # Get resolution of the image
    res = img.select(0).projection().nominalScale()
    
    # Run reduce regions for allotments and select only the columns with reducers
    img_rr = img.reduceRegions(collection = in_fc, reducer = ee.Reducer.frequencyHistogram(),\
                                scale = res,\
                                tileScale = tile_scale).select(['histogram'])
    
    # Function to process histogram and key names to set as properties
    def process_histogram(f):
        # Cast function to feature
        f = ee.Feature(f)
        
        # Get histogram
        histogram = ee.Dictionary(f.get('histogram'))
        
        # Function to rename histogram keys 
        def rename_histogram_keys(key):
            key = ee.String(key).slice(0,2)
            return(ee.String('c').cat(key))
        
        # Rename histogram
        histogram = histogram.rename(histogram.keys(), histogram.keys().map(rename_histogram_keys))
        
        return(f.set(histogram))
    
    # Clean up histogram and set as properties
    img_rr = img_rr.map(process_histogram).select(['c.*'])

    # Add values of 0 for any histogram classes without values
    def add_missing_props(f):
        f = ee.Feature(f)
        
        # Get properties from reduceRegions call
        f_props = f.propertyNames().remove("system:index")
        
        # Iterate over properties in reduceRegions call to identify missing properties
        def get_missing_props(prop, classes):
            classes_remove = ee.List(classes).remove(prop)
            return(classes_remove)
        
        missing_props = ee.List(f_props.iterate(get_missing_props, classes))
        
        # Construct dictionary of missing_props: 0 to add as properties to each feature
        missing_props_dict = ee.Dictionary.fromLists(missing_props, ee.List.repeat(0, missing_props.length()))
        
        return(f.set(missing_props_dict))
    img_rr = img_rr.map(add_missing_props)

    # Get list of RR features
    img_rr_list = img_rr.toList(img_rr.size())
    
    # Get size of RR features
    img_rr_size = img_rr_list.size()
    
    # Function to create feature collection next to equator after reduction
    def pts_to_equator_rr(i):
        
        # Cast number to EE number
        i = ee.Number(i)
        
        # Get properties
        properties = ee.Feature(img_rr_list.get(i)).toDictionary()
        
        # Create geometry at equator
        geom = ee.Geometry.Point([i.multiply(0.0002),0.0002])
        
        # Return object with properties
        return(ee.Feature(geom).set(properties))

    # Create equator feature collection
    equator_fc = ee.FeatureCollection(ee.List.sequence(0, img_rr_size.subtract(1), 1).map(pts_to_equator_rr))
    
    return(equator_fc)


def pts_to_img_categorical(in_fc, in_ic_name):
    '''
    :param in_fc: e.g. output of img_to_pts_categorical
    :param in_ic_name: e.g. input image collection name for applying logic
    :return: Earth Engine image of pixels at the equator with bands for histogram bins
    '''
    # Cast to FeatureCollections
    fc = ee.FeatureCollection(in_fc)

    # Define classes for reclassification of properties
    # Reclassify drought blends using schema below
    # <-2.0 (D4) = c0
    # -2.0--1.5 (D3) = c1
    # -1.5--1.2 (D2) = c2
    # -1.2--0.7 (D1) = c3
    # -0.7--0.5 (D0) = c4
    # -0.5-0.5 (Neutral) = c5
    # 0.5-0.7 (W0) = c6
    # 0.7-1.2 (W1) = c7
    # 1.2-1.5 (W2) = c8
    # 1.5-2.0 (W3) = c9
    # >2.0 (W4) = c10
    if in_ic_name == "GridMET_Drought":
        classes = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10']
    # USDM classes described below
    # -1 Neutral or Wet = c0
    # 0 Abnormal Dry (D0) = c1
    # 1 Moderate Drought (D1) = c2
    # 2 Severe Drought (D2) = c3
    # 3 Extreme Drought (D3) = c4
    # 4 Exceptional Drought (D4) = c5
    elif in_ic_name == "USDM":
        classes = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5']
    # MTBS classes described below
    # 0 Background = c0
    # 1 Unburned to low severity = c1
    # 2 Low severity = c2
    # 3 Moderate severity = c3
    # 4 High severity = c4
    # 5 Increased greenness = c5
    # 6 Non-mapping area = c6
    elif in_ic_name == "MTBS":
        classes = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6']
        
    # Get list of properties to iterate over for creating multiband image for each date
    props = ee.List(classes)
    
    # Function to generate image from stats stored in Feature Collection property
    def generate_stat_image(prop):
        img = fc.reduceToImage(properties = [prop], reducer = ee.Reducer.mean()).rename([prop])
        return(img)
    
    # Generate multi-band stats image
    img_mb = ee.ImageCollection(props.map(generate_stat_image)).toBands()\
            .rename(props)
    
    return(img_mb)


# Function to calculate short-term and long-term blends
def preprocess_gm_drought(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in gridmet drought image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    def generate_gm_drought_imgs(img):

        # Define property list
        property_list = ["system:index", "system:time_start"]
        
        # Define preliminary variables for short-term blend calculation
        stb_variable = "Short_Term_Drought_Blend"
        stb_pdsi_img = img.select("pdsi")
        stb_z_img = img.select("z")
        stb_spi90d_img = img.select("spi90d")
        stb_spi30d_img = img.select("spi30d")
        
        # Define weights for short-term blend calculation
        stb_pdsi_coef = 0.2
        stb_z_coef = 0.35
        stb_spi90d_coef = 0.25
        stb_spi30d_coef = 0.2
        
        # Calculate short-term blend
        stblend = stb_pdsi_img.expression(
            "b() * pdsi_coef / 2 + spi90d * spi90d_coef + spi30d * spi30d_coef + z * z_coef / 2",{
                "spi90d": stb_spi90d_img, 
                "spi30d": stb_spi30d_img, 
                "z": stb_z_img, 
                "pdsi_coef": stb_pdsi_coef,
                "spi90d_coef": stb_spi90d_coef, 
                "spi30d_coef": stb_spi30d_coef, 
                "z_coef": stb_z_coef})
        
        # Define preliminary variables for long-term blend calculation
        ltb_variable = "Long_Term_Drought_Blend"
        ltb_pdsi_img = img.select("pdsi")
        ltb_spi180d_img = img.select("spi180d")
        ltb_spi1y_img = img.select("spi1y")
        ltb_spi2y_img = img.select("spi2y")
        ltb_spi5y_img = img.select("spi5y")
        
        # Define weights for long-term blend calculation
        ltb_pdsi_coef = 0.35
        ltb_spi180d_coef = 0.15
        ltb_spi1y_coef = 0.2
        ltb_spi2y_coef = 0.2
        ltb_spi5y_coef = 0.1
        
        # Calculate short-term blend
        ltblend = ltb_pdsi_img.expression(
            "b() * pdsi_coef / 2 + spi180d* spi180d_coef + spi1y * spi1y_coef + spi2y * spi2y_coef + spi5y * spi5y_coef",{
                "spi180d": ltb_spi180d_img, 
                "spi1y": ltb_spi1y_img, 
                "spi2y": ltb_spi2y_img, 
                "spi5y": ltb_spi5y_img,
                "spi180d_coef": ltb_spi180d_coef, 
                "spi1y_coef": ltb_spi1y_coef, 
                "spi2y_coef": ltb_spi2y_coef,
                "spi5y_coef": ltb_spi5y_coef, 
                "pdsi_coef": ltb_pdsi_coef})
        return ltblend.addBands(stblend).select([0,1], [ltb_variable, stb_variable]).copyProperties(img, property_list)
    
    # Map function to calculate drought blend
    # Filter for dates without NAs for the long term blend
    out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).map(generate_gm_drought_imgs)
    
    # Convert Image Collection to multi-band image
    out_i = out_ic.toBands()
    
    # Select variable to serve as input
    out_i = out_i.select(['[0-9]{8}_' + var_name])
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))
    
    return(out_i)


def define_parameters(level):
    """Define parameters by level
    :param level: level of analysis ['allotment', 'fieldoffice', 'districtoffice', 'stateoffice']
    :return: in_fc, in_fc_id, land_unit_long, land_unit_short, tile_scale, mask, mask_path parameters for running exports
    """

    # ----- Define mask, if applicable -----
    # For BLM, we will apply mask to field offices, district offices, and state offices, but not to allotments
    # Apply mask for ownership, landcover, or other variables. Must be binary mask.
    mask = False
    mask_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-sma-binary'

    # ----- Define additional parameters required for functions and output properties below -----
    # Define land unit names
    if(level == 'allotment'):
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-grazing-allotment-polygons'
        land_unit_long = 'BLM_Natl_Grazing_Allotment_Polygons'
        land_unit_short = 'BLM_Allotments'
        tile_scale = 1
        in_fc_id = 'ALLOT_ID'
    elif(level == 'fieldoffice'):
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-fieldoffice-polygons'
        land_unit_long = 'BLM_Natl_FieldOffice_Polygons'
        land_unit_short = 'BLM_FieldOffices'
        tile_scale = 1
        in_fc_id = 'FO_ID'
    elif(level == 'districtoffice'):
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-districtoffice-polygons'
        land_unit_long = 'BLM_Natl_DistrictOffice_Polygons'
        land_unit_short = 'BLM_DistrictOffices'
        tile_scale = 1
        in_fc_id = 'DO_ID'
    elif(level == 'stateoffice'):
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-stateoffice-polygons'
        land_unit_long = 'BLM_Natl_StateOffice_Polygons'
        land_unit_short = 'BLM_StateOffices'
        tile_scale = 1
        in_fc_id = 'SO_ID'

    in_fc = ee.FeatureCollection(in_fc_path)

    return in_fc, in_fc_path, in_fc_id, land_unit_long, land_unit_short, tile_scale, mask, mask_path