import datetime
import logging
import ee
import google.auth

logging.basicConfig(level=logging.INFO)
logging.info("Starting gridMET Drought continuous export tasks to BLM database")


def handle_event(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        event (dict): Event payload.
    """

    # Need to authenticate using instructions here: https://google-auth.readthedocs.io/en/master/reference/google.auth.html
    credentials, project_id = google.auth.default()

    logging.info('Initializing GEE using application default credentials')
    ee.Initialize(credentials, project=project_id)

    ee.data.setWorkloadTag('ce-blm-database-gridmet-drought-cont')

    run_export()


def run_export():
    """Earth Engine export function
    """
    # Define time period to export
    start_date = datetime.datetime(1980, 1, 1)
    end_date = datetime.datetime(2050, 1, 1)

    # Define input Image Collection
    in_ic_name = 'GridMET_Drought_Cont'
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
            all_dates = ee.ImageCollection('GRIDMET/DROUGHT').filterDate(start_date, end_date).aggregate_array('system:time_start').getInfo()

            # Get list of dates from collection
            coll_dates = ee.ImageCollection(out_path).aggregate_array('system:time_start').distinct().getInfo()

            # Get list of dates missing from collection and filter out dates before 1986
            miss_dates = sorted(set(all_dates) - set(coll_dates))
            miss_dates = [i for i in miss_dates if i >= 504982643000]

            for date in miss_dates:

                logging.info("Running ", datetime.datetime.fromtimestamp(date/1000.0), ' for ', land_unit_short, ' ', var_name)

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
                out_fc = img_to_pts_continuous(in_i = in_i, in_fc = in_fc, tile_scale = properties.get('tile_scale'))

                # Convert centroid time-series to image collection time-series
                out_i = pts_to_img_continuous(in_fc = out_fc)

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


def img_to_pts_continuous(in_i, in_fc, tile_scale):
    """
    :param in_i: e.g. Image for single date
    :param in_fc: e.g. ee.FeatureCollection(in_fc_path)
    :return: Earth Engine image of pixels at the equator with bands for percentiles and mean
    """
    # Cast input image to ee.Image
    img = ee.Image(in_i)
    
    # Get resolution of the image
    res = img.select(0).projection().nominalScale()

    # Conditionally convert polygon to point if smaller than area of pixel
    def smallpolygons_to_points(f):
        
        f = ee.Feature(f)
        f = ee.Feature(ee.Algorithms.If(f.area(100).gte(res.pow(2).multiply(2)), f, f.centroid()))
        return(f)
    
    in_fc = in_fc.map(smallpolygons_to_points)
    
    # Run reduce regions for allotments and select only the columns with reducers
    img_rr = img.reduceRegions(collection = in_fc, reducer = ee.Reducer.percentile([5, 25, 50, 75, 95])\
                                .combine(reducer2 = ee.Reducer.mean(), sharedInputs = True),\
                                scale = res,\
                                tileScale = tile_scale).select(['mean', 'p.*'])
    
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


def pts_to_img_continuous(in_fc):
    """
    :param in_fc: e.g. Output of .img_to_pts_continuous()
    :param properties: e.g. {'land-unit': land_unit, 'in-fc-path': in_fc_path, "in-fc-id": in_fc_id, "in-ic-paths": in_ic_path, "var-type": var_type, "var-name": var_name}
    :return: Earth Engine image of pixels at the equator with values for land unit ID
    """
    # Cast to FeatureCollections
    fc = ee.FeatureCollection(in_fc)
    
    # Get list of properties to iterate over for creating multiband image for each date
    props = fc.first().propertyNames().remove('system:index')
    
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