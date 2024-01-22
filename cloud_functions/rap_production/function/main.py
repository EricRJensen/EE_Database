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
    """Earth Engine export
    """
    # Define time period to export
    start_date = datetime.datetime(1980, 1, 1)
    end_date = datetime.datetime(2050, 1, 1)

    # Define input Image Collection
    in_ic_name = 'RAP_Production'
    in_ic_paths = ['projects/rap-data-365417/assets/npp-partitioned-v3']
    in_ic = ee.ImageCollection(in_ic_paths[0])
    in_ic_res = ee.Number(in_ic.first().projection().nominalScale()).round().getInfo()

    # Loop over land unit types
    fc_types  = ['allotment', 'fieldoffice', 'districtoffice', 'stateoffice']

    for fc_type in fc_types:

        in_fc, in_fc_path, in_fc_id, land_unit_long, land_unit_short, tile_scale, mask, mask_path = define_parameters(fc_type)

        # Define variable from Image Collection
        var_dict = {'afgAGB': {'units': 'lbs/acre'},
                    'pfgAGB': {'units': 'lbs/acre'},
                    'shrAGB': {'units': 'lbs/acre'},
                    'herbaceousAGB': {'units': 'lbs/acre'}}
        
        # Loop over variables
        var_names = ['afgAGB', 'pfgAGB', 'shrAGB', 'herbaceousAGB']

        for var_name in var_names:

            var_type = 'Continuous'
            var_units = var_dict.get(var_name).get('units')

            out_path = f"projects/climate-engine-pro/assets/blm-database/{land_unit_short.replace('_', '').lower()}-{in_ic_name.replace('_', '').lower()}-{var_name.replace('_', '').lower()}"
        
            # Get list of all dates
            all_dates = ee.ImageCollection('projects/rap-data-365417/assets/npp-partitioned-16day-v3').filterDate(start_date, end_date).aggregate_array('system:time_start').getInfo()

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
                in_i = preprocess_rap(in_ic_paths = in_ic_paths, var_name = properties.get('var_name'), date = date)

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


# Function to preprocess RAP data 
def preprocess_rap(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Function to convert NPP to aboveground biomass
    def rap_annual_biomass_function(img):

        year = ee.Date(img.get('system:time_start')).format('YYYY')
        matYear = ee.ImageCollection("projects/rap-data-365417/assets/gridmet-MAT").filterDate(year).first()
        fANPP = (matYear.multiply(0.0129)).add(0.171).rename('fANPP')

        # NPP scalar, KgC to lbsC, m2 to acres, fraction of NPP aboveground, C to biomass
        agb = img.multiply(0.0001).multiply(2.20462).multiply(4046.86).multiply(fANPP).multiply(2.1276)\
            .rename(['afgAGB', 'pfgAGB', 'shrAGB'])\
            .copyProperties(img, ['system:time_start'])\
            .set('year', year)
            
        herbaceous = ee.Image(agb).reduce(ee.Reducer.sum()).rename(['herbaceousAGB'])

        agb = ee.Image(agb).addBands(herbaceous)

        return(agb)
    
    # Define function to preprocess RAP NPP 16-day
    def rap_16day_biomass_function(image):

        # Import GridMET MAT collection
        mat = ee.ImageCollection("projects/rap-data-365417/assets/gridmet-MAT")

        # Use previous year's MAT to partition between above and below-ground for current year's provisional
        mat_last = ee.Image(mat.toList(mat.size()).get(mat.size().subtract(1)))
        mat_last = mat_last\
            .set("system:time_start", ee.Date(mat_last.get("system:time_start")).advance(1, 'year').millis())\
            .set("system:index", ee.Date(mat_last.get("system:time_start")).advance(1, 'year').format("YYYY"))
        mat = ee.ImageCollection(mat.toList(mat.size()).add(mat_last))

        # Select current MAT to partition between above and below-ground
        year = ee.Date(image.get('system:time_start')).format('YYYY')
        matYear = mat.filterDate(year).first()
        fANPP = (matYear.multiply(0.0129)).add(0.171).rename('fANPP')

        # Apply scaling and partitioning factors
        agb = image.multiply(0.0001)\
            .multiply(2.20462)\
            .multiply(4046.86)\
            .multiply(fANPP)\
            .multiply(2.1276)\
            .rename(['afgAGB', 'pfgAGB', 'shrAGB'])\
            .copyProperties(image, ['system:time_start'])\
            .set('year', year)

        herbaceous = ee.Image(agb).reduce(ee.Reducer.sum()).rename(['herbaceousAGB'])
        agb = ee.Image(agb).addBands(herbaceous)
        return agb
    
    if in_ic_paths[0] == 'projects/rap-data-365417/assets/vegetation-cover-v3':
        
        # Read-in rap image collection
        in_ic = ee.ImageCollection(in_ic_paths[0])
        
        # Filter for collection for images in date range and select variable of interest
        out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).select(var_name)
        
        # Convert Image Collection to multi-band image
        out_i = out_ic.toBands()
        
        # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
        def replace_name(name):
            return ee.String(name).replace(var_name, '').replace('_', '0101')
        
        # Finish cleaning input image
        out_i = out_i.rename(out_i.bandNames().map(replace_name))

        return(out_i)
    
    elif in_ic_paths[0] == 'projects/rap-data-365417/assets/npp-partitioned-v3':
        
        # Read-in rap image collection and map function over bands
        in_ic = ee.ImageCollection(in_ic_paths[0]).select(['afgNPP', 'pfgNPP', 'shrNPP']).map(rap_annual_biomass_function)

        # Filter for collection for images in date range and select variable of interest
        out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).select(var_name)

        # Convert Image Collection to multi-band image
        out_i = out_ic.toBands()

        # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
        def replace_name(name):
            return ee.String(name).replace(var_name, '').replace('_', '0101')
        
        # Finish cleaning input image
        out_i = out_i.rename(out_i.bandNames().map(replace_name))

        return(out_i)
    
    elif in_ic_paths[0] == 'projects/rap-data-365417/assets/npp-partitioned-16day-v3':

        # Read in provisional data
        prov_ic = ee.ImageCollection('projects/rap-data-365417/assets/npp-partitioned-16day-v3-provisional').select(['afgNPP', 'pfgNPP', 'shrNPP'])

        # Read-in rap image collection 
        in_ic = ee.ImageCollection(in_ic_paths[0]).select(['afgNPP', 'pfgNPP', 'shrNPP'])
        
        # Merge filter by system:time_start and map function over bands
        merged_ic = in_ic.merge(prov_ic).filter(ee.Filter.eq('system:time_start', date)).map(rap_16day_biomass_function)

        # Filter for dates without NAs for the long term blend
        out_ic = merged_ic.select(var_name)
    
        # Convert Image Collection to multi-band image
        out_i = out_ic.toBands()
    
        # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
        def replace_name(name):
            date_str = ee.String(name).replace(var_name, '').replace('_', '').replace('_', '')
            date_str = date_str.slice(-7)
            str_date = ee.Date.parse('YYYYD', date_str).format('YYYYMMdd')
            return str_date
    
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
    mask_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-sma-binary'

    # ----- Define additional parameters required for functions and output properties below -----
    # Define land unit names
    if(level == 'allotment'):
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-grazing-allotment-polygons'
        land_unit_long = 'BLM_Natl_Grazing_Allotment_Polygons'
        land_unit_short = 'BLM_Allotments'
        tile_scale = 1
        in_fc_id = 'ALLOT_ID'
        mask = False
    elif(level == 'fieldoffice'):
        mask = True
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-fieldoffice-polygons'
        land_unit_long = 'BLM_Natl_FieldOffice_Polygons'
        land_unit_short = 'BLM_FieldOffices'
        tile_scale = 1
        in_fc_id = 'FO_ID'
        mask = True
    elif(level == 'districtoffice'):
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-districtoffice-polygons'
        land_unit_long = 'BLM_Natl_DistrictOffice_Polygons'
        land_unit_short = 'BLM_DistrictOffices'
        tile_scale = 1
        in_fc_id = 'DO_ID'
        mask = True
    elif(level == 'stateoffice'):
        in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-stateoffice-polygons'
        land_unit_long = 'BLM_Natl_StateOffice_Polygons'
        land_unit_short = 'BLM_StateOffices'
        tile_scale = 1
        in_fc_id = 'SO_ID'
        mask = True

    in_fc = ee.FeatureCollection(in_fc_path)

    return in_fc, in_fc_path, in_fc_id, land_unit_long, land_unit_short, tile_scale, mask, mask_path