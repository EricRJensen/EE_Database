from datetime import date
from datetime import timedelta
import pandas as pd
import ee


# Apply function to select ID column and convert the ID string to numeric
def generate_id_img(in_fc, in_fc_id):
    """
    :param in_fc: e.g. ee.FeatureCollection(in_fc_path)
    :param properties: e.g. {'land-unit': land_unit, 'in-fc-path': in_fc_path, "in-fc-id": in_fc_id, "in-ic-paths": in_ic_path, "var-type": var_type, "var-name": var_name}
    :return: Earth Engine image of pixels at the equator with values for land unit ID
    """
    # Function to select ID band
    def select_id(f):
        fc_id = in_fc_id
        return(f.select([fc_id]).set(fc_id, ee.Number.parse(f.get(fc_id))))
    in_fc = in_fc.map(select_id)

    # Convert feature collection to list
    in_fc_list = in_fc.toList(in_fc.size())

    # Get size of in_fc_list
    in_fc_size = in_fc_list.size()

    # Function to create feature collection next to equator during initialization
    def pts_to_equator_init(i):

        # Cast number to EE number
        i = ee.Number(i)
        
        # Get properties
        properties = ee.Feature(in_fc_list.get(i)).toDictionary()
        
        # Create geometry at equator
        geom = ee.Geometry.Point([i.multiply(0.0002), 0.0002])
        
        # Return object with properties
        return(ee.Feature(geom).set(properties))

    # Create equator feature collection
    out_fc = ee.FeatureCollection(ee.List.sequence(0, in_fc_size.subtract(1), 1).map(pts_to_equator_init)).set('f_id', 'id')

    # Get ID property name
    prop = out_fc.first().propertyNames().remove('system:index')

    # Reduce ID property to image
    id_i = out_fc.reduceToImage(properties = prop, reducer = ee.Reducer.mean()).rename('id')

    # Return the id image and the points geometry for creating image export geometry
    return(ee.List([id_i, out_fc]))


# Function to return feature time-series as centroid feature collection for continuous variables
def img_to_pts_continuous(in_i, in_fc):
    """
    :param in_i: e.g. Image for single date
    :param in_fc: e.g. ee.FeatureCollection(in_fc_path)
    :return: Earth Engine image of pixels at the equator with bands for percentiles and mean
    """
    # Cast input image to ee.Image
    img = ee.Image(in_i)
    
    # Get resolution of the image
    res = img.select(0).projection().nominalScale()
    
    # Run reduce regions for allotments and select only the columns with reducers
    img_rr = img.reduceRegions(collection = in_fc, reducer = ee.Reducer.percentile([5, 25, 50, 75, 95])\
                                .combine(reducer2 = ee.Reducer.mean(), sharedInputs = True),\
                                scale = res,\
                                tileScale = 16).select(['mean', 'p.*'])
    
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


# Function to generate series image collection from feature collections
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

# Function to return feature time-series as centroid feature collection for continuous variables
def img_to_pts_categorical(in_i, in_fc, in_ic_name):
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
            .where(img.gte(2.0), 10)
    
    # Maintain original values for USDM
    elif in_ic_name == "USDM":
        img = img
    
    # Maintain original values for MTBS
    elif in_ic_name == "MTBS":
        img = img
    
    # Get resolution of the image
    res = img.select(0).projection().nominalScale()
    
    # Run reduce regions for allotments and select only the columns with reducers
    img_rr = img.reduceRegions(collection = in_fc, reducer = ee.Reducer.frequencyHistogram(),\
                                scale = res,\
                                tileScale = 16).select(['histogram'])
    
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
    
    #Clean up histogram and set as properties
    img_rr = img_rr.map(process_histogram).select(['c.*'])

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


# Function to generate series image collection from feature collections
def pts_to_img_categorical(in_fc, in_ic_name):
    '''
    :param in_fc: e.g. output of img_to_pts_categorical
    :param properties: e.g. {'land-unit': land_unit, 'in-fc-path': in_fc_path, "in-fc-id": in_fc_id, "in-ic-paths": in_ic_path, "var-type": var_type, "var-name": var_name}
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


# Export ID image to new Image Collection
def export_img(out_i, out_region, out_path, out_id, properties):
    '''
    :param out_i: e.g. Image to export returned from .pts_to_img*()
    :param out_fc: e.g. Feature Collection at equator returned from .img_to_pts*()
    :param properties: e.g. {'land-unit': land_unit, 'in-fc-path': in_fc_path, "in-fc-id": in_fc_id, "in-ic-paths": in_ic_path, "var-type": var_type, "var-name": var_name}
    :return: Earth Engine image of pixels at the equator with bands for histogram bins
    '''

    # Define variables for export task
    var_name_exp = properties.get('var_name').replace('_', '').lower()
    in_ic_name_exp = properties.get('in_ic_name').replace('_', '').lower()
    land_unit_exp = properties.get('land_unit_short').replace('_', '').lower()

    # Queue and start export task
    task = ee.batch.Export.image.toAsset(
        image = out_i.set(properties),
        description = f'append - {land_unit_exp} {in_ic_name_exp} {var_name_exp} - {out_id}',
        assetId = f'{out_path}/{out_id}',
        region = out_region,
        scale = 22.264,
        maxPixels = 1e13)
    task.start()