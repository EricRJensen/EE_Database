from datetime import date
from datetime import timedelta
import pandas as pd
import ee


# Apply function to select ID column and convert the ID string to numeric
def generate_id_i(in_fc, properties):

    # Function to select ID band
    def select_id(f):
        fc_id = properties.get('in-fc-id')
        return(f.select([fc_id]).set(fc_id, ee.Number.parse(f.get(fc_id))))
    in_fc = in_fc.map(select_id)

    # Convert feature collection to list
    in_fc_list = in_fc.toList(in_fc.size())

    # Get size of in_fc_list
    in_fc_size = in_fc_list.size()

    # Function to create feature collection next to equator
    def pts_to_equator(i):

        # Cast number to EE number
        i = ee.Number(i)
        
        # Get properties
        properties = ee.Feature(in_fc_list.get(i)).toDictionary()
        
        # Create geometry at equator
        geom = ee.Geometry.Point([i.multiply(0.0002), 0.0002])
        
        # Return object with properties
        return(ee.Feature(geom).set(properties))

    # Create equator feature collection
    out_fc = ee.FeatureCollection(ee.List.sequence(0, in_fc_size.subtract(1), 1).map(pts_to_equator)).set('f_id', 'id')

    # Get ID property name
    prop = out_fc.first().propertyNames().remove('system:index')

    # Reduce ID property to image
    id_i = out_fc.reduceToImage(properties = prop, reducer = ee.Reducer.mean())\
            .rename(['id'])\
            .set("system:index", out_fc.get('f_id'))\
            .set("land-unit", properties.get('land-unit'))\
            .set("in-fc-path", properties.get('in-fc-path'))\
            .set("in-fc-id", properties.get('in-fc-id'))\
            .set("in-ic-path", properties.get('in-ic-path'))\
            .set("var-type", properties.get('var-type'))\
            .set("var-name", properties.get('var-name'))

    # Return the id image and the points geometry for creating image export geometry
    return(ee.List([id_i, out_fc]))


# Function to return feature time-series as centroid feature collection for continuous variables
def img_to_pts_continuous(in_i, in_fc):
    
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
    
    # Function to create feature collection next to equator
    def pts_to_equator(i):
        
        # Cast number to EE number
        i = ee.Number(i)
        
        # Get properties
        properties = ee.Feature(img_rr_list.get(i)).toDictionary()
        
        # Create geometry at equator
        geom = ee.Geometry.Point([i.multiply(0.0002),0.0002])
        
        # Return object with properties
        return(ee.Feature(geom).set(properties))
    
    # Create equator feature collection
    equator_fc = ee.FeatureCollection(ee.List.sequence(0, img_rr_size.subtract(1), 1).map(pts_to_equator))
    
    return(equator_fc)


# Function to generate series image collection from feature collections
def pts_to_img_continuous(in_fc, properties):
    
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
            .rename(props)\
            .set("system:index", properties.get('in-date'))\
            .set("land-unit", properties.get('land-unit'))\
            .set("in-fc-path", properties.get('in-fc-path'))\
            .set("in-fc-id", properties.get('in-fc-id'))\
            .set("in-ic-path", properties.get('in-ic-path'))\
            .set("var-type", properties.get('var-type'))\
            .set("var-name", properties.get('var-name'))
    
    return(img_mb)

# Function to return feature time-series as centroid feature collection for continuous variables
def img_to_pts_categorical(in_i, in_fc):
    
    # Cast input image to ee.Image
    img = ee.Image(in_i)

    # Need to further pre-process drought blends to be able to extract bins consistent with drought.gov
    # There are no reducers that allow histogram bins with variable widths, so we have to put bins into categories to start
    # Reclassify drought blends using schema below
    # <-2.0 (D4) = 0
    # -2.0--1.5 (D3) = 1
    # -1.5--1.2 (D2) = 2
    # -1.2--0.7 (D1) = 3
    # -0.7--0.5 (D0) = 4
    # -0.5-0.5 (Neutral) = 5
    # 0.5-0.7 (W0) = 6
    # 0.7-1.2 (W1) = 7
    # 1.2-1.5 (W2) = 8
    # 1.5-2.0 (W3) = 9
    # >2.0 (W4) = 10
    # Reclassify drought blends
    # There needs to be logic here to handle "USDM" "long-term-blend" etc. differently
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
            key = ee.String(key).replace('.0','')
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
    
    # Function to create feature collection next to equator
    def pts_to_equator(i):
        
        # Cast number to EE number
        i = ee.Number(i)
        
        # Get properties
        properties = ee.Feature(img_rr_list.get(i)).toDictionary()
        
        # Create geometry at equator
        geom = ee.Geometry.Point([i.multiply(0.0002),0.0002])
        
        # Return object with properties
        return(ee.Feature(geom).set(properties))
    
    # Create equator feature collection
    equator_fc = ee.FeatureCollection(ee.List.sequence(0, img_rr_size.subtract(1), 1).map(pts_to_equator))
    
    return(equator_fc)


# Function to generate series image collection from feature collections
def pts_to_img_categorical(in_fc, properties):
    
    # Cast to FeatureCollections
    fc = ee.FeatureCollection(in_fc)

    blends_classes = ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10']
        
    # Get list of properties to iterate over for creating multiband image for each date
    props = ee.List(blends_classes)
    
    # Function to generate image from stats stored in Feature Collection property
    def generate_stat_image(prop):
        img = fc.reduceToImage(properties = [prop], reducer = ee.Reducer.sum()).rename([prop])
        return(img)
    
    # Generate multi-band stats image
    img_mb = ee.ImageCollection(props.map(generate_stat_image)).toBands()\
            .rename(props)\
            .set("system:index", properties.get('in-date'))\
            .set("land-unit", properties.get('land-unit'))\
            .set("in-fc-path", properties.get('in-fc-path'))\
            .set("in-fc-id", properties.get('in-fc-id'))\
            .set("in-ic-path", properties.get('in-ic-path'))\
            .set("var-type", properties.get('var-type'))\
            .set("var-name", properties.get('var-name'))
    
    return(img_mb)


# Export ID image to new Image Collection
def export_img(out_i, out_path, out_fc, var_name_exp, in_date):
    task = ee.batch.Export.image.toAsset(
        image = out_i,
        description = f'Append - {var_name_exp} - {in_date}',
        assetId = f'{out_path}/{in_date}',
        region = out_fc.geometry().buffer(20),
        scale = 22.264,
        maxPixels = 1e13)
    task.start()