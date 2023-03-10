from datetime import date
from datetime import timedelta
import os
import datetime
import pandas as pd
import ee
import geemap

def do_something():
    return "This is the do something method"

# Apply function to select ID column and convert the ID string to numeric
def generate_id_i(in_fc, in_fc_id, land_unit, in_fc_path, in_ic_path, in_var_type, var_name):

    # Function to select ID band
    def select_id(f):
        return(f.select([in_fc_id]).set(in_fc_id, ee.Number.parse(ee.Feature(f.get(in_fc_id)))))
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
            .set("land-unit", land_unit)\
            .set("in-fc-path", in_fc_path)\
            .set("in-fc-id", in_fc_id)\
            .set("in-ic-path", in_ic_path)\
            .set("in-var-type", in_var_type)\
            .set("var-name", var_name)

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
def pts_to_img_continuous(in_fc, out_fc, land_unit, in_fc_path, in_fc_id, in_ic_path, in_var_type, var_name):
    
    # Cast to FeatureCollections
    fc = ee.FeatureCollection(in_fc)
    
    # Get list of properties to iterate over for creating multiband image for each date
    props = fc.first().propertyNames().remove('system:index')
    
    # Function to generate image from stats stored in Feature Collection property
    def generate_stat_image(prop):
        img = fc.reduceToImage(properties = [prop], reducer = ee.Reducer.sum()).rename([prop])
        return(img)
    
    # Generate multi-band stats image
    img_mb = ee.ImageCollection(props.map(generate_stat_image)).toBands()\
            .rename(props)\
            .set("system:index", out_fc.get('f_id'))\
            .set("land-unit", land_unit)\
            .set("in-fc-path", in_fc_path)\
            .set("in-fc-id", in_fc_id)\
            .set("in-ic-path", in_ic_path)\
            .set("in-var-type", in_var_type)\
            .set("var-name", var_name)
    
    return(img_mb)


# Export ID image to new Image Collection
def export_img(out_i_date, out_path, out_fc, var_name_exp, in_date):
    task = ee.batch.Export.image.toAsset(
        image = out_i_date,
        description = f'Initialize - {var_name_exp} - {in_date}',
        assetId = f'{out_path}/{in_date}',
        region = out_fc.geometry().buffer(20),
        scale = 22.264,
        maxPixels = 1e13)
    task.start()