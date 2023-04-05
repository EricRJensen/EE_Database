from datetime import date
from datetime import timedelta
import pandas as pd
import ee


# Function to calculate short-term and long-term blends
def preprocess_gm_drought(in_ic_paths, var_name, start_date, end_date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param start_date: e.g. datetime.datetime(2022, 1, 1)
    :param end_date: e.g. datetime.datetime(2022, 5, 1)
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in gridmet drought image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    def generate_gm_drought_imgs(img):

        # Define property list
        property_list = ["system:index", "system:time_start"]
        
        # Define preliminary variables for short-term blend calculation
        stb_variable = "short_term_drought_blend"
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
        ltb_variable = "long_term_drought_blend"
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
    out_ic = in_ic.filterDate(start_date, end_date).map(generate_gm_drought_imgs)
    
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

# Function to preprocess GridMET
def preprocess_gm(in_ic_paths, var_name, start_date, end_date, aggregation_days):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param start_date: e.g. datetime.datetime(2022, 1, 1)
    :param end_date: e.g. datetime.datetime(2022, 5, 1)
    :param aggregation_days: e.g. aggregation period for the specified variable
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in gridmet image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])
    
    # Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filterDate(start_date, end_date)
    
    # Get GridMET drought dates to match temporal cadence to
    gm_drought = ee.ImageCollection("GRIDMET/DROUGHT").filterDate(start_date, end_date)
    gm_drought_dates = gm_drought.aggregate_array('system:time_start')

    # Function to aggregate statistics over the day range based on aggregation_days arg
    def aggregate_over_dates(date):
        
        # Filter for next five days
        date = ee.Date(date)
        out_ic_aggregate = out_ic.filterDate(date, date.advance(5, 'day'))
    
        # Aggregate variables over that time
        pr_img = out_ic_aggregate.select('pr').reduce(ee.Reducer.sum()).rename(['pr'])
        tmmn_img = out_ic_aggregate.select('tmmn').reduce(ee.Reducer.mean()).rename(['tmmn'])
        tmmx_img = out_ic_aggregate.select('tmmx').reduce(ee.Reducer.mean()).rename(['tmmx'])
        eto_img = out_ic_aggregate.select('eto').reduce(ee.Reducer.sum()).rename(['eto'])
        vpd_img = out_ic_aggregate.select('vpd').reduce(ee.Reducer.mean()).rename(['vpd'])
    
        return(pr_img.addBands(tmmn_img).addBands(tmmx_img).addBands(eto_img).addBands(vpd_img)\
                .set('system:time_start', date)\
                .set('system:index', date.format('YYYYMMdd')))

    # Apply aggregation function and convert to multiband image
    out_ic = ee.ImageCollection(gm_drought_dates.map(aggregate_over_dates)).select(var_name)
    out_i = out_ic.toBands()

    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))

    return(out_i)


# Function to preprocess RAP data 
def preprocess_rap(in_ic_paths, var_name, start_date, end_date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param start_date: e.g. datetime.datetime(2022, 1, 1)
    :param end_date: e.g. datetime.datetime(2022, 5, 1)
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    if in_ic_paths[0] == 'projects/rangeland-analysis-platform/vegetation-cover-v3':
        
        # Read-in rap image collection
        in_ic = ee.ImageCollection(in_ic_paths[0])
        
        # Filter for collection for images in date range and select variable of interest
        out_ic = in_ic.filterDate(start_date, end_date).select(var_name)
        
        # Convert Image Collection to multi-band image
        out_i = out_ic.toBands()
        
        # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
        def replace_name(name):
            return ee.String(name).replace(var_name, '').replace('_', '0101')
        
        # Finish cleaning input image
        out_i = out_i.rename(out_i.bandNames().map(replace_name))

        return(out_i)
    
    elif in_ic_paths[0] == 'projects/rangeland-analysis-platform/npp-partitioned-v3':

        # Function to convert NPP to aboveground biomass
        def production_conversion(img):

            year = ee.Date(img.get('system:time_start')).format('YYYY')
            matYear = ee.ImageCollection("projects/rangeland-analysis-platform/gridmet-MAT").filterDate(year).first()
            fANPP = (matYear.multiply(0.0129)).add(0.171).rename('fANPP')

            # NPP scalar, KgC to lbsC, m2 to acres, fraction of NPP aboveground, C to biomass
            agb = img.multiply(0.0001).multiply(2.20462).multiply(4046.86).multiply(fANPP).multiply(2.1276)\
                .rename(['afgAGB', 'pfgAGB'])\
                .copyProperties(img, ['system:time_start'])\
                .set('year', year)
            
            herbaceous = ee.Image(agb).reduce(ee.Reducer.sum()).rename(['herbaceousAGB'])

            agb = ee.Image(agb).addBands(herbaceous)

            return(agb)
        
        # Read-in rap image collection and map function over bands
        in_ic = ee.ImageCollection(in_ic_paths[0]).select(['afgNPP', 'pfgNPP']).map(production_conversion)

        # Filter for collection for images in date range and select variable of interest
        out_ic = in_ic.filterDate(start_date, end_date).select(var_name)
        
        # Convert Image Collection to multi-band image
        out_i = out_ic.toBands()
        
        # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
        def replace_name(name):
            return ee.String(name).replace(var_name, '').replace('_', '0101')
        
        # Finish cleaning input image
        out_i = out_i.rename(out_i.bandNames().map(replace_name))

        return(out_i)
    

# Function to preprocess usdm
def preprocess_usdm(in_ic_paths, var_name, start_date, end_date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param start_date: e.g. datetime.datetime(2022, 1, 1)
    :param end_date: e.g. datetime.datetime(2022, 5, 1)
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in usdm image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    #Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filterDate(start_date, end_date).select(var_name)

    #Filter for CONUS
    out_ic = out_ic.filter(ee.Filter.eq('region', 'conus'))
    
    # Convert Image Collection to multi-band image
    out_i = out_ic.toBands()
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '').replace('conus_','')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))
    
    return(out_i)


# Function to preprocess MODIS TERRA NET ET 16-day
def preprocess_modet(in_ic_paths, var_name, start_date, end_date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param start_date: e.g. datetime.datetime(2022, 1, 1)
    :param end_date: e.g. datetime.datetime(2022, 5, 1)
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in MODIS ET image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    #Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filterDate(start_date, end_date).select(var_name)
    
    # Convert Image Collection to multi-band image
    out_i = out_ic.toBands()
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '').replace('_', '').replace('_', '')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))
    
    return(out_i)

# Function to preprocess MODIS LST
def preprocess_modlst(in_ic_paths, var_name, start_date, end_date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param start_date: e.g. datetime.datetime(2022, 1, 1)
    :param end_date: e.g. datetime.datetime(2022, 5, 1)
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in MODIS LST image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    #Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filterDate(start_date, end_date).select(var_name)
    
    # Convert Image Collection to multi-band image
    out_i = out_ic.toBands()
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '').replace('_', '').replace('_', '')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))
    
    return(out_i)