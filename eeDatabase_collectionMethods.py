import ee


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


# Function to preprocess GridMET
def preprocess_gm(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Get GridMET drought dates to match temporal cadence to
    gm_drought = ee.ImageCollection("GRIDMET/DROUGHT").filter(ee.Filter.eq('system:time_start', date))
    gm_drought_dates = gm_drought.aggregate_array('system:time_start')

    # Read-in gridmet image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    # Function to aggregate statistics over the day range from GridMET drought
    def aggregate_over_dates(date):
        
        # Filter for previous five days
        date = ee.Date(date)
        in_ic_aggregate = in_ic.filterDate(date.advance(-5, 'day'), date)
    
        # Aggregate variables over that time and convert temperature to celsius
        pr_img = in_ic_aggregate.select('pr').reduce(ee.Reducer.sum()).rename(['precip'])
        tmmn_img = in_ic_aggregate.select('tmmn').reduce(ee.Reducer.mean()).rename(['tmmn']).subtract(273.15)
        tmmx_img = in_ic_aggregate.select('tmmx').reduce(ee.Reducer.mean()).rename(['tmmx']).subtract(273.15)
        eto_img = in_ic_aggregate.select('eto').reduce(ee.Reducer.sum()).rename(['eto'])
        vpd_img = in_ic_aggregate.select('vpd').reduce(ee.Reducer.mean()).rename(['vpd'])
        wind_img = in_ic_aggregate.select('vs').reduce(ee.Reducer.mean()).rename(['windspeed'])
        solar_img = in_ic_aggregate.select('srad').reduce(ee.Reducer.mean()).rename(['srad'])
    
        return(pr_img.addBands(tmmn_img).addBands(tmmx_img).addBands(eto_img).addBands(vpd_img).addBands(wind_img).addBands(solar_img)\
                .set('system:time_start', date)\
                .set('system:index', date.format('YYYYMMdd')))

    # Apply aggregation function and convert to multiband image
    out_ic = ee.ImageCollection(gm_drought_dates.map(aggregate_over_dates)).select(var_name)
    out_i = out_ic.toBands()

    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '')
    
    # Finish cleaning input image and apply projection
    out_i = out_i.rename(out_i.bandNames().map(replace_name)).setDefaultProjection(in_ic.first().projection())

    return(out_i)


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


# Function to preprocess usdm
def preprocess_usdm(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in usdm image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    # Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).select(var_name)

    # Filter for CONUS
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
def preprocess_modet(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in MODIS ET image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    #Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).select(var_name)
    
    # Convert Image Collection to multi-band image
    out_i = out_ic.toBands()
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '').replace('_', '').replace('_', '')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))
    
    return(out_i)


# Function to preprocess MODIS LST
def preprocess_modlst(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in MODIS LST image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    #Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).select(var_name)
    
    # Convert Image Collection to multi-band image
    out_i = out_ic.toBands()
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('_', '').replace('_', '').replace('_', '')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))

    # Apply scaling and convert from kelvin to celsius
    out_i = out_i.multiply(0.02).subtract(273.15)
    
    return(out_i)


# Function to preprocess ls ndvi median composites
def preprocess_lsndvi(in_ic_paths, var_name, date, in_fc):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :param in_fc: input feature collection for generating bounding box (convex hull)
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Property List
    property_list = ["system:index", "system:time_start"]

    # Get RAP dates to match temporal cadence to
    rap_16day = ee.ImageCollection("projects/rap-data-365417/assets/npp-partitioned-16day-v3").merge(ee.ImageCollection('projects/rap-data-365417/assets/npp-partitioned-16day-v3-provisional')).filter(ee.Filter.eq('system:time_start', date))
    date = rap_16day.aggregate_array('system:time_start').get(0)

    # Unpack paths
    ic5,ic7,ic8,ic9 = in_ic_paths

    # Generate bounding box
    def bbox(f):
        return(f.simplify(1000))
    in_fc_bbox = ee.FeatureCollection(in_fc.map(bbox)).geometry().convexHull(10)
    
    #Create image collections from paths
    ic5_ic = ee.ImageCollection(ic5).filterBounds(in_fc_bbox).filterDate(date, ee.Date(date).advance(16, 'day'))
    ic7_ic = ee.ImageCollection(ic7).filterBounds(in_fc_bbox).filterDate(date, ee.Date(date).advance(16, 'day'))
    ic8_ic = ee.ImageCollection(ic8).filterBounds(in_fc_bbox).filterDate(date, ee.Date(date).advance(16, 'day'))
    ic9_ic = ee.ImageCollection(ic9).filterBounds(in_fc_bbox).filterDate(date, ee.Date(date).advance(16, 'day'))

    # Define processing functions 
    # CloudMask
    def landsat_qa_pixel_cloud_mask_func(img):
        """
        Apply collection 2 CFMask cloud mask to a daily Landsat SR image
        https://prd-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/atoms/files/LSDS-1328_Landsat8-9-OLI-TIRS-C2-L2-DFCB-v6.pdf
        :param img: Earth Engine Image
        :return: Earth Engine Image
        """
        qa_img = img.select(["QA_PIXEL"])
        cloud_mask = (
            qa_img.rightShift(3).bitwiseAnd(1).neq(0)
            # cloud confidence
            # .And(qa_img.rightShift(8).bitwiseAnd(3).gte(cloud_confidence))
            # cirrus
            .Or(qa_img.rightShift(2).bitwiseAnd(1).neq(0))
            # shadow
            .Or(qa_img.rightShift(4).bitwiseAnd(1).neq(0))
            # snow
            .Or(qa_img.rightShift(5).bitwiseAnd(1).neq(0))
            # dilate
            .Or(qa_img.rightShift(1).bitwiseAnd(1).neq(0))
        )
        return img.updateMask(cloud_mask.Not())

    # Radsat Mask
    def landsat_qa_pixel_radsat_mask_func(img):
        """
        Apply collection 2 RADSAT mask to a daily Landsat SR image
        This function can be applied to Landsat 1-5, 7, 8, and 9
        https://www.usgs.gov/landsat-missions/landsat-collection-2-quality-assessment-bands
        :param img: Earth Engine Image
        :return: Earth Engine Image
        """
        qa_img = img.select(["QA_RADSAT"])
        radsat_mask = (
            # Band 1
            qa_img.rightShift(0).bitwiseAnd(1).neq(0)
            # Band 2
            .Or(qa_img.rightShift(1).bitwiseAnd(1).neq(0))
            # Band 3
            .Or(qa_img.rightShift(2).bitwiseAnd(1).neq(0))
            # Band 4
            .Or(qa_img.rightShift(3).bitwiseAnd(1).neq(0))
            # Band 5
            .Or(qa_img.rightShift(4).bitwiseAnd(1).neq(0))
            # Band 6
            .Or(qa_img.rightShift(5).bitwiseAnd(1).neq(0))
            # Band 7
            .Or(qa_img.rightShift(6).bitwiseAnd(1).neq(0))
        )
        return img.updateMask(radsat_mask.Not())

    # Band Functions
    def landsat5_sr_band_func(img):
        """
        Rename Landsat 4 and 5 bands to common band names
        Scale reflectance values by 0.0000275 then offset by -0.2
        :param img: Earth Engine Image
        :return: Earth Engine Image
        """
        return (
            ee.Image(img)
            .select(["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7", "ST_B6"],
                    ["blue", "green", "red", "nir", "swir1", "swir2", "LST_Day_1km"],)
            .multiply([0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.00341802])
            .add([-0.2, -0.2, -0.2, -0.2, -0.2, -0.2, 149.0])
            .addBands(img.select(["QA_PIXEL"], ["QA_PIXEL"]))
            .copyProperties(img, property_list)
        )

    def landsat7_sr_band_func(img):
        """
        Change band order to match Landsat 8
        For now, don't include pan-chromatic or high gain thermal band
        Scale reflectance values by 0.0000275 then offset by -0.2
        :param img: Earth Engine Image
        :return:Earth Engine Image
        """
        return (
            ee.Image(img)
            .select(["SR_B1", "SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B7", "ST_B6"],
                    ["blue", "green", "red", "nir", "swir1", "swir2", "LST_Day_1km"],)
            .multiply([0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.00341802])
            .add([-0.2, -0.2, -0.2, -0.2, -0.2, -0.2, 149.0])
            .addBands(img.select(["QA_PIXEL"], ["QA_PIXEL"]))
            .copyProperties(img, property_list)
        )

    def landsat8_sr_band_func(img):
        """
        Rename Landsat 8 and 9 bands to common band names
        For now, don't include coastal, cirrus, or pan-chromatic
        Scale reflectance values by 0.0000275 then offset by -0.2
        :param img: Earth Engine Image
        :return: Earth Engine Image
        """
        return (
            ee.Image(img)
            .select(["SR_B2", "SR_B3", "SR_B4", "SR_B5", "SR_B6", "SR_B7", "ST_B10"],
                    ["blue", "green", "red", "nir", "swir1", "swir2", "LST_Day_1km"],)
            .multiply([0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.0000275, 0.00341802])
            .add([-0.2, -0.2, -0.2, -0.2, -0.2, -0.2, 149.0])
            .addBands(img.select(["QA_PIXEL"], ["QA_PIXEL"]))
            .copyProperties(img, property_list)
        )
    
    def ndvi_func(image):
        ndvi = image.normalizedDifference(['nir', 'red']).rename('NDVI');
        return image.addBands(ndvi);
    
    # Apply processing functions 
    out_ic5 = ic5_ic.map(landsat_qa_pixel_cloud_mask_func).map(landsat_qa_pixel_radsat_mask_func).map(landsat5_sr_band_func).map(ndvi_func)
    out_ic7 = ic7_ic.map(landsat_qa_pixel_cloud_mask_func).map(landsat_qa_pixel_radsat_mask_func).map(landsat7_sr_band_func).map(ndvi_func)
    out_ic8 = ic8_ic.map(landsat_qa_pixel_cloud_mask_func).map(landsat_qa_pixel_radsat_mask_func).map(landsat8_sr_band_func).map(ndvi_func)
    out_ic9 = ic9_ic.map(landsat_qa_pixel_cloud_mask_func).map(landsat_qa_pixel_radsat_mask_func).map(landsat8_sr_band_func).map(ndvi_func)

    # Merge LS SR Image Collections
    collection = ee.ImageCollection([])
    collection = collection.merge(out_ic5)
    collection = collection.merge(out_ic7)
    collection = collection.merge(out_ic8)
    collection = collection.merge(out_ic9)
    out_ic = ee.ImageCollection(collection).select(var_name)

    # Define function to generate median NDVI composites
    def collection_maker_16day(date):
        date = ee.Date(date)
        date_range =  out_ic.filterDate(date,date.advance(16, 'day'))
        ndviMedian = date_range.reduce(ee.Reducer.median()).rename(['NDVI'])
        return (ndviMedian.set('system:time_start', date).set('system:index', date.format('YYYYMMdd'))).setDefaultProjection(out_ic.first().projection())

    # Apply function to dates list
    collection = ee.ImageCollection(ee.List([date]).map(collection_maker_16day)).select(var_name)
    
    # Convert Image Collection to multi-band image
    out_i = collection.toBands()
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        date_str = ee.String(name).replace(var_name, '').replace('_', '')
        return date_str

    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name))

    return(out_i)


# Function to preprocess MTBS
def preprocess_mtbs(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in mtbs image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    # Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).select(var_name)
    
    # Convert Image Collection to multi-band image
    out_i = out_ic.toBands()
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(var_name, '').replace('mtbs_mosaic_', '').replace('_', '').cat('0101')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name)).unmask()
    
    return(out_i)


# Function to preprocess VegDRI
def preprocess_vegdri(in_ic_paths, var_name, date):
    """
    :param in_ic_paths: e.g. ['GRIDMET/DROUGHT'] or ['projects/rangeland-analysis-platform/vegetation-cover-v3']
    :param var_name: e.g. 'NDVI', 'long_term_drought_blend', 'tmmn'
    :param date: e.g. system:time_start in milliseconds since Unix epoch
    :return: Earth Engine time-series image with dates (YYYYMMDD) as bands
    """
    # Read-in VegDRI image collection
    in_ic = ee.ImageCollection(in_ic_paths[0])

    # Filter for collection for images in date range and select variable of interest
    out_ic = in_ic.filter(ee.Filter.eq('system:time_start', date)).select(var_name)

    # Function to scale VegDRI values to PDSI range
    def normalize_vegdri(img):
        return img.addBands(img.subtract(128).divide(16).rename('vegdri_norm'))
    
    # Convert Image Collection to multi-band image
    out_i = normalize_vegdri(out_ic.toBands()).select(['vegdri_norm'], ['vegdri'])
    
    # Bandnames must be an eight digit character string 'YYYYMMDD'. Annual data will be 'YYYY0101'.
    def replace_name(name):
        return ee.String(name).replace(ee.String('_').cat(var_name), '')
    
    # Finish cleaning input image
    out_i = out_i.rename(out_i.bandNames().map(replace_name)).unmask()
    
    return(out_i)