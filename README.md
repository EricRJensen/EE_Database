# EE_Database
This repository is a Python module for use with Google Earth Engine for producing a novel Earth Engine pixel database to store zonal statistics from climate and remote sensing datasets for regions of interest. This database system has been implemented for the Bureau of Land Management (BLM) to support national-scale operational monitoring of all BLM state offices, district offices, field offices, and grazing allotments (more information at reports.climateengine.org and app.climateengine.org).

### Logic of module
There are four primary python scripts used for populating the database system:
1. eeDatabase_collectionMethods.py provides preprocessing functions for all datasets and variables currently enabled for the database. These functions take arguments of the path to the input Image Collection, the variable to run (band), and the date to run and return a single one-band image with the date of the image (YYYYMMDD) encoded as the band name. Dataset currently enabled include gridMET, gridMET Drought, RAP Cover, RAP Production, RAP 16-day Production, Landsat 5/7/8/9, US Drought Monitor, Monitoring Trends in Burn Severity, MODIS SSEBop ET, and MODIS LST).
2. eeDatebase_coreMethods.py provides functions for running zonal statistics for areas of interest over images. The functions handle categorical and continuous data uniquely, with categorical datasets storing histograms of pixels in different classes as bands in the output images and categorical datasets storing statistics that represent the distribution of values across the area of interest (mean, median and the 5th, 25th, 75th, and 95th percentiles). The outputs are an image collection with the first image encoding the ID with subsequent images representing each date in the timeseries with bands for the statistics described.
3. eeDatabase_collectionInfo.py is a series of dictionaries storing image collection and variable metadata.
4. Export_EEPixel_Timeseries_ImageCollection.ipynb is a notebook for populating the database using the scripts described above.

# Related modules
- BLM Reports module for generating real-time PDF/PNG Drought and Site Characterization Reports at reports.climateengine.org: https://github.com/Google-Drought/BLM_Reports
- BLM FeatureViews module for generating Earth Engine FeatureViews and Feature Collections for visualizing choropleth maps of current conditions, trends, anomalies, and other summaries

### Graphics
<img width="1229" alt="image" src="https://github.com/Google-Drought/EE_Database/assets/33233973/44970ffd-8068-4968-836e-47d8491933e5">

https://github.com/Google-Drought/EE_Database/assets/33233973/60c086a4-ecf6-4a8c-a9ca-009d64ebe861

