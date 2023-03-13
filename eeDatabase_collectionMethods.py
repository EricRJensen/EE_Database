from datetime import date
from datetime import timedelta
import os
import datetime
import pandas as pd
import ee
import geemap

def do_something():
    return "This is the do something method"

# Function to calculate short-term and long-term blends
def preprocess_gm_drought(in_ic, var_name, start_date, end_date):
    
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
# Function to preprocess RAP data 
def preprocess_rap(in_ic, var_name, start_date, end_date):

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