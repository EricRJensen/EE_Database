# Define input Image Collection variables using dataset dictionary
in_ic_dict = {'GridMET_Drought': {'in_ic_paths': ['GRIDMET/DROUGHT'],
                                  'var_names': ['Long_Term_Drought_Blend', 'Short_Term_Drought_Blend'],
                                  'var_type': 'Categorical',
                                  'ic_mask': False},
            'GridMET_Drought_Cont': {'in_ic_paths': ['GRIDMET/DROUGHT'],
                                  'var_names': ['Long_Term_Drought_Blend', 'Short_Term_Drought_Blend'],
                                  'var_type': 'Continuous',
                                  'ic_mask': False},
            'GridMET': {'in_ic_paths': ['IDAHO_EPSCOR/GRIDMET'],
                        'var_names': ['precip', 'tmmn', 'tmmx', 'eto', 'vpd', 'windspeed', 'srad'],
                        'var_type': 'Continuous',
                        'ic_mask': False},
            'RAP_Cover': {'in_ic_paths': ['projects/rap-data-365417/assets/vegetation-cover-v3'],
                          'var_names': ['AFG', 'BGR', 'LTR', 'PFG', 'SHR', 'TRE'],
                          'var_type': 'Continuous',
                          'ic_mask': True},
            'RAP_Production': {'in_ic_paths': ['projects/rap-data-365417/assets/npp-partitioned-v3'],
                               'var_names': ['afgAGB', 'pfgAGB', 'shrAGB', 'herbaceousAGB'],
                               'var_type': 'Continuous',
                               'ic_mask': True},
            'RAP_16dProduction': {'in_ic_paths': ['projects/rap-data-365417/assets/npp-partitioned-16day-v3'],
                                  'var_names': ['afgAGB', 'pfgAGB', 'shrAGB', 'herbaceousAGB'],
                                  'var_type': 'Continuous',
                                  'ic_mask': True},
            'USDM': {'in_ic_paths': ['projects/climate-engine/usdm/weekly'],
                     'var_names': ['drought'],
                     'var_type': 'Categorical',
                     'ic_mask': False},
            'MOD11_LST': {'in_ic_paths': ['MODIS/061/MOD11A2'],
                          'var_names': ['LST_Day_1km'],
                          'var_type': 'Continuous',
                          'ic_mask': True},
            'Landsat': {'in_ic_paths': ['LANDSAT/LT05/C02/T1_L2', 'LANDSAT/LE07/C02/T1_L2', 'LANDSAT/LC08/C02/T1_L2', 'LANDSAT/LC09/C02/T1_L2'],
                        'var_names': ['NDVI'],
                        'var_type': 'Continuous',
                        'ic_mask': True},
            'MOD16_ET': {'in_ic_paths': ['MODIS/006/MOD16A2'],
                         'var_names': ['ET', 'PET'],
                         'var_type': 'Continuous',
                         'ic_mask': True},
            'MTBS': {'in_ic_paths': ['projects/climate-engine-pro/assets/mtbs_mosaics_annual'],
                     'var_names': ['Severity'],
                     'var_type': 'Categorical',
                     'ic_mask': True},
            'VegDRI': {'in_ic_paths': ['projects/climate-engine-pro/assets/ce-veg-dri'],
                     'var_names': ['vegdri'],
                     'var_type': 'Categorical',
                     'ic_mask': False},
            'VegDRI_Cont': {'in_ic_paths': ['projects/climate-engine-pro/assets/ce-veg-dri'],
                     'var_names': ['vegdri'],
                     'var_type': 'Continuous',
                     'ic_mask': False}}

# Define properties for variables in dictionary
var_dict = {'Long_Term_Drought_Blend': {'units': 'drought'},
            'Short_Term_Drought_Blend': {'units': 'drought'},
            'precip': {'units': 'mm'},
            'tmmn': {'units': 'degrees C'},
            'tmmx': {'units': 'degrees C'},
            'eto': {'units': 'mm'},
            'vpd': {'units': 'kPa'},
            'windspeed': {'units': 'm/s'},
            'srad': {'units': 'W/m^2'},
            'AFG': {'units': '% cover'},
            'BGR': {'units': '% cover'},
            'LTR': {'units': '% cover'},
            'PFG': {'units': '% cover'},
            'SHR': {'units': '% cover'},
            'TRE': {'units': '% cover'},
            'afgAGB': {'units': 'lbs/acre'},
            'pfgAGB': {'units': 'lbs/acre'},
            'shrAGB': {'units': 'lbs/acre'},
            'herbaceousAGB': {'units': 'lbs/acre'},
            'drought': {'units': 'drought'},
            'LST_Day_1km': {'units': 'degrees C'},
            'NDVI': {'units': 'unitless'},
            'ET': {'units': 'mm'},
            'PET': {'units': 'mm'},
            'Severity': {'units': 'fire severity'},
            'vegdri': {'units': 'drought'}}