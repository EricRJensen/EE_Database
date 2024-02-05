// Require client library and private key.
var ee = require('@google/earthengine');
var privateKey = require('./.private-key.json');
const functions = require('@google-cloud/functions-framework');


// Initialize client library and run analysis.
var runAnalysis = function() {
    
    ee.initialize(null, null, function() {

        // Import all BLM land unit Feature Collections and annual RAP Image Collections
        var blm_al = ee.FeatureCollection('projects/dri-apps/assets/blm-admin/blm-natl-grazing-allotment-polygons'),
            blm_fo = ee.FeatureCollection('projects/dri-apps/assets/blm-admin/blm-natl-admu-fieldoffice-polygons'),
            blm_do = ee.FeatureCollection('projects/dri-apps/assets/blm-admin/blm-natl-admu-districtoffice-polygons'),
            blm_so = ee.FeatureCollection('projects/dri-apps/assets/blm-admin/blm-natl-admu-stateoffice-polygons'),
            rap_cov_afg_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapcover-afg'),
            rap_cov_pfg_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapcover-pfg'),
            rap_cov_shr_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapcover-shr'),
            rap_cov_tre_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapcover-tre'),
            rap_cov_bgr_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapcover-bgr'),
            rap_cov_ltr_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapcover-ltr'),
            rap_prd_afg_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapproduction-afgagb'),
            rap_prd_pfg_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapproduction-pfgagb'),
            rap_prd_tot_al = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmallotments-rapproduction-herbaceousagb'),
            rap_cov_afg_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapcover-afg'),
            rap_cov_pfg_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapcover-pfg'),
            rap_cov_shr_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapcover-shr'),
            rap_cov_tre_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapcover-tre'),
            rap_cov_bgr_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapcover-bgr'),
            rap_cov_ltr_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapcover-ltr'),
            rap_prd_afg_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapproduction-afgagb'),
            rap_prd_pfg_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapproduction-pfgagb'),
            rap_prd_tot_fo = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmfieldoffices-rapproduction-herbaceousagb'),
            rap_cov_afg_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapcover-afg'),
            rap_cov_pfg_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapcover-pfg'),
            rap_cov_shr_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapcover-shr'),
            rap_cov_tre_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapcover-tre'),
            rap_cov_bgr_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapcover-bgr'),
            rap_cov_ltr_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapcover-ltr'),
            rap_prd_afg_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapproduction-afgagb'),
            rap_prd_pfg_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapproduction-pfgagb'),
            rap_prd_tot_do = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmdistrictoffices-rapproduction-herbaceousagb'),
            rap_cov_afg_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapcover-afg'),
            rap_cov_pfg_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapcover-pfg'),
            rap_cov_shr_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapcover-shr'),
            rap_cov_tre_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapcover-tre'),
            rap_cov_bgr_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapcover-bgr'),
            rap_cov_ltr_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapcover-ltr'),
            rap_prd_afg_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapproduction-afgagb'),
            rap_prd_pfg_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapproduction-pfgagb'),
            rap_prd_tot_so = ee.ImageCollection('projects/climate-engine-pro/assets/blm-database/blmstateoffices-rapproduction-herbaceousagb')
        
        // Define bounds of images
        var geom_al = ee.Geometry.Polygon(
                [[[-0.00021188665414229035, 0.00020869015318670687],
                  [-0.00021188665414229035, -0.00001393319507416009],
                  [4.318031974544283, -0.00001393319507416009],
                  [4.318031974544283, 0.00020869015318670687]]], null, false);
        var geom_so =ee.Geometry.Polygon(
                [[[-0.00020999502213125965, 0.00021059316294293678],
                  [-0.00020999502213125965, -0.000009347976266565],
                  [0.0020135562512146876, -0.000009347976266565],
                  [0.0020135562512146876, 0.00021059316294293678]]], null, false);
        var geom_do = ee.Geometry.Polygon(
                [[[-0.00021804164917593738, 0.00021059316294293678],
                  [-0.00021804164917593738, -0.00001739460326799394],
                  [0.009212605247186367, -0.00001739460326799394],
                  [0.009212605247186367, 0.00021059316294293678]]], null, false);
        var geom_fo = ee.Geometry.Polygon(
                [[[-0.0002064309226756933, 0.00021152890654416108],
                  [-0.0002064309226756933, -0.000017799964224257777],
                  [0.024410212863754666, -0.000017799964224257777],
                  [0.024410212863754666, 0.00021152890654416108]]], null, false);
        
        // Define dictionaries of objects for each type
        var al_info = {'fc': blm_al, 'id': 'ALLOT_ID', 'geom': geom_al, 'level': 'al', 'cov_data': ee.List([rap_cov_afg_al, rap_cov_pfg_al, rap_cov_shr_al, rap_cov_tre_al, rap_cov_bgr_al, rap_cov_ltr_al]), 'prd_data': [rap_prd_afg_al, rap_prd_pfg_al, rap_prd_tot_al]}
        var fo_info = {'fc': blm_fo, 'id': 'FO_ID', 'geom': geom_fo, 'level': 'fo', 'cov_data': ee.List([rap_cov_afg_fo, rap_cov_pfg_fo, rap_cov_shr_fo, rap_cov_tre_fo, rap_cov_bgr_fo, rap_cov_ltr_fo]), 'prd_data': [rap_prd_afg_fo, rap_prd_pfg_fo, rap_prd_tot_fo]}
        var do_info = {'fc': blm_do, 'id': 'DO_ID', 'geom': geom_do, 'level': 'do', 'cov_data': ee.List([rap_cov_afg_do, rap_cov_pfg_do, rap_cov_shr_do, rap_cov_tre_do, rap_cov_bgr_do, rap_cov_ltr_do]), 'prd_data': [rap_prd_afg_do, rap_prd_pfg_do, rap_prd_tot_do]}
        var so_info = {'fc': blm_so, 'id': 'SO_ID', 'geom': geom_so, 'level': 'so', 'cov_data': ee.List([rap_cov_afg_so, rap_cov_pfg_so, rap_cov_shr_so, rap_cov_tre_so, rap_cov_bgr_so, rap_cov_ltr_so]), 'prd_data': [rap_prd_afg_so, rap_prd_pfg_so, rap_prd_tot_so]}


        // -------------------- Generate stats images --------------------------
        
        // Get images with values
        function get_id_image(ic){
          return(ic.filter(ee.Filter.eq('system:index', '0_id')).first())
        }
        
        // Get images with values
        function get_values_images(ic){
          return(ic.filter(ee.Filter.neq('system:index', '0_id')).select('mean'))
        }
        
        // Generate year_ic for Sen's slope calculation
        function generate_year_image(i){
          i = ee.Image(i)
          var i_dict = i.toDictionary()
          var date = ee.Date(i.get('system:time_start'))
          var year = ee.Number.parse(date.format('YYYY')).toInt()
          var year_i = ee.Image(year).rename('Year').toFloat()
          return(year_i.addBands(i).set(i_dict).set('Year', year))
        }
        
        // Get current values
        function get_current_conditions(ic){
          ic = get_values_images(ic)
          return(ic.sort('system:time_start', false).first())
        }
        
        // Get median values
        function get_median_values(ic){
          ic = get_values_images(ic)
          return(ic.reduce(ee.Reducer.median()))
        }
        
        // Get difference from average anomaly
        function get_current_anomaly_dif(ic){
          ic = get_values_images(ic)
          var current_i = get_current_conditions(ic)
          var median_i = get_median_values(ic).reduce(ee.Reducer.median())
          return(current_i.subtract(median_i))
        }
        
        // Get percent of average anomaly
        function get_current_anomaly_pct(ic){
          ic = get_values_images(ic)
          var current_i = get_current_conditions(ic)
          var median_i = get_median_values(ic).reduce(ee.Reducer.median())
          return(current_i.divide(median_i))
        }
        
        // Get sen's slope trends
        function get_sens_slope(ic, len){
          ic = get_values_images(ic)
          var prepped_ic = ic.map(generate_year_image).sort('Year', false).limit(len).sort('Year', true)
          var sens_i = prepped_ic.reduce(ee.Reducer.sensSlope())
          return(sens_i.select('slope'))
        }
        
        // Run all stats functions above
        function run_stats(ic){
          
          // Cast Image Collection and get objects
          ic = ee.ImageCollection(ic)
          var var_name = ee.String(ic.first().get('var_name'))
          
          // Run stats functions
          var curr_i = get_current_conditions(ic).rename(var_name.cat('_CURR'))
          var medn_i = get_median_values(ic).rename(var_name.cat('_MEDN'))
          var anom_dif_i = get_current_anomaly_dif(ic).rename(var_name.cat('_ANOM_DIF'))
          var anom_pct_i = get_current_anomaly_pct(ic).rename(var_name.cat('_ANOM_PCT'))
          var sens_all_i = get_sens_slope(ic, ic.size()).rename(var_name.cat('_SENS_ALL'))
          var sens_30_i = get_sens_slope(ic, 30).rename(var_name.cat('_SENS_30'))
          var sens_20_i = get_sens_slope(ic, 20).rename(var_name.cat('_SENS_20'))
          var sens_10_i = get_sens_slope(ic, 10).rename(var_name.cat('_SENS_10'))
          
          return(curr_i.addBands(medn_i).addBands(anom_dif_i).addBands(anom_pct_i).addBands(sens_all_i).addBands(sens_30_i).addBands(sens_20_i).addBands(sens_10_i))
        }
        
        // Function to clean up band names after converting to bands
        function rename_bands(str){
          return(ee.String(str).slice(2, 100))
        }
        
        // Iterate run_stats function over list of database Image Collections, convert to image, and clean up band names
        function generate_stats_fc(dict, dataset){
          
          // Cast Dictionary and get level type
          var level = dict.level
          
          // Get ID image
          var ic = ee.ImageCollection(ee.List(dict[dataset]).get(0))
          var id_i = get_id_image(ic)
          
          // Run stats on Image Collection and process into Image
          var stats_ic = ee.ImageCollection(ee.List(dict[dataset]).map(run_stats))
          var stats_i = stats_ic.toBands()
          stats_i = stats_i.rename(stats_i.bandNames().map(rename_bands))
          
          // Combine ID image and stats image
          var all_i = id_i.addBands(stats_i)
          
          // Sample Image Collection to return Feature Collection
          var all_fc = all_i.sample(ee.Geometry(dict.geom))
          return(all_fc.set('level', level))
        }
        
        // Run functions for all levels
        al_info.cov_stats = ee.FeatureCollection(generate_stats_fc(al_info, 'cov_data'))
        fo_info.cov_stats = ee.FeatureCollection(generate_stats_fc(fo_info, 'cov_data'))
        do_info.cov_stats = ee.FeatureCollection(generate_stats_fc(do_info, 'cov_data'))
        so_info.cov_stats = ee.FeatureCollection(generate_stats_fc(so_info, 'cov_data'))
        al_info.prd_stats = ee.FeatureCollection(generate_stats_fc(al_info, 'prd_data'))
        fo_info.prd_stats = ee.FeatureCollection(generate_stats_fc(fo_info, 'prd_data'))
        do_info.prd_stats = ee.FeatureCollection(generate_stats_fc(do_info, 'prd_data'))
        so_info.prd_stats = ee.FeatureCollection(generate_stats_fc(so_info, 'prd_data'))


        // -------------------- Generate metadata for exports ---------------------
        
        // Metadata calculated off of database image collection
        var ic_metadata = rap_cov_afg_al
        
        var metadata = {'system:time_start': ic_metadata.aggregate_array('system:time_start').reduce(ee.Reducer.max()),
                      'CURR_YEAR': ee.Date(get_current_conditions(ic_metadata).get('system:time_start')).format('Y'),
                      'ANOM_YEAR': ee.Date(get_current_conditions(ic_metadata).get('system:time_start')).format('Y'),
                      'ANOM_CLIM_START': ee.Date(get_values_images(ic_metadata).aggregate_array('system:time_start').reduce(ee.Reducer.min())).format('Y'),
                      'ANOM_CLIM_END': ee.Date(get_values_images(ic_metadata).aggregate_array('system:time_start').reduce(ee.Reducer.max())).format('Y'),
                      'SENS_ALL_START': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).aggregate_array('Year').reduce(ee.Reducer.min()),
                      'SENS_ALL_END': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).aggregate_array('Year').reduce(ee.Reducer.max()),
                      'SENS_30_START': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).limit(30).aggregate_array('Year').reduce(ee.Reducer.min()),
                      'SENS_30_END': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).limit(30).aggregate_array('Year').reduce(ee.Reducer.max()),
                      'SENS_20_START': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).limit(20).aggregate_array('Year').reduce(ee.Reducer.min()),
                      'SENS_20_END': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).limit(20).aggregate_array('Year').reduce(ee.Reducer.max()),
                      'SENS_10_START': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).limit(10).aggregate_array('Year').reduce(ee.Reducer.min()),
                      'SENS_10_END': get_values_images(ic_metadata).map(generate_year_image).sort('Year', false).limit(10).aggregate_array('Year').reduce(ee.Reducer.max())
        }
        
        var var_metadata = {'CURR': {'ce_calc': 'value'},
                           'MEDN': {'ce_calc': 'clim'},
                           'ANOM_DIF': {'ce_calc': 'anom'},
                           'ANOM_PCT': {'ce_calc': 'anompercentof'},
                           'SENS_ALL': {'ce_calc': 'mk_sen'},
                           'SENS_30': {'ce_calc': 'mk_sen'},
                           'SENS_20': {'ce_calc': 'mk_sen'},
                           'SENS_10': {'ce_calc': 'mk_sen'}}
        
        
        
        // -------------------- Join stats to geometries ----------------------
        
        al_info.cov_out_fc = complete_join(al_info, 'cov_stats').set(metadata).set('variable_dictionaries', var_metadata)
        fo_info.cov_out_fc = complete_join(fo_info, 'cov_stats').set(metadata).set('variable_dictionaries', var_metadata)
        do_info.cov_out_fc = complete_join(do_info, 'cov_stats').set(metadata).set('variable_dictionaries', var_metadata)
        so_info.cov_out_fc = complete_join(so_info, 'cov_stats').set(metadata).set('variable_dictionaries', var_metadata)
        al_info.prd_out_fc = complete_join(al_info, 'prd_stats').set(metadata).set('variable_dictionaries', var_metadata)
        fo_info.prd_out_fc = complete_join(fo_info, 'prd_stats').set(metadata).set('variable_dictionaries', var_metadata)
        do_info.prd_out_fc = complete_join(do_info, 'prd_stats').set(metadata).set('variable_dictionaries', var_metadata)
        so_info.prd_out_fc = complete_join(so_info, 'prd_stats').set(metadata).set('variable_dictionaries', var_metadata)

        console.log(so_info.level)



    })
}

// "Join" function to parse geometry and stats as a feature
var parse_features = function(list){
          
  // Cast objects
  list = ee.List(list)
  var id = ee.String(list.get(0))
  var dict = ee.Dictionary(list.get(1))
  var dataset = ee.String(list.get(2))
          
  var f_geom = ee.FeatureCollection(dict.get('fc')).filter(ee.Filter.eq(dict.get('id'), id)).geometry()
  var f_stats = ee.FeatureCollection(dict.get(dataset)).filter(ee.Filter.eq('id', ee.Number.parse(id))).first().toDictionary()
  var f = ee.Feature(f_geom, f_stats)
  return(f)
}

// Complete join
var complete_join = function(dict, dataset){
          
  // Zip IDs and dicts to unpack as "tuples" in parse_features()
  var ids_list = ee.List(dict['fc'].aggregate_array(dict['id']))
  ids_list = ids_list.slice(0,1).cat(ids_list.slice(2, ids_list.size()))

  var zip_lists = function(id){
    return(ee.List([ee.String(id), ee.Dictionary(dict), ee.String(dataset)]))
    }
        
  var all_list = ids_list.map(zip_lists)
        
  var fc = ee.FeatureCollection(all_list.map(parse_features))
  return(fc)
}



// Authenticate using a service account.
ee.data.authenticateViaPrivateKey(privateKey, runAnalysis, function(e) {
    console.error('Authentication error: ' + e);
})

// Register an HTTP function with the Functions Framework that will be executed
// when you make an HTTP request to the deployed function's endpoint.
functions.http('runGET', (req, res) => {
});