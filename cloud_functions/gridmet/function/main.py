import argparse
import datetime
import logging
import math
import os
import re
import sys
import time

import ee
from flask import abort, Response
from google.cloud import tasks_v2

import eeDatabase_coreMethods as eedb_cor
import eeDatabase_collectionMethods as eedb_col
import eeDatabase_collectionInfo as eedb_colinfo

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requets').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


# ------------------------------------- Define parameters -----------------------------------------------
# ----- Define time period to export -----
start_date = datetime.datetime(1980, 1, 1)
end_date = datetime.datetime(2024, 1, 1)

# ----- Define input Image Collection -----
# Define input dataset
# See dictionary below for list of input datasets
in_ic_name = 'GridMET'

# ----- Define variable from dataset -----
# See dictionary below for variables available for each dataset
var_names = ['precip', 'tmmn', 'tmmx', 'eto', 'vpd', 'windspeed', 'srad']
var_name = var_names[0]

# -----Define input Feature Collection -----
# Define input path for Feature Collection
in_fc_path = 'projects/dri-apps/assets/blm-admin/blm-natl-grazing-allotment-polygons'
in_fc = ee.FeatureCollection(in_fc_path)

# ----- Define mask, if applicable -----
# For BLM, we will apply mask to field offices, district offices, and state offices, but not to allotments
# Apply mask for ownership, landcover, or other variables. Must be binary mask.
mask = False
mask_path = 'projects/dri-apps/assets/blm-admin/blm-natl-admu-sma-binary'

# ----- Define additional parameters required for functions and output properties below -----
# Define land unit names
if(in_fc_path == 'projects/dri-apps/assets/blm-admin/blm-natl-grazing-allotment-polygons'):
    land_unit_long = 'BLM_Natl_Grazing_Allotment_Polygons'
    land_unit_short = 'BLM_Allotments'
    tile_scale = 1
    in_fc_id = 'ALLOT_ID'
elif(in_fc_path == 'projects/dri-apps/assets/blm-admin/blm-natl-admu-fieldoffice-polygons'):
    land_unit_long = 'BLM_Natl_FieldOffice_Polygons'
    land_unit_short = 'BLM_FieldOffices'
    tile_scale = 16
    in_fc_id = 'FO_ID'
elif(in_fc_path == 'projects/dri-apps/assets/blm-admin/blm-natl-admu-districtoffice-polygons'):
    land_unit_long = 'BLM_Natl_DistrictOffice_Polygons'
    land_unit_short = 'BLM_DistrictOffices'
    tile_scale = 16
    in_fc_id = 'DO_ID'
elif(in_fc_path == 'projects/dri-apps/assets/blm-admin/blm-natl-admu-stateoffice-polygons'):
    land_unit_long = 'BLM_Natl_StateOffice_Polygons'
    land_unit_short = 'BLM_StateOffices',
    tile_scale = 16
    in_fc_id = 'SO_ID'

# ----- Pull out additional variables needed to run exports -----
in_ic_paths = eedb_colinfo.in_ic_dict.get(in_ic_name).get('in_ic_paths')
in_ic_res = ee.Number(ee.ImageCollection(in_ic_paths[0]).first().projection().nominalScale()).round().getInfo()
var_type = eedb_colinfo.in_ic_dict.get(in_ic_name).get('var_type')
var_units = eedb_colinfo.var_dict.get(var_name).get('units')
out_path = f"projects/climate-engine-pro/assets/blm-database/{land_unit_short.replace('_', '').lower()}-{in_ic_name.replace('_', '').lower()}-{var_name.replace('_', '').lower()}"



# -------------------------------- Might break ---------------------------------
if 'FUNCTION_REGION' in os.environ:
    logging.debug(f'\nInitializing GEE using application default credentials')
    import google.auth
    credentials, project_id = google.auth.default(
        default_scopes=['https://www.googleapis.com/auth/earthengine'])
    ee.Initialize(credentials)
else:
    ee.Initialize()


# ------------------ Analogous to database export functions --------------------
def rtma_daily_asset_ingest(tgt_dt, rs_source=None, overwrite_flag=False):
    """Build daily RTMA reference ET asset for a single date

    Parameters
    ----------
    tgt_dt : datetime
    rs_source : {'GRIDMET', 'NLDAS'}, optional
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """
    tgt_date = tgt_dt.strftime('%Y-%m-%d')
    logging.info(f'Export RTMA daily asset - {tgt_date}')

    export_name = 'rtma_daily_{}'.format(tgt_dt.strftime(ASSET_DT_FMT))
    asset_id = '{}/{}{:02d}{:02d}'.format(
        ASSET_COLL_ID, tgt_dt.year, tgt_dt.month, tgt_dt.day)
    # logging.info('asset_id: {}'.format(asset_id))

    # Set start time to 6 UTC to match GRIDMET (or 7?)
    # This should help set the solar sum and tmax/tmin correctly
    start_date = ee.Date.fromYMD(tgt_dt.year, tgt_dt.month, tgt_dt.day)\
        .advance(6, 'hour')
    end_date = start_date.advance(1, 'day')

    src_coll = ee.ImageCollection(SOURCE_COLL_ID)\
        .filterDate(start_date, end_date)

    # The spatial grid for assets starting on 2018-12-05 is slightly larger
    # Intentionally using the smaller grid for all daily assets
    # if src_date <= '2018-12-04':
    src_size_str = '2145x1377'
    src_geo = [2539.703, 0, -2764486.9281005403,
               0, -2539.703, 3232110.5100932177]
    # else:
    # src_size_str = '2345x1597'
    # src_geo = [2539.703, 0, -3272417.1397942575,
    #            0, -2539.703, 3790838.3367873137]

    # This CRS WKT is modified from the GRIB2 CRS to align the images in EE
    src_crs = (
        'PROJCS["NWS CONUS", \n'
        '  GEOGCS["WGS 84", \n'
        '    DATUM["World Geodetic System 1984", \n'
        '      SPHEROID["WGS 84", 6378137.0, 298.257223563, AUTHORITY["EPSG","7030"]], \n'
        '      AUTHORITY["EPSG","6326"]], \n'
        '    PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]], \n'
        '    UNIT["degree", 0.017453292519943295], \n'
        '    AXIS["Geodetic longitude", EAST], \n'
        '    AXIS["Geodetic latitude", NORTH], \n'
        '    AUTHORITY["EPSG","4326"]], \n'
        '  PROJECTION["Lambert_Conformal_Conic_1SP"], \n'
        '  PARAMETER["semi_major", 6371200.0], \n'
        '  PARAMETER["semi_minor", 6371200.0], \n'
        '  PARAMETER["central_meridian", -95.0], \n'
        '  PARAMETER["latitude_of_origin", 25.0], \n'
        '  PARAMETER["scale_factor", 1.0], \n'
        '  PARAMETER["false_easting", 0.0], \n'
        '  PARAMETER["false_northing", 0.0], \n'
        '  UNIT["m", 1.0], \n'
        '  AXIS["x", EAST], \n'
        '  AXIS["y", NORTH]]'
    )

    # Check if there are 24 available images
    src_count = get_info(src_coll.size(), default_value=-1)
    logging.debug(f'  Source image count: {src_count}')
    if src_count == -1:
        logging.info(f'  Could not get source image count, skipping\n{e}')
        return f'{export_name} - Could not get source image count, skipping\n{e}'
    elif src_count == 0:
        logging.info(f'  No RTMA source data for date, skipping')
        return f'{export_name} - No RTMA source data for date, skipping\n'
    elif src_count == 23:
        # Allow daily images to be built even if only 23 hours are available
        logging.info(f'  Only 23 hours of RTMA data for date')
    elif src_count < 23:
        logging.info(f'  Less than 23 hours of RTMA data for date, skipping')
        return f'{export_name} - Less than 23 hours of RTMA data for date, skipping\n'
    # elif src_count < 24:
    #     return f'{export_name} - Less than 24 hours data for date, skipping\n'


    # Use NLDAS for solar if it exists
    # If NLDAS doesn't exist (or isn't complete), attempt to use GRIDMET
    # For GRIDMET images, always overwrite existing
    if rs_source is None:
        rs_source = 'NLDAS'
    if rs_source.upper() not in ['NLDAS', 'GRIDMET']:
        return f'{export_name} - Unsupported rs source, skipping'

    if rs_source.upper() == 'NLDAS':
        rs_coll = ee.ImageCollection('NASA/NLDAS/FORA0125_H002')\
            .filterDate(start_date, end_date)\
            .select(['shortwave_radiation'])
        rs_count = get_info(rs_coll.size(), default_value=-1)
        logging.debug(f'  Rs image count: {rs_count}')

        if rs_count == -1:
            logging.info(f'  Error requesting NLDAS Rs data, trying GRIDMET')
            rs_source = 'GRIDMET'
            overwrite_flag = True
        elif rs_count == 0:
            logging.info(f'  No NLDAS Rs data for date, trying GRIDMET')
            rs_source = 'GRIDMET'
            overwrite_flag = True
        elif rs_count < 24:
            logging.info(f'  Less than 24 hours of NLDAS Rs data, trying GRIDMET')
            rs_source = 'GRIDMET'
            overwrite_flag = True
        else:
            rs_img = ee.Image(rs_coll.sum()).double().divide(24.0)

    if rs_source.upper() == 'GRIDMET':
        rs_coll = ee.ImageCollection('IDAHO_EPSCOR/GRIDMET')\
            .filterDate(start_date, end_date)\
            .select(['srad'])
        rs_count = get_info(rs_coll.size(), default_value=-1)
        logging.debug(f'  Rs image count: {rs_count}')

        if rs_count == -1:
            logging.info(f'  Error requesting GRIDMET rs data, skipping')
            return f'{export_name} - Error requesting rs data, skipping\n'
        elif rs_count == 0:
            logging.info(f'  No GRIDMET Rs data for date, skipping')
            return f'{export_name} - No Rs data for date, skipping\n'
        else:
            rs_img = ee.Image(rs_coll.first())

    if ee.data.getInfo(asset_id):
        # Force overwrite of GRIDMET based images once NLDAS is available
        existing_rs_source = get_info(ee.Image(asset_id).get('rs_source'))
        if existing_rs_source == 'GRIDMET' and rs_source == 'NLDAS':
            overwrite_flag = True

        if overwrite_flag:
            try:
                ee.data.deleteAsset(asset_id)
            except Exception as e:
                return f'{export_name} - An error occurred while trying to '\
                       f'delete the existing asset, skipping\n{e}\n'
        else:
            logging.info('The asset already exists and overwrite is False, skipping')
            return f'{export_name} - The asset already exists and overwrite '\
                   f'is False, skipping\n'

    # We should get the same answer passing in the rs_source keyword or the
    #   rs image, but we would need to convert the rs image to MJ m-2 day-1
    refet_obj = openet.refetgee.Daily.rtma(src_coll, rs=rs_source)
    # refet_obj = openet.refetgee.Daily.rtma(src_coll, rs=rs_img.multiply(0.0864))

    # # We could use the HGT band from one of the images for the elevation
    # refet_obj = openet.refetgee.Daily.rtma(
    #     src_coll, rs=rs_source, elev=ee.Image(src_coll.first()).select(['HGT']))

    # The precip band is not present for all images
    # Write an empty (masked) image for all dates that are missing
    prcp_coll = src_coll.select(['ACPC01'])
    prcp_count = get_info(prcp_coll.size(), default_value=-1)
    logging.debug(f'  Precip image count: {rs_count}')

    if prcp_count == 24:
        # kg/m^2=mm, assume water density of 1000kg/m3 (24hr total gives mm/day)
        prcp_img = prcp_coll.sum().double()
    elif prcp_count == 23:
        # Allow daily images to be built even if only 23 hours are available
        logging.info(f'  Only 23 hours of precip data for date')
        prcp_img = prcp_coll.sum().double()
    # CGM - For testing, error if precip is missing, but later just use masked image
    elif prcp_count == -1:
        logging.info(f'{export_name} - Error requesting precip data, skipping\n')
        return f'{export_name} - Error requesting precip data, skipping\n'
    elif prcp_count == 0:
        logging.info(f'{export_name} - No precip data for date, skipping\n')
        return f'{export_name} - No precip data for date, skipping\n'
    elif prcp_count < 23:
        logging.info(f'{export_name} - Less than 23 hours of precip data, skipping\n')
        return f'{export_name} - Less than 23 hours of precip data, skipping\n'
    else:
        logging.info(f'{export_name} - Unexpected precip count ({prcp_count}), skipping\n')
        return f'{export_name} - Unexpected precip count ({prcp_count}), skipping\n'
    # else:
    #     logging.info(f'  No precip data, filling with a masked image')
    #     prcp_img = src_coll.first().select(['TMP']).multiply(0).updateMask(0)


    # # TODO: Compute the hourly wind vectors,
    # #   then average the vectors to get the daily mean speed and direction
    # def wind_vectors(img):
    #     speed = img.select(['WIND'])
    #     direction = img.select(['WDIR'])
    #     u = speed.multiply(direction.cos())
    #     v = speed.multiply(direction.sin())


    output_image = ee.Image([
            src_coll.select(['TMP']).max(),
            src_coll.select(['TMP']).min(),
            rs_img.double(),
            src_coll.select(['SPFH']).mean(),
            src_coll.select(['WIND']).mean(),
            wind_direction_circular_mean(src_coll.select(['WDIR'])),
            src_coll.select(['TCDC']).mean(),
            src_coll.select(['PRES']).mean(),
            src_coll.select(['DPT']).mean(),
            refet_obj.eto,
            refet_obj.etr,
            prcp_img,
        ])\
        .rename(['TMAX', 'TMIN', 'SRAD', 'SPH', 'WIND', 'WDIR',
                 'TCDC', 'PRES', 'DPT', 'ETo', 'ETr', 'PRCP'])\
        .toFloat()\
        .set({
            'system:index': tgt_dt.strftime('%Y%m%d'),
            'system:time_start': start_date.millis(),
            # 'system:time_end': end_date.millis(),
            'date': tgt_dt.strftime('%Y-%m-%d'),
            'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
            'precip_images': prcp_count,
            'refetgee_version': openet.refetgee.__version__,
            'rs_images': rs_count,
            'rs_source': rs_source,
            'source_images': src_count,
        })

    export_task = ee.batch.Export.image.toAsset(
        image=output_image,
        description=export_name,
        assetId=asset_id,
        dimensions=src_size_str,
        crs=src_crs,
        crsTransform='[' + ', '.join(map(str, src_geo)) + ']',
    )

    # Try to start the task a couple of times
    for i in range(1, 6):
        try:
            export_task.start()
            break
        except ee.ee_exception.EEException as e:
            logging.warning('EE Exception, retry {}\n{}'.format(i, e))
        except Exception as e:
            logging.warning('Unhandled Exception: {}'.format(e))
            return 'Unhandled Exception: {}'.format(e)
        time.sleep(i ** 3)

    logging.info(f'  {export_name} - {export_task.id}')
    return f'{export_name} - {export_task.id}\n'


# --------------- Analogous to database check dates function -----------------
def rtma_daily_asset_dates(start_dt, end_dt, overwrite_flag=False):
    """Identify dates of missing RTMA daily assets

    Parameters
    ----------
    start_dt : datetime
    end_dt : datetime
    overwrite_flag : bool, optional

    Returns
    -------
    list : datetimes

    """
    logging.info('Building RTMA daily asset ingest date list')

    task_id_re = re.compile('rtma_daily_(?P<date>\d{8})')
    asset_id_re = re.compile(
        ASSET_COLL_ID.split('projects/')[-1] + '/(?P<date>\d{8})$')

    # Figure out which asset dates need to be ingested
    # Start with a list of dates to check
    # logging.debug('\nBuilding Date List')
    test_dt_list = list(date_range(start_dt, end_dt, skip_leap_days=False))
    if not test_dt_list:
        logging.info('Empty date range')
        return []
    # logging.info('\nTest dates: {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    # Check if any of the needed dates are currently being ingested
    # Check task list before checking asset list in case a task switches
    #   from running to done before the asset list is retrieved.
    task_id_list = [
        desc.replace('\nAsset ingestion: ', '')
        for desc in get_ee_tasks(states=['RUNNING', 'READY']).keys()]
    task_date_list = [
        datetime.datetime.strptime(m.group('date'), '%Y%m%d').strftime('%Y-%m-%d')
        for task_id in task_id_list
        for m in [task_id_re.search(task_id)] if m]
    # logging.info('Task dates: {}'.format(', '.join(task_date_list)))

    # Switch date list to be dates that are missing
    test_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or dt.strftime('%Y-%m-%d') not in task_date_list]
    if not test_dt_list:
        logging.info('All dates are queued for export')
        return []
    # else:
    #     logging.info('\nMissing asset dates: {}'.format(', '.join(
    #         map(lambda x: x.strftime('%Y-%m-%d'), test_dt_list))))

    # Check if the assets already exist
    # For now, assume the collection exists
    # Only checking NLDAS based assets will cause GRIDMET based ones to be rebuilt
    asset_coll = ee.ImageCollection(ASSET_COLL_ID)\
        .filterDate(start_dt.strftime('%Y-%m-%d'),
                    (end_dt + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))\
        .filter(ee.Filter.eq('rs_source', 'NLDAS'))
    # CGM - Should we continually rebuild images that are missing an hour image?
    #     .filter(ee.Filter.eq('source_images', 24))\
    #     .filter(ee.Filter.eq('precip_images', 24))\
    #     .filter(ee.Filter.eq('rs_images', 24))
    asset_id_list = get_info(asset_coll.aggregate_array('system:index'))
    asset_id_list = [f'{asset_id}/{item}' for item in asset_id_list]
    asset_date_list = [
        datetime.datetime.strptime(m.group('date'), ASSET_DT_FMT)
            .strftime('%Y-%m-%d')
        for asset_id in asset_id_list
        for m in [asset_id_re.search(asset_id)] if m]
    # logging.info('Asset dates: {}'.format(', '.join(asset_date_list)))

    # Switch date list to be dates that are missing
    tgt_dt_list = [
        dt for dt in test_dt_list
        if overwrite_flag or dt.strftime('%Y-%m-%d') not in asset_date_list]
    if not tgt_dt_list:
        logging.info('No missing asset dates')
        return []
    # else:
    #     logging.info('\nMissing asset dates: {}'.format(', '.join(
    #         map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))))

    return tgt_dt_list


# ------------------ Analogous to database iteration in Jupyter notebook ------------------

def cron_scheduler(request):
    """Parse JSON/request arguments and queue ingest tasks for a date range"""
    logging.info('Queuing RTMA daily asset ingest tasks')
    response = 'Queue RTMA daily asset ingest tasks\n'
    args = {}

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'start' in request_json:
        start_date = request_json['start']
    elif request_args and 'start' in request_args:
        start_date = request_args['start']
    else:
        start_date = None

    if request_json and 'end' in request_json:
        end_date = request_json['end']
    elif request_args and 'end' in request_args:
        end_date = request_args['end']
    else:
        end_date = None

    if start_date is None and end_date is None:
        start_date = (datetime.date.today() -
                      datetime.timedelta(days=START_DAY_OFFSET)).strftime('%Y-%m-%d')
        end_date = (datetime.date.today() -
                    datetime.timedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d')
    elif start_date is None or end_date is None:
        abort(400, description='Both start and end date must be specified')

    try:
        args['start_dt'] = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    except:
        abort(400, description=f'Start date {start_date} could not be parsed')
    try:
        args['end_dt'] = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    except:
        abort(400, description=f'End date {end_date} could not be parsed')

    if args['end_dt'] < args['start_dt']:
        abort(400, description='End date must be after start date')

    if request_json and 'overwrite' in request_json:
        overwrite_flag = request_json['overwrite']
    elif request_args and 'overwrite' in request_args:
        overwrite_flag = request_args['overwrite']
    else:
        overwrite_flag = 'false'

    if overwrite_flag.lower() in ['true', 't']:
        args['overwrite_flag'] = True
    elif overwrite_flag.lower() in ['false', 'f']:
        args['overwrite_flag'] = False
    else:
        abort(400, description=f'overwrite "{overwrite_flag}" could not be parsed')

    for tgt_dt in rtma_daily_asset_dates(**args):
        response += rtma_daily_asset_ingest(tgt_dt, overwrite_flag=False)

    return Response(response, mimetype='text/plain')


def date_range(start_dt, end_dt, days=1, skip_leap_days=False):
    """Generate dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.
    days : int, optional
        Step size (the default is 1).
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing (the default is True).

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt <= end_dt:
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            yield curr_dt
        curr_dt += datetime.timedelta(days=days)


def get_info(ee_obj, max_retries=4, default_value=None):
    """Make an exponential back off getInfo call on an Earth Engine object"""
    output = default_value
    for i in range(1, max_retries):
        try:
            output = ee_obj.getInfo()
            break
        except ee.ee_exception.EEException as e:
            if ('Earth Engine memory capacity exceeded' in str(e) or
                    'Earth Engine capacity exceeded' in str(e) or
                    'Too many concurrent aggregations' in str(e) or
                    'Computation timed out.' in str(e)):
                # TODO: Maybe add 'Connection reset by peer'
                logging.info(f'    Resending query ({i}/{max_retries})')
                logging.info(f'    {e}')
                time.sleep(i ** 3)
                continue
            else:
                logging.info(f'{e}')
                logging.info('Unhandled Earth Engine exception')
                # CGM - Should this be a continue instead of a break?
                break
        except Exception as e:
            logging.info(f'    Resending query ({i}/{max_retries})')
            logging.debug(f'    {e}')
            time.sleep(i ** 3)
            continue

    return output


def wind_direction_circular_mean(input_coll):
    """Compute circular mean of wind direction images (degrees, 0 is "north")

    Parameters
    ----------
    input_coll : ee.ImageCollection

    Returns
    -------
    ee.Image

    References
    ----------
    https://en.wikipedia.org/wiki/Mean_of_circular_quantities

    Notes
    -----
    The input angles are converted from 0 is "up" (clockwise increasing angles)
        to 0 is "right" (with counter-clockwise increasing angles).
    After computing the atan2 of the sums, the output angle is converted back
        to 0 is "up".  Since the output of atan2 is [-180, 180], which becomes
        [-90, 270] after converting, the final .mod(angle+360) is
        needed to get the output range back to [0, 360].
    Note, the inputs to the GEE atan2 function are a little different from the
        standard atan2(y, x) notation since x is first.

    """
    d2r = math.pi / 180.0
    x_coll = ee.ImageCollection(input_coll.map(
        lambda angle: ee.Image(angle).multiply(-1).add(90).multiply(d2r).cos()))
    y_coll = ee.ImageCollection(input_coll.map(
        lambda angle: ee.Image(angle).multiply(-1).add(90).multiply(d2r).sin()))
    return ee.Image(x_coll.reduce(ee.Reducer.sum()))\
        .atan2(ee.Image(y_coll.reduce(ee.Reducer.sum())))\
        .divide(d2r).multiply(-1).add(90)\
        .add(360).mod(360)


def delay_task(delay_time=0, task_max=-1, task_count=0):
    """Delay script execution based on number of READY tasks

    Parameters
    ----------
    delay_time : float, int
        Delay time in seconds between starting export tasks or checking the
        number of queued tasks if "ready_task_max" is > 0.  The default is 0.
        The delay time will be set to a minimum of 10 seconds if
        ready_task_max > 0.
    task_max : int, optional
        Maximum number of queued "READY" tasks.
    task_count : int
        The current/previous/assumed number of ready tasks.
        Value will only be updated if greater than or equal to ready_task_max.

    Returns
    -------
    int : ready_task_count

    """
    if task_max > 3000:
        raise ValueError('The maximum number of queued tasks must be less than 3000')

    # Force delay time to be a positive value since the parameter used to
    #   support negative values
    if delay_time < 0:
        delay_time = abs(delay_time)

    if ((task_max is None or task_max <= 0) and (delay_time >= 0)):
        # Assume task_max was not set and just wait the delay time
        logging.debug(f'  Pausing {delay_time} seconds, not checking task list')
        time.sleep(delay_time)
        return 0
    elif task_max and (task_count < task_max):
        # Skip waiting or checking tasks if a maximum number of tasks was set
        #   and the current task count is below the max
        logging.debug(f'  Ready tasks: {task_count}')
        return task_count

    # If checking tasks, force delay_time to be at least 10 seconds if
    #   ready_task_max is set to avoid excessive EE calls
    delay_time = max(delay_time, 10)

    # Make an initial pause before checking tasks lists to allow
    #   for previous export to start up
    # CGM - I'm not sure what a good default first pause time should be,
    #   but capping it at 30 seconds is probably fine for now
    logging.debug(f'  Pausing {min(delay_time, 30)} seconds for tasks to start')
    time.sleep(delay_time)

    # If checking tasks, don't continue to the next export until the number
    #   of READY tasks is greater than or equal to "ready_task_max"
    while True:
        ready_task_count = len(get_ee_tasks(states=['READY']).keys())
        logging.debug(f'  Ready tasks: {ready_task_count}')
        if ready_task_count >= task_max:
            logging.debug(f'  Pausing {delay_time} seconds')
            time.sleep(delay_time)
        else:
            logging.debug(f'  {task_max - ready_task_count} open task '
                          f'slots, continuing processing')
            break

    return ready_task_count


def get_ee_tasks(states=['RUNNING', 'READY'], verbose=False, retries=6):
    """Return current active tasks

    Parameters
    ----------
    states : list, optional
        List of task states to check (the default is ['RUNNING', 'READY']).
    verbose : bool, optional
        This parameter is deprecated and is no longer being used.
        To get verbose logging of the active tasks use utils.print_ee_tasks().
    retries : int, optional
        The number of times to retry getting the task list if there is an error.

    Returns
    -------
    dict : task descriptions (key) and full task info dictionary (value)

    """
    logging.debug('\nRequesting Task List')
    task_list = None
    for i in range(retries):
        try:
            # TODO: getTaskList() is deprecated, switch to listOperations()
            task_list = ee.data.getTaskList()
            # task_list = ee.data.listOperations()
            break
        except Exception as e:
            logging.warning(
                f'  Error getting task list, retrying ({i}/{retries})\n  {e}')
            time.sleep((i+1) ** 2)
    if task_list is None:
        raise Exception('\nUnable to retrieve task list, exiting')

    task_list = sorted(
        [task for task in task_list if task['state'] in states],
        key=lambda t: (t['state'], t['description'], t['id']))
    # task_list = sorted([
    #     [t['state'], t['description'], t['id']] for t in task_list
    #     if t['state'] in states])

    # Convert the task list to a dictionary with the task name as the key
    return {task['description']: task for task in task_list}


def arg_valid_date(input_date):
    """Check that a date string is ISO format (YYYY-MM-DD)

    This function is used to check the format of dates entered as command
      line arguments.
    DEADBEEF - It would probably make more sense to have this function
      parse the date using dateutil parser (http://labix.org/python-dateutil)
      and return the ISO format string

    Parameters
    ----------
    input_date : string

    Returns
    -------
    datetime

    Raises
    ------
    ArgParse ArgumentTypeError

    """
    try:
        return datetime.datetime.strptime(input_date, '%Y-%m-%d')
    except ValueError:
        msg = f'Not a valid date: "{input_date}".'
        raise argparse.ArgumentTypeError(msg)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest RTMA daily assets into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=arg_valid_date, metavar='DATE',
        default=(datetime.date.today() -
                 datetime.timedelta(days=START_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=arg_valid_date, metavar='DATE',
        default=(datetime.date.today() -
                 datetime.timedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d'),
        help='End date (format YYYY-MM-DD)')
    # parser.add_argument(
    #     '--rs', choices=['GRIDMET', 'NLDAS'], default='NLDAS',
    #     help='Solar radiation source dataset')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--delay', default=0, type=float,
        help='Delay (in seconds) between each export tasks')
    parser.add_argument(
        '--ready', default=-1, type=int,
        help='Maximum number of queued READY tasks')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    # # Build the image collection if it doesn't exist
    # logging.debug('Image Collection: {}'.format(ASSET_COLL_ID))
    # if not ee.data.getInfo(ASSET_COLL_ID):
    #     logging.info('\nImage collection does not exist and will be built'
    #                  '\n  {}'.format(ASSET_COLL_ID))
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    logging.debug('\nRequesting Task List')
    tasks = get_ee_tasks(states=['READY'])
    ready_task_count = len(tasks.keys())
    logging.info(f'  Tasks: {ready_task_count}')
    ready_task_count = delay_task(
        delay_time=0, task_max=args.ready, task_count=ready_task_count)

    ingest_dt_list = rtma_daily_asset_dates(
        args.start, args.end, overwrite_flag=args.overwrite)

    for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
        response = rtma_daily_asset_ingest(ingest_dt, overwrite_flag=args.overwrite)
        # logging.info(f'  {response}')

        # Pause before starting the next export task
        ready_task_count += 1
        ready_task_count = delay_task(
            delay_time=args.delay, task_max=args.ready,
            task_count=ready_task_count)