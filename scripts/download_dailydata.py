import json
import re
from .extract_dailydata import *
from .response import *
from .util import (response_download_file,
                   response_download_error)
from .dates import get_ncinfo_date
from app.scripts._global import GLOBAL_CONFIG

def download_analysis_dailydata(params):
    params = _get_varids_dailydata(params)

    if params['geomExtract'] == 'original':
        return _get_original_data(params)
    elif params['geomExtract'] == 'rectangle':
        if params['spatialAvg']:
            return _get_rectangle_point(params)
        else:
            return _get_rectangle_grid(params)
    elif params['geomExtract'] == 'points':
        return _get_multipoints_data(params)
    elif params['geomExtract'] == 'geojson':
        return _get_geojson_data(params)
    else:
        if params['spatialAvg']:
            return _get_polygons_points(params)
        else:
            return _get_polygons_grid(params)

def _get_original_data(params):
    return _wrap_download_gridded_data(
            extract_rectangular_grid_dailydata,
            params,
            bbox=None
        )

def _get_rectangle_grid(params):
    bbox = {
        k: float(params[k])
        for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
    }
    return _wrap_download_gridded_data(
            extract_rectangular_grid_dailydata,
            params,
            bbox=bbox
        )

def _get_rectangle_point(params):
    bbox = {
        k: float(params[k])
        for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
    }
    return _wrap_download_spoints_data(
            extract_rectangle_point_dailydata,
            params,
            bbox=bbox
        )

def _get_polygons_grid(params):
    return _wrap_download_gridded_data(
            extract_polygons_grid_dailydata,
            params
        )

def _get_polygons_points(params):
    return _wrap_download_spoints_data(
            extract_polygons_points_dailydata,
            params
        )

def _get_multipoints_data(params):
    return _wrap_download_spoints_data(
            extract_multipoints_dailydata,
            params
        )

def _get_geojson_data(params):
    return _wrap_download_spoints_data(
            extract_geojson_points_dailydata,
            params
        )

def _wrap_download_spoints_data(download_function, params, **kwargs):
    datainfo = _get_info_dailydata(params)
    filename = _get_dailydata_filename(params)
    out = download_function(params, **kwargs)
    if out['status'] == -1:
        return _format_down_data_error(
                out, params, filename
            )

    return _response_download_points(
            out, params, datainfo
        )

def _wrap_download_gridded_data(download_function, params, **kwargs):
    datainfo = _get_info_dailydata(params)
    season = _format_date_dailydata(params)
    filename = f"{params['seasParams']}_{season}"
    out = download_function(params, **kwargs)
    if out['status'] == -1:
        return _format_down_data_error(
                out, params, filename
            )

    return _response_download_grid(
            out['data'], params, datainfo
        )

def _response_download_grid(out, params, datainfo):
    season = _format_date_dailydata(params)
    filename = f"{params['seasParams']}_{season}"
    timeinfo = get_ncinfo_date('daily_season', season)

    if len(out) > 1:
        if params['finalOutput']:
            out_data = response_data_zip(
                out,
                params['outFormat'],
                datainfo,
                timeinfo,
                filename
            )
            resfile = f'{filename}.zip'
            mimetype = 'application/zip'
            bin_data = True
        else:
            return response_data_poly_json(out, datainfo)
    else:
        out = out[0]
        if out['poly']:
            poly = re.sub(r'[^a-zA-Z0-9]', '', out['poly'])
            out_file = f'{poly}_{filename}'
        else:
            out_file = filename

        if params['outFormat'] == 'netCDF-Format':
            out_data = response_data_nc(
                out, datainfo, timeinfo
            )
            resfile = f'{out_file}.nc'
            mimetype = 'application/netcdf'
            bin_data = True
        elif params['outFormat'] == 'JSON-Format':
            out_data = response_data_json(out, datainfo)
            resfile = f'{out_file}.json'
            mimetype = 'application/json'
            bin_data = False
        elif params['outFormat'] == 'CSV-Column-Format':
            out_data = response_data_csv(out, datainfo)
            resfile = f'{out_file}.csv'
            mimetype = 'text/csv'
            bin_data = False
        else:
            out_data = {
                'status': -1,
                'message': 'Unknown output format'
            }
            return _format_down_data_error(
                    out_data, params, out_file
                )

    if params['webApp']:
        if bin_data:
            out_data = out_data.getvalue()
            out_data = base64.b64encode(out_data).decode('utf-8')

        return json.dumps({
                'status': 0,
                'data': out_data,
                'filename': resfile,
                'mimetype': mimetype
            })
    else:
        return response_download_file(
                out_data, resfile, mimetype
            )

def _response_download_points(data_points, params, dataset):
    filename = _get_dailydata_filename(params)

    if params['outFormat'] == 'CSV-CDT-Format':
        coords = data_points['coords']
        coords['loc'] = [
            re.sub(r'[^a-zA-Z0-9]', '', p)
            for p in coords['loc']
        ]
        out_data = response_raw_points_cdt(
            data_points['data'],
            data_points['dates'],
            coords
        )
        filename = f'{filename}.csv'
        mimetype = 'text/csv'
    elif params['outFormat'] == 'JSON-Format':
        out_data = response_raw_points_json(
            data_points['data'],
            data_points['dates'],
            data_points['coords'],
            dataset
        )
        filename = f'{filename}.json'
        mimetype = 'application/json'
    else:
        out_data = {
            'status': -1,
            'message': 'Unknown output format'
        }
        return _format_down_data_error(
                out_data, params, filename
            )

    if params['webApp']:
        return json.dumps({
                'status': 0,
                'data': out_data,
                'filename': filename,
                'mimetype': mimetype
            })
    else:
        return response_download_file(
                out_data, filename, mimetype
            )

def _get_varids_dailydata(params):
    defvar = GLOBAL_CONFIG['datasets'][params['dataset']]['variables']
    if params['variable'] == 'temperature':
        if params['seasParams'] == 'MinTemp':
            params['varNames'] = [defvar['minimum_temperature']]
        elif params['seasParams'] == 'MaxTemp':
            params['varNames'] = [defvar['maximum_temperature']]
        elif params['seasParams'] == 'NumCD':
            params['varNames'] = [defvar['minimum_temperature']]
        elif params['seasParams'] == 'NumHD':
            params['varNames'] = [defvar['maximum_temperature']]
        else:
            params['varNames'] = [defvar['minimum_temperature'],
                                  defvar['maximum_temperature']]
        return params
    elif params['variable'] == 'rainfall':
        params['varNames'] = [defvar['rainfall']]
        return params
    else:
        msg = f"Unknown parameter <variable={params['variable']}>."
        return {'status': -1, 'message': msg}

def _format_down_data_error(out, params, filename):
    if params['finalOutput']:
        return response_download_error(
                out['message'], filename, 422
            )
    else:
        out['filename'] = filename
        return json.dumps(out)

def _get_info_dailydata(params):
    info = {
        'lon': 'lon', 'lat': 'lat',
        'time': 'time', 'missval': -99
    }
    if params['variable'] == 'rainfall':
        if params['seasParams'] == 'TotRain':
            info['out_varid'] = 'tot_rain'
            info['name'] = 'Total rainfall'
            info['units'] = 'mm'
            info['prec'] = 'float'
        elif params['seasParams'] == 'RainInt':
            info['out_varid'] = 'rain_int'
            info['name'] = 'Rainfall intensity'
            info['units'] = 'mm'
            info['prec'] = 'float'
        elif params['seasParams'] == 'NumWD':
            info['out_varid'] = 'nwd'
            info['name'] = 'Number of wet days'
            info['units'] = 'days'
            info['prec'] = 'integer'
        elif params['seasParams'] == 'NumDD':
            info['out_varid'] = 'ndd'
            info['name'] = 'Number of dry days'
            info['units'] = 'days'
            info['prec'] = 'integer'
        elif params['seasParams'] == 'NumDS':
            info['out_varid'] = 'nds'
            info['name'] = 'Number of dry spells'
            info['units'] = 'spells'
            info['prec'] = 'integer'
        elif params['seasParams'] == 'NumWS':
            info['out_varid'] = 'nws'
            info['name'] = 'Number of wety spells'
            info['units'] = 'spells'
            info['prec'] = 'integer'
        elif params['seasParams'] == 'LongDS':
            info['out_varid'] = 'lds'
            info['name'] = 'Longest dry spell'
            info['units'] = 'days'
            info['prec'] = 'integer'
        elif params['seasParams'] == 'LongWS':
            info['out_varid'] = 'lws'
            info['name'] = 'Longest wet spell'
            info['units'] = 'days'
            info['prec'] = 'integer'
        else:
            return None
    elif params['variable'] == 'temperature':
        if params['seasParams'] == 'MeanTemp':
            info['out_varid'] = 'tmean'
            info['name'] = 'Mean temperature'
            info['units'] = '°C'
            info['prec'] = 'float'
        elif params['seasParams'] == 'MinTemp':
            info['out_varid'] = 'tmin'
            info['name'] = 'Minimum temperature'
            info['units'] = '°C'
            info['prec'] = 'float'
        elif params['seasParams'] == 'MaxTemp':
            info['out_varid'] = 'tmax'
            info['name'] = 'Maximum temperature'
            info['units'] = '°C'
            info['prec'] = 'float'
        elif params['seasParams'] == 'NumCD':
            info['out_varid'] = 'ncd'
            info['name'] = 'Number of cold days'
            info['units'] = 'days'
            info['prec'] = 'integer'
        elif params['seasParams'] == 'NumHD':
            info['out_varid'] = 'nhd'
            info['name'] = 'Number of hot days'
            info['units'] = 'days'
            info['prec'] = 'integer'
        elif params['seasParams'] == 'CDD':
            info['out_varid'] = 'cdd'
            info['name'] = 'Chilling degree days'
            info['units'] = '°C-days'
            info['prec'] = 'float'
        elif params['seasParams'] == 'HDD':
            info['out_varid'] = 'hdd'
            info['name'] = 'Heating degree days'
            info['units'] = '°C-days'
            info['prec'] = 'float'
        elif params['seasParams'] == 'GDD':
            info['out_varid'] = 'gdd'
            info['name'] = 'Growing degree days'
            info['units'] = '°C-days'
            info['prec'] = 'float'
        else:
            return None
    else:
        return None
    return info

def _format_date_dailydata(params):
    s_year = int(params['Year'])
    s_mon = int(params['startMonth'])
    s_day = int(params['startDay'])
    e_mon = int(params['endMonth'])
    e_day = int(params['endDay'])

    same_year_seas = (s_mon, s_day) <= (e_mon, e_day)
    if same_year_seas:
        e_year = s_year
    else:
        e_year = s_year + 1

    start = f'{s_year}-{s_mon:02d}-{s_day:02d}'
    end = f'{e_year}-{e_mon:02d}-{e_day:02d}'
    return f'{start}_{end}'

def _get_dailydata_filename(params):
    period = f"{params['startDate']}-{params['endDate']}"
    s_mon = int(params['startMonth'])
    s_day = int(params['startDay'])
    e_mon = int(params['endMonth'])
    e_day = int(params['endDay'])
    seas1 = f'{s_mon:02d}-{s_day:02d}'
    seas2 = f'{e_mon:02d}-{e_day:02d}'
    filename = f"{params['seasParams']}_{period}_{seas1}_{seas2}"
    return filename
