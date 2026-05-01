import json
import re
import copy
from .extract_data import *
from .response import *
from .util import (response_download_file,
                   response_download_error)
from .dates import get_ncinfo_date, format_output_date
from app.scripts._global import GLOBAL_CONFIG

def download_rawdata(params):
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

def _get_download_dataset(params):
    datasets = GLOBAL_CONFIG['datasets'][params['dataset']]
    tmp = datasets[params['temporalRes']]
    out = tmp['netcdf'][params['variable']]
    out['compute'] = tmp['compute']
    out_copy = copy.deepcopy(out)
    return out_copy

def _format_down_data_error(out, params, filename):
    if params['finalOutput']:
        return response_download_error(out['message'], filename, 422)
    else:
        out['filename'] = filename
        return json.dumps(out)

def _get_original_data(params):
    return _wrap_download_gridded_data(extract_rectangular_grid_data, params, bbox=None)

def _get_rectangle_grid(params):
    bbox = {k: float(params[k]) for k in ['minLon', 'maxLon', 'minLat', 'maxLat']}
    return _wrap_download_gridded_data(extract_rectangular_grid_data, params, bbox=bbox)

def _get_polygons_grid(params):
    return _wrap_download_gridded_data(extract_polygons_grid_data, params)

def _get_rectangle_point(params):
    bbox = {k: float(params[k]) for k in ['minLon', 'maxLon', 'minLat', 'maxLat']}
    return _wrap_download_spoints_data(extract_rectangle_point_data, params, bbox=bbox)

def _get_polygons_points(params):
    return _wrap_download_spoints_data(extract_polygons_points_data, params)

def _get_multipoints_data(params):
    return _wrap_download_spoints_data(extract_multipoints_data, params)

def _get_geojson_data(params):
    return _wrap_download_spoints_data(extract_geojson_points_data, params)

def _wrap_download_spoints_data(download_function, params, **kwargs):
    dataset = _get_download_dataset(params)
    period = format_output_date(params)
    filename = f"{params['variable']}_{params['temporalRes']}_{period}"
    out = download_function(params, dataset, **kwargs)
    if out['status'] == -1:
        return _format_down_data_error(out, params, filename)

    return _response_download_points(out, params, dataset)

def _wrap_download_gridded_data(download_function, params, **kwargs):
    dataset = _get_download_dataset(params)
    filename = f"{params['variable']}_{params['temporalRes']}_{params['Date']}"
    out = download_function(params, dataset, filename, **kwargs)
    if out['status'] == -1:
        return _format_down_data_error(out, params, filename)

    return _response_download_grid(out['data'], params, dataset)

def _response_download_points(data_points, params, dataset):
    period = format_output_date(params)
    filename = f"{params['variable']}_{params['temporalRes']}_{period}"

    if params['outFormat'] == 'CSV-CDT-Format':
        coords = data_points['coords']
        coords['loc'] = [re.sub(r'[^a-zA-Z0-9]', '', p) for p in coords['loc']]
        out_data = response_raw_points_cdt(data_points['data'], data_points['dates'], coords)
        filename = f'{filename}.csv'
        mimetype = 'text/csv'
    elif params['outFormat'] == 'JSON-Format':
        out_data = response_raw_points_json(data_points['data'], data_points['dates'],
                                            data_points['coords'], dataset)
        filename = f'{filename}.json'
        mimetype = 'application/json'
    else:
        out_data = {'status': -1, 'message': 'Unknown output format'}
        return _format_down_data_error(out_data, params, filename)

    if params['webApp']:
        return json.dumps({'status': 0, 'data': out_data,
                           'filename': filename, 'mimetype': mimetype})
    else:
        return response_download_file(out_data, filename, mimetype)

def _response_download_grid(out, params, dataset):
    filename = f"{params['variable']}_{params['temporalRes']}_{params['Date']}"
    timeinfo = get_ncinfo_date(params['temporalRes'], params['Date'])

    if len(out) > 1:
        if params['finalOutput']:
            out_data = response_data_zip(out, params['outFormat'], dataset, timeinfo, filename)
            resfile = f'{filename}.zip'
            mimetype = 'application/zip'
            bin_data = True
        else:
            return response_data_poly_json(out, dataset)
    else:
        out = out[0]
        if out['poly']:
            poly = re.sub(r'[^a-zA-Z0-9]', '', out['poly'])
            out_file = f'{poly}_{filename}'
        else:
            out_file = filename

        if params['outFormat'] == 'netCDF-Format':
            out_data = response_data_nc(out, dataset, timeinfo)
            resfile = f'{out_file}.nc'
            mimetype = 'application/netcdf'
            bin_data = True
        elif params['outFormat'] == 'JSON-Format':
            out_data = response_data_json(out, dataset)
            resfile = f'{out_file}.json'
            mimetype = 'application/json'
            bin_data = False
        elif params['outFormat'] == 'CSV-Column-Format':
            out_data = response_data_csv(out, dataset)
            resfile = f'{out_file}.csv'
            mimetype = 'text/csv'
            bin_data = False
        else:
            out_data = {'status': -1, 'message': 'Unknown output format'}
            return _format_down_data_error(out_data, params, out_file)

    if params['webApp']:
        if bin_data:
            out_data = out_data.getvalue()
            out_data = base64.b64encode(out_data).decode('utf-8')

        return json.dumps({'status': 0, 'data': out_data,
                           'filename': resfile, 'mimetype': mimetype})
    else:
        return response_download_file(out_data, resfile, mimetype)
