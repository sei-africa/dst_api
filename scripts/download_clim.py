import re
import json
import base64
import copy
from .extract_clim import *
from .response import *
from .util import (response_download_file,
                   response_download_error)
from app.scripts._global import GLOBAL_CONFIG
from app.scripts._cache import cache, hash_pamars_clim

def download_climdata(params):
    if params['geomExtract'] == 'original':
        return _clim_original_data(params)
    elif params['geomExtract'] == 'rectangle':
        if params['spatialAvg']:
            return _clim_rectangle_point(params)
        else:
            return _clim_rectangle_grid(params)
    elif params['geomExtract'] == 'points':
        return _clim_multipoints_data(params)
    elif params['geomExtract'] == 'geojson':
        return _clim_geojson_data(params)
    else:
        if params['spatialAvg']:
            return _clim_polygons_points(params)
        else:
            return _clim_polygons_grid(params)

def _clim_netcdf_info(params):
    dataset = _clim_get_dataset(params)
    keys = ['name', 'varid', 'units', 'missval']
    info = {key: dataset[key] for key in keys}

    if params['climFunction'] == 'mean':
        info['name'] = f"Average, {info['name']} Climatology"
    elif params['climFunction'] == 'median':
        info['name'] = f"Median, {info['name']} Climatology"
    elif params['climFunction'] == 'min':
        info['name'] = f"Minimum, {info['name']} Climatology"
    elif params['climFunction'] == 'max':
        info['name'] = f"Maximum, {info['name']} Climatology"
    elif params['climFunction'] == 'stdev':
        info['name'] = f"Standard Deviation, {info['name']} Climatology"
        info['units'] = ''
    elif params['climFunction'] == 'percentile':
        if type(params['precentileValue']) is list:
            info['name'] = [f"{p}th Percentile, {info['name']}" for p in params['precentileValue']]
            info['out_varid'] = [f"{info['varid']}_{p}" for p in params['precentileValue']]
        else:
            info['name'] = f"{params['precentileValue']}th Percentile, {info['name']}"
    elif params['climFunction'] == 'cv':
        info['name'] = f"Coefficient of Variation, {info['name']} Climatology"
        info['units'] = '%'
    elif params['climFunction'] == 'frequency':
        info['name'] = f"Frequency of {info['name']} {params['frequencyOper']} {params['frequencyThres']}"
        info['units'] = '%'
    elif params['climFunction'] == 'mean-stdev':
        info['name'] = [f"Average, {info['name']} Climatology",
                        f"Standard Deviation, {info['name']} Climatology"]
        info['out_varid'] = ['mean', 'stdev']
    else:
        info['name'] = None

    return info

def _clim_get_dataset(params):
    datasets = GLOBAL_CONFIG['datasets'][params['dataset']]
    out = datasets[params['temporalRes']]['netcdf'][params['variable']]
    out_copy = copy.deepcopy(out)
    return out_copy

def _clim_get_filename(params):
    period = f"{params['startYear']}-{params['endYear']}"
    var_time = f"{params['variable']}_{params['temporalRes']}"
    filename = f"clim_{var_time}_{period}_{params['climFunction']}"
    if params['climFunction'] == 'percentile':
        perc = params['precentileValue']
        if type(perc) is list:
            perc = [str(p) for p in perc]
            perc = '-'.join(perc)

        filename = f"{filename}-{perc}"
    return filename

def _check_params_Poly(params):
    if 'Poly' in params:
        if type(params['Poly']) is not list:
            params['Poly'] = [params['Poly']]
    return params

def _format_out_clim_error(out_clim, params, filename):
    if params['finalOutput']:
        return response_download_error(out_clim['message'], filename, 422)
    else:
        out_clim['filename'] = filename
        return json.dumps(out_clim)

def _clim_original_data(params):
    return _wrap_clim_gridded_data(climatology_gridded_data, params, bbox=None)

def _clim_rectangle_grid(params):
    bbox = {k: float(params[k]) for k in ['minLon', 'maxLon', 'minLat', 'maxLat']}
    return _wrap_clim_gridded_data(climatology_gridded_data, params, bbox=bbox)

def _clim_polygons_grid(params):
    params = _check_params_Poly(params)
    return _wrap_clim_gridded_data(climatology_polygons_grid_data, params)

def _clim_rectangle_point(params):
    bbox = {k: float(params[k]) for k in ['minLon', 'maxLon', 'minLat', 'maxLat']}
    return _wrap_clim_spoints_data(climatology_retangle_point_data, params, bbox=bbox)

def _clim_polygons_points(params):
    params = _check_params_Poly(params)
    return _wrap_clim_spoints_data(climatology_polygons_point_data, params)

def _clim_multipoints_data(params):
    return _wrap_clim_spoints_data(climatology_multipoints_data, params)

def _clim_geojson_data(params):
    return _wrap_clim_spoints_data(climatology_geojson_data, params)

def _wrap_clim_gridded_data(clim_function, params, **kwargs):
    dataset = _clim_get_dataset(params)
    filename = _clim_get_filename(params)

    cache_key = hash_pamars_clim(params)
    cached_data = cache.get(cache_key)

    if cached_data is None:
        out_clim = clim_function(params, dataset, **kwargs)
        if out_clim['status'] == -1:
            return _format_out_clim_error(out_clim, params, filename)
        cached_data = out_clim['data']
        cache.set(cache_key, cached_data)

    return _response_out_clim_grid(cached_data, params)

def _wrap_clim_spoints_data(clim_function, params, **kwargs):
    dataset = _clim_get_dataset(params)
    filename = _clim_get_filename(params)

    cache_key = hash_pamars_clim(params)
    cached_data = cache.get(cache_key)

    if cached_data is None:
        cached_data = clim_function(params, dataset, **kwargs)
        if cached_data['status'] == -1:
            return _format_out_clim_error(cached_data, params, filename)
        cache.set(cache_key, cached_data)

    return _response_out_clim_points(cached_data, params)

def _response_out_clim_points(cached_data, params):
    filename = _clim_get_filename(params)
    ncinfo = _clim_netcdf_info(params)

    if params['outFormat'] == 'CSV-CDT-Format':
        coords = cached_data['coords']
        coords['loc'] = [re.sub(r'[^a-zA-Z0-9]', '', p) for p in coords['loc']]
        out_data = response_clim_points_cdt(cached_data['data'], cached_data['dates'], coords)
        filename = f'{filename}.csv'
        mimetype = 'text/csv'
    elif params['outFormat'] == 'JSON-Format':
        out_data = response_clim_points_json(cached_data['data'], cached_data['dates'],
                                    cached_data['coords'], ncinfo, cached_data['mpars'])
        filename = f'{filename}.json'
        mimetype = 'application/json'
    else:
        out_err = {'status': -1, 'message': 'Unknown output format'}
        return _format_out_clim_error(out_err, params, filename)

    if params['webApp']:
        return json.dumps({'status': 0, 'data': out_data, 'filename': filename})
    else:
        return response_download_file(out_data, filename, mimetype)

def _response_out_clim_grid(cached_data, params):
    filename = _clim_get_filename(params)
    ncinfo = _clim_netcdf_info(params)

    if len(cached_data) > 1:
        if params['finalOutput']:
            out_data = response_clim_zip(cached_data, params['outFormat'], ncinfo, filename)
            resfile = f'{filename}.zip'
            mimetype = 'application/zip'
            bin_data = True
        else:
            return response_clim_poly_json(cached_data, ncinfo)
    else:
        cached_data = cached_data[0]
        if cached_data['poly']:
            poly = re.sub(r'[^a-zA-Z0-9]', '', cached_data['poly'])
            out_file = f'{poly}_{filename}'
        else:
            out_file = filename

        if params['outFormat'] == 'netCDF-Format':
            out_data = response_clim_nc(cached_data, ncinfo)
            resfile = f'{out_file}.nc'
            mimetype = 'application/netcdf'
            bin_data = True
        elif params['outFormat'] == 'JSON-Format':
            out_data = response_clim_json(cached_data, ncinfo)
            resfile = f'{out_file}.json'
            mimetype = 'application/json'
            bin_data = False
        elif params['outFormat'] == 'CSV-Column-Format':
            out_data = response_clim_csv(cached_data, ncinfo)
            resfile = f'{out_file}.csv'
            mimetype = 'text/csv'
            bin_data = False
        else:
            out_err = {'status': -1, 'message': 'Unknown output format'}
            return _format_out_clim_error(out_err, params, filename)

    if params['webApp']:
        if bin_data:
            out_data = out_data.getvalue()
            out_data = base64.b64encode(out_data).decode('utf-8')

        return json.dumps({'status': 0, 'data': out_data, 'binary': bin_data,
                           'filename': resfile, 'mimetype': mimetype})
    else:
        return response_download_file(out_data, resfile, mimetype)
