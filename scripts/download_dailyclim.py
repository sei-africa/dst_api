import json
import re
import json
import base64
import copy
from .extract_dailyclim import *
from .download_dailydata import (_get_info_dailydata, 
                                 _get_varids_dailydata)
from .response import *
from .util import (response_download_file,
                   response_download_error)
from app.scripts._global import GLOBAL_CONFIG
from app.scripts._cache import cache, hash_pamars_clim

def download_analysis_dailyclim(params):
    params = _get_varids_dailydata(params)

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

def _clim_original_data(params):
    return _wrap_clim_gridded_data(
            climatology_gridded_dailydata,
            params,
            bbox=None
        )

def _clim_rectangle_grid(params):
    bbox = {
        k: float(params[k])
        for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
    }
    return _wrap_clim_gridded_data(
            climatology_gridded_dailydata,
            params,
            bbox=bbox
        )

def _clim_rectangle_point(params):
    bbox = {
        k: float(params[k])
        for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
    }
    return _wrap_clim_spoints_data(
            climatology_rectangle_point_dailydata,
            params,
            bbox=bbox
        )

def _clim_polygons_grid(params):
    params = _check_params_Poly(params)
    return _wrap_clim_gridded_data(
            climatology_polygons_grid_dailydata,
            params
        )

def _clim_polygons_points(params):
    params = _check_params_Poly(params)
    return _wrap_clim_spoints_data(
            climatology_polygons_point_dailydata,
            params
        )

def _clim_multipoints_data(params):
    return _wrap_clim_spoints_data(
            climatology_multipoints_dailydata,
            params
        )

def _clim_geojson_data(params):
    return _wrap_clim_spoints_data(
            climatology_geojson_dailydata,
            params
        )

def _wrap_clim_gridded_data(clim_function, params, **kwargs):
    filename = _clim_get_filename(params)
    datainfo = _clim_netcdf_info(params)

    cache_key = hash_pamars_clim(params)
    cached_data = cache.get(cache_key)

    if cached_data is None:
        out_clim = clim_function(params, datainfo, **kwargs)
        if out_clim['status'] == -1:
            return _format_out_clim_error(
                        out_clim, params, filename
                    )
        cached_data = out_clim['data']
        cache.set(cache_key, cached_data)

    return _response_out_clim_grid(cached_data, params)

def _wrap_clim_spoints_data(clim_function, params, **kwargs):
    filename = _clim_get_filename(params)
    datainfo = _clim_netcdf_info(params)

    cache_key = hash_pamars_clim(params)
    cached_data = cache.get(cache_key)

    if cached_data is None:
        cached_data = clim_function(params, datainfo, **kwargs)
        if cached_data['status'] == -1:
            return _format_out_clim_error(
                    cached_data, params, filename
                )
        cache.set(cache_key, cached_data)

    return _response_out_clim_points(cached_data, params)

def _response_out_clim_grid(cached_data, params):
    filename = _clim_get_filename(params)
    ncinfo = _clim_netcdf_info(params)

    if len(cached_data) > 1:
        if params['finalOutput']:
            out_data = response_clim_zip(
                cached_data,
                params['outFormat'],
                ncinfo,
                filename
            )
            resfile = f'{filename}.zip'
            mimetype = 'application/zip'
            bin_data = True
        else:
            return response_clim_poly_json(
                    cached_data, ncinfo
                )
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
            out_err = {
                'status': -1,
                'message': 'Unknown output format'
            }
            return _format_out_clim_error(
                    out_err, params, filename
                )

    if params['webApp']:
        if bin_data:
            out_data = out_data.getvalue()
            out_data = base64.b64encode(out_data).decode('utf-8')

        return json.dumps({
                'status': 0,
                'data': out_data,
                'binary': bin_data,
                'filename': resfile,
                'mimetype': mimetype
            })
    else:
        return response_download_file(
                out_data, resfile, mimetype
            )

def _response_out_clim_points(cached_data, params):
    filename = _clim_get_filename(params)
    ncinfo = _clim_netcdf_info(params)

    if params['outFormat'] == 'CSV-CDT-Format':
        coords = cached_data['coords']
        coords['loc'] = [re.sub(r'[^a-zA-Z0-9]', '', p) for p in coords['loc']]
        out_data = response_clim_points_cdt(
            cached_data['data'],
            cached_data['dates'],
            coords
        )
        filename = f'{filename}.csv'
        mimetype = 'text/csv'
    elif params['outFormat'] == 'JSON-Format':
        out_data = response_clim_points_json(
            cached_data['data'],
            cached_data['dates'],
            cached_data['coords'],
            ncinfo,
            cached_data['mpars']
        )
        filename = f'{filename}.json'
        mimetype = 'application/json'
    else:
        out_err = {
            'status': -1,
            'message': 'Unknown output format'
        }
        return _format_out_clim_error(
                out_err, params, filename
            )

    if params['webApp']:
        return json.dumps({
                'status': 0,
                'data': out_data,
                'filename': filename
            })
    else:
        return response_download_file(
                out_data, filename, mimetype
            )

def _clim_get_filename(params):
    period = f"{params['startYear']}-{params['endYear']}"
    p1 = params['seasParams']
    p2 = params['seasStats']
    filename = f"clim_{p1}_{period}_{p2}"
    return filename

def _clim_netcdf_info(params):
    datainfo = _get_info_dailydata(params)
    keys = ['name', 'out_varid', 'units', 'missval']
    info = {key: datainfo[key] for key in keys}

    if params['seasStats'] == 'mean':
        info['name'] = f"Average, {info['name']} Climatology"
    elif params['seasStats'] == 'median':
        info['name'] = f"Median, {info['name']} Climatology"
    elif params['seasStats'] == 'stdev':
        info['name'] = f"Standard Deviation, {info['name']} Climatology"
    elif params['seasStats'] == 'cv':
        info['name'] = f"Coefficient of Variation, {info['name']} Climatology"
        info['units'] = '%'
    elif params['seasStats'] == 'probExc':
        info['name'] = f"Probability of exceeding {params['probaThres']}, {info['name']}"
        info['units'] = '%' if params['probaUnit'] == 'perc' else ''
        info['out_varid'] = 'proba_exceeding'
    elif params['seasStats'] == 'probNoExc':
        info['name'] = f"Probability of non-exceeding {params['probaThres']}, {info['name']}"
        info['units'] = '%' if params['probaUnit'] == 'perc' else ''
        info['out_varid'] = 'proba_non_exceeding'
    else:
        info['name'] = None

    return info

def _format_out_clim_error(out_clim, params, filename):
    if params['finalOutput']:
        return response_download_error(
                out_clim['message'], filename, 422
            )
    else:
        out_clim['filename'] = filename
        return json.dumps(out_clim)

def _check_params_Poly(params):
    if 'Poly' in params:
        if type(params['Poly']) is not list:
            params['Poly'] = [params['Poly']]
    return params
