import json
import re
import base64
import copy
from .extract_dailyanom import *
from .download_dailydata import (_get_info_dailydata, 
                                 _get_varids_dailydata,
                                 _get_dailydata_filename)
from .download_dailyclim import _check_params_Poly
# from .response import *
from .response import response_anomaly_points_cdt
from .util import (response_download_file,
                   response_download_error)
from app.scripts._cache import cache, hash_pamars_anom

# app.dst_api.scripts

def download_analysis_dailyanom(params):
    params = _get_varids_dailydata(params)

    if params['geomExtract'] == 'original':
        return _anom_original_data(params)
    elif params['geomExtract'] == 'rectangle':
        if params['spatialAvg']:
            return _anom_rectangle_point(params)
        else:
            return _anom_rectangle_grid(params)
    elif params['geomExtract'] == 'points':
        return _anom_multipoints_data(params)
    elif params['geomExtract'] == 'geojson':
        return _anom_geojson_data(params)
    else:
        if params['spatialAvg']:
            return _anom_polygons_points(params)
        else:
            return _anom_polygons_grid(params)

def _anom_original_data(params):
    return _wrap_anom_gridded_data(
            anomaly_gridded_dailydata,
            params,
            bbox=None
        )

def _anom_rectangle_grid(params):
    bbox = {
        k: float(params[k])
        for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
    }
    return _wrap_anom_gridded_data(
            anomaly_gridded_dailydata,
            params,
            bbox=bbox
        )

def _anom_rectangle_point(params):
    bbox = {
        k: float(params[k])
        for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
    }
    return _wrap_anom_spoints_data(
            anomaly_rectangle_point_dailydata,
            params,
            bbox=bbox
        )

def _anom_polygons_grid(params):
    params = _check_params_Poly(params)
    return _wrap_anom_gridded_data(
            anomaly_polygons_grid_dailydata,
            params
        )

def _anom_polygons_points(params):
    params = _check_params_Poly(params)
    return _wrap_anom_spoints_data(
            anomaly_polygons_point_dailydata,
            params
        )

def _anom_multipoints_data(params):
    return _wrap_anom_spoints_data(
            anomaly_multipoints_dailydata,
            params
        )

def _anom_geojson_data(params):
    return _wrap_anom_spoints_data(
            anomaly_geojson_dailydata,
            params
        )

def _wrap_anom_gridded_data(anom_function, params, **kwargs):
    filename = _clim_get_filename(params)
    datainfo = _clim_netcdf_info(params)

    cache_key = hash_pamars_clim(params)
    cached_data = cache.get(cache_key)

    if cached_data is None:
        out_clim = anom_function(params, datainfo, **kwargs)
        if out_clim['status'] == -1:
            return _format_out_clim_error(
                        out_clim, params, filename
                    )
        cached_data = out_clim['data']
        cache.set(cache_key, cached_data)

    return _response_out_clim_grid(cached_data, params)

def _wrap_anom_spoints_data(anom_function, params, **kwargs):
    datainfo = _get_info_dailydata(params)
    filename = _get_dailydata_filename(params)
    filename = f'anomaly_{filename}'

    cache_key = hash_pamars_anom(params)
    cached_data = cache.get(cache_key)

    if cached_data is None:
        anomaly_data = anom_function(params, datainfo, **kwargs)
        if anomaly_data['status'] == -1:
            return _format_out_anom_error(
                    anomaly_data, params, filename
                )

        cached_data = get_anomaly_multipoints_dailydata(
            anomaly_data, params['anomaly'], datainfo
        )
        cache.set(cache_key, cached_data)

    return _response_anomaly_points(cached_data, params)


########################

def _response_anomaly_points(anom_data, params):
    filename = _get_dailydata_filename(params)
    filename = f'anomaly_{filename}'

    if params['outFormat_0'] == 'CSV-CDT-Format':
        out_data = response_anomaly_points_cdt(anom_data)
        filename = f'{filename}.csv'
        mimetype = 'text/csv'
    elif params['outFormat_0'] == 'JSON-Format':
        out_data = json.dumps(anom_data)
        filename = f'{filename}.json'
        mimetype = 'application/json'
    else:
        out_data = {
            'status': -1,
            'message': 'Unknown output format'
        }
        return response_download_error(
                out_data['message'], filename, 422
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

def _format_out_anom_error(anom_data, params, filename):
    if params['finalOutput']:
        return response_download_error(
                anom_data['message'], filename, 422
            )
    else:
        anom_data['filename'] = filename
        return json.dumps(anom_data)
