import re
import json
import base64
from .extract_zarrclim import *
from .response import *
from .download_clim import (_clim_netcdf_info,
                            _clim_get_dataset,
                            _clim_get_filename,
                            _check_params_Poly,
                            _format_out_clim_error,
                            _response_out_clim_grid,
                            _response_out_clim_points)
from app.scripts._global import GLOBAL_CONFIG

def extract_climdata(params):
    if params['geomExtract'] == 'original':
        return _zarrclim_original_data(params)
    elif params['geomExtract'] == 'rectangle':
        if params['spatialAvg']:
            return _zarrclim_rectangle_point(params)
        else:
            return _zarrclim_rectangle_grid(params)
    elif params['geomExtract'] == 'points':
        return _zarrclim_multipoints_data(params)
    elif params['geomExtract'] == 'geojson':
        return _zarrclim_geojson_data(params)
    else:
        if params['spatialAvg']:
            return _zarrclim_polygons_points(params)
        else:
            return _zarrclim_polygons_grid(params)

def _zarrclim_original_data(params):
    return _wrap_zarrclim_gridded_data(zarrclim_gridded_data, params, bbox=None)

def _zarrclim_rectangle_grid(params):
    bbox = {k: float(params[k]) for k in ['minLon', 'maxLon', 'minLat', 'maxLat']}
    return _wrap_zarrclim_gridded_data(zarrclim_gridded_data, params, bbox=bbox)

def _zarrclim_polygons_grid(params):
    params = _check_params_Poly(params)
    return _wrap_zarrclim_gridded_data(zarrclim_polygons_grid_data, params)

def _zarrclim_rectangle_point(params):
    bbox = {k: float(params[k]) for k in ['minLon', 'maxLon', 'minLat', 'maxLat']}
    return _wrap_zarrclim_spoints_data(zarrclim_retangle_point_data, params, bbox=bbox)

def _zarrclim_polygons_points(params):
    params = _check_params_Poly(params)
    return _wrap_zarrclim_spoints_data(zarrclim_polygons_point_data, params)

def _zarrclim_multipoints_data(params):
    return _wrap_zarrclim_spoints_data(zarrclim_multipoints_data, params)

def _zarrclim_geojson_data(params):
    return _wrap_zarrclim_spoints_data(zarrclim_geojson_data, params)

def _wrap_zarrclim_gridded_data(clim_function, params, **kwargs):
    dataset = _clim_get_dataset(params)
    filename = _clim_get_filename(params)
    out_clim = clim_function(params, dataset, **kwargs)
    if out_clim['status'] == -1:
        return _format_out_clim_error(out_clim, params, filename)

    return _response_out_clim_grid(out_clim['data'], params)

def _wrap_zarrclim_spoints_data(clim_function, params, **kwargs):
    dataset = _clim_get_dataset(params)
    filename = _clim_get_filename(params)
    out_clim = clim_function(params, dataset, **kwargs)
    if out_clim['status'] == -1:
        return _format_out_clim_error(out_clim, params, filename)

    return _response_out_clim_points(out_clim, params)
