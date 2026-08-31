import json
import copy
import numpy as np
import pandas as pd
import xarray as xr

from app.scripts._global import GLOBAL_CONFIG
from app.scripts._cache import cache, hash_distr_pamars_spei

from .zarrdata import get_zarr_dataset
from .dates import convert_strings_npdatetime64
from .shapefiles import get_shapefiles_data

from .spei_spatial import (
    spei_aggregate_data,
    spei_compute_params,
    spei_spatial_computation
)
from .aggregate_dataarray import xr_aggregate_data


# from app.dst_api.scripts.dates import convert_strings_npdatetime64
# from app.dst_api.scripts import (
#     get_zarr_dataset,
#     spei_aggregate_data,
#     convert_strings_npdatetime64,
#     xr_aggregate_data,
# )
# from app.dst_api.scripts.spei_spatial import (
#     spei_aggregate_data,
#     spei_compute_params,
#     spei_spatial_computation
# )

def get_spi_data(params):
    if params['gridded']:
        return get_spi_spatial_data(params)
    else:
        return None

def get_spi_spatial_data(params):
    cache_key = hash_distr_pamars_spei(params)
    distr_pars = cache.get(cache_key)

    if distr_pars is None:
        try:
            distr_pars = _spi_distribution_pars(params)
            distr_pars = distr_pars.compute(
                scheduler='single-threaded'
            )
        except Exception as e:
            return {'status': -1, 'message': str(e)}
        cache.set(cache_key, distr_pars)

    try:
        spi_data = _spi_spatial_data(distr_pars, params)
    except Exception as e:
        return {'status': -1, 'message': str(e)}

    return {'status': 0, 'data': spi_data}

def _spi_spatial_data(distr_pars, params):
    spi_args = _format_spei_args(params)
    precip, _ = _get_spei_data(params)

    this_date = convert_strings_npdatetime64(
        params['Date'],
        params['temporalRes'],
        sep = '-'
    ).astype('datetime64[ns]')

    if spi_args['time_res'] == 'dekadal':
        xr_ds = precip.sel(time=this_date)
    else:
        if spi_args['tscale'] == 1:
            xr_ds = precip.sel(time=this_date)
        else:
            prev_date = pd.DateOffset(months=spi_args['tscale'])
            start_date = pd.DatetimeIndex(this_date)
            start_date = (start_date - prev_date).to_numpy()
            xr_ds = precip.sel(time=slice(start_date[0], this_date[0]))

    precip_aggr = spei_aggregate_data(
        xr_ds, spi_args['tscale']
    )
    spi = spei_spatial_computation(
        precip_aggr,
        distr_pars,
        spi_args['tscale'],
        spi_args['frequency'],
        spi_args['distribution'],
        spi_args['time_res'],
        params['analysis']
    )

    if params['geomExtract'] == 'original':
        return _spei_gridded_data(spi)

    if params['geomExtract'] == 'rectangle':
        bbox = {
            k: float(params[k])
            for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
        }
        spi = spi.sel(
            lon=slice(bbox['minLon'], bbox['maxLon']),
            lat=slice(bbox['minLat'], bbox['maxLat'])
        )
        return _spei_gridded_data(spi)

    if params['geomExtract'] == 'polygons':
        shpObj = get_shapefiles_data(params)
        if shpObj['status'] == -1: return shpObj

        multipolygons = False
        if type(shpObj['polys']) is list:
            if len(shpObj['polys']) > 1:
                multipolygons = True
            else:
                shpObj['polys'] = shpObj['polys'][0]

        # here shp extraction
        return _spei_gridded_data(spi)

def _spei_gridded_data(spei):
    out = {}
    if spei.attrs['time_resolution'] == 'monthly':
        out['Date'] = spei.time.dt.strftime('%Y-%m').values[0]

    if spei.attrs['time_resolution'] == 'dekadal':
        yymm = spei['time'].dt.strftime('%Y-%m').values[0]
        dekad = xr.where(
            spei['time'].dt.day <= 10, 1,
            xr.where(spei['time'].dt.day <= 20, 2, 3)
        )
        out['Date'] = f'{yymm}-{dekad.values[0]}'

    out['Latitude'] = spei['lat'].round(6).values.tolist()
    out['Longitude'] = spei['lon'].round(6).values.tolist()
    out['Dimensions'] = {
        'Latitude': spei.sizes['lat'],
        'Longitude': spei.sizes['lon']
    }

    miss = -9999.0
    out['Missing'] = miss
    out['Data'] = spei.fillna(miss).values.tolist()

    out['VariableVarId'] = spei.name
    out['VariableName'] = spei.attrs['long_name']
    out['VariableUnits'] = spei.attrs['units']
    return out

def _spi_distribution_pars(params):
    precip, _ = _get_spei_data(params)
    spi_args = _format_spei_args(params)
    precip_aggr = spei_aggregate_data(precip, spi_args['tscale'])
    return spei_compute_params(
        precip_aggr,
        spi_args['tscale'],
        spi_args['frequency'],
        spi_args['distribution'],
        spi_args['time_res'],
        min_non_na=5
    )

def _format_spei_args(params):
    tscale = int(params['timeScale'])
    timeres = str(params['temporalRes'])
    distribution = str(params.get('distribution', 'gamma'))
    if timeres == 'dekadal' and tscale > 1:
        raise ValueError('Time scale must be 1 for dekadal data')
    if timeres == 'dekadal':
        frequency = 36
    else:
        frequency = 12

    return {
        'tscale': tscale,
        'time_res': timeres,
        'distribution': distribution,
        'frequency': frequency
    }

def _get_spei_data(params):
    data_sets = GLOBAL_CONFIG['datasets'][params['dataset']]
    dset_vars = data_sets['variables']
    params_data = {
        k: params[k]
        for k in ['temporalRes', 'dataset']
    }

    params_precip = params_data.copy()
    params_precip['variable'] = dset_vars['rainfall']
    info_precip = data_sets[params_precip['temporalRes']]['netcdf']
    info_precip = info_precip[params_precip['variable']]
    precip = get_zarr_dataset(params_precip)
    precip_da = precip[params_precip['variable']]
    if info_precip['compute']:
        precip_da = xr_aggregate_data(
            precip_da,
            info_precip['function'],
            info_precip['input'],
            params['temporalRes'],
            info_precip['minfrac']
        )

    et0_da = None
    if params['analysis'] == 'spei':
        params_et0 = params_data.copy()
        if 'reference_evapotranspiration' not in dset_vars:
            raise ValueError('No evapotranspiration data found.')
        params_et0['variable'] = dset_vars['reference_evapotranspiration']
        info_et0 = data_sets[params_et0['temporalRes']]['netcdf']
        info_et0 = info_et0[params_et0['variable']]
        et0 = get_zarr_dataset(params_et0)
        et0_da = et0['et0']
        if info_et0['compute']:
            et0_da = xr_aggregate_data(
                et0_da,
                info_et0['function'],
                info_et0['input'],
                params['temporalRes'],
                info_et0['minfrac']
            )

    return precip_da, et0_da
