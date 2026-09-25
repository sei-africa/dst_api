import numpy as np
import pandas as pd
import xarray as xr

from app.scripts._global import GLOBAL_CONFIG
from app.scripts._cache import (
    cache,
    hash_pamars_spei,
    hash_distr_pamars_spei
)

from .zarrdata import get_zarr_dataset
from .dates import convert_strings_npdatetime64
from .shapefiles import (
    get_shapefiles_data,
    format_bbox_polygons,
    extract_polygons_griddata
)
from .netcdf import extract_netcdf_bbox

from .spei_functions import *
from .aggregate_dataarray import xr_aggregate_data

########
# from app.dst_api.scripts.dates import convert_strings_npdatetime64
# from app.dst_api.scripts import (
#     get_zarr_dataset,
#     spei_aggregate_data,
#     convert_strings_npdatetime64,
#     xr_aggregate_data,
# )
# from app.dst_api.scripts.spei_functions import *
# from app.dst_api.scripts.shapefiles import (
#     get_shapefiles_data,
#     format_bbox_polygons,
#     extract_polygons_griddata
# )
# from app.dst_api.scripts.netcdf import extract_netcdf_bbox

########

def get_spi_data(params):
    cache_key = hash_pamars_spei(params)
    # cache.delete(cache_key)
    spi_data = cache.get(cache_key)

    if spi_data is None:
        if params['gridded']:
            distr_pars = get_spi_distribution_pars(params)
            if distr_pars['status'] == -1: return distr_pars
            try:
                spi_data = _spei_spatial_data(distr_pars['data'], params)
            except Exception as e:
                return {'status': -1, 'message': str(e)}
        else:
            spi_data = None

        cache.set(cache_key, spi_data)

    return {'status': 0, 'data': spi_data}

def get_spi_distribution_pars(params):
    cache_key = hash_distr_pamars_spei(params)
    # cache.delete(cache_key)
    distr_pars = cache.get(cache_key)

    if distr_pars is None:
        try:
            distr_pars = _spei_distribution_pars(params)
        except Exception as e:
            return {'status': -1, 'message': str(e)}

        cache.set(cache_key, distr_pars)

    return {'status': 0, 'data': distr_pars}

def _spei_spatial_data(distr_pars, params):
    precip, et0 = _get_spei_data(params)
    spei_args = SPEI_Wrapper_setup(precip, params)
    if params['analysis'] == 'spei':
        data = precip - et0
    else:
        data = precip

    if spei_args['time_res'] == 'dekadal':
        this_date = convert_strings_npdatetime64(
            params['Date'],
            params['temporalRes'],
            sep = '-'
        ).astype('datetime64[ns]')
        xr_ds = data.sel(time=this_date)
    else:
        if spei_args['tscale'] == 1:
            this_date = convert_strings_npdatetime64(
                params['Date'],
                params['temporalRes'],
                sep = '-',
                mon_day=1
            ).astype('datetime64[ns]')
            xr_ds = data.sel(time=this_date)
        else:
            seas_dates = convert_strings_npdatetime64(
                params['Date'].split('_'),
                'monthly',
                sep = '-',
                mon_day=1
            ).astype('datetime64[ns]')
            xr_ds = data.sel(time=slice(seas_dates[0], seas_dates[1]))

    data_aggr = xr_ds.sum(
        dim='time',
        skipna=True,
        keepdims=True
    ).assign_coords(
        time=[xr_ds.time.values[-1]]
    )

    spei = SPEI_Computation_sp(
        data_aggr,
        distr_pars,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        spei_args['time_res'],
        params['analysis']
    )

    if params['geomExtract'] == 'original':
        return _spei_gridded_data(spei)

    if params['geomExtract'] == 'rectangle':
        bbox = {
            k: float(params[k])
            for k in ['minLon', 'maxLon', 'minLat', 'maxLat']
        }
        spei = spei.sel(
            lon=slice(bbox['minLon'], bbox['maxLon']),
            lat=slice(bbox['minLat'], bbox['maxLat'])
        )
        return _spei_gridded_data(spei)

    if params['geomExtract'] == 'polygons':
        shpObj = get_shapefiles_data(params)
        if shpObj['status'] == -1: return shpObj

        multipolygons = False
        if type(shpObj['polys']) is list:
            if len(shpObj['polys']) > 1:
                multipolygons = True
            else:
                shpObj['polys'] = shpObj['polys'][0]

        np_spei = {
            'lon': spei['lon'].values,
            'lat': spei['lat'].values,
            'data': np.squeeze(spei.values)
        }
        info_spei = {
            'date': params['Date'],
            'varid': spei.name,
            'long_name': spei.attrs['long_name'],
            'units': spei.attrs['units']
        }

        if multipolygons:
            out_spei = []
            for poly in shpObj['polys']:
                bbox = format_bbox_polygons(
                    shpObj['bbox'],
                    params['shpField'],
                    poly
                )
                ret = extract_netcdf_bbox(np_spei, bbox)
                ext = extract_polygons_griddata(
                    ret,
                    shpObj['shp'],
                    params['shpField'],
                    poly
                )
                ext['poly'] = poly
                ext = ext | info_spei
                out_spei += [_np_spei_gridded_data(ext)]
        else:
            out = extract_polygons_griddata(
                np_spei,
                shpObj['shp'],
                params['shpField'],
                shpObj['polys']
            )
            out['poly'] = shpObj['polys']
            out = out | info_spei
            out_spei = _np_spei_gridded_data(out)

        return out_spei

def _spei_distribution_pars(params):
    precip, et0 = _get_spei_data(params)
    spei_args = SPEI_Wrapper_setup(precip, params)
    if params['analysis'] == 'spei':
        data = precip - et0
    else:
        data = precip
    data_aggr = SPEI_Aggregate_data(
        data, spei_args['tscale']
    )
    return SPEI_Compute_params(
        data_aggr,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        min_non_na=5
    )

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

def _np_spei_gridded_data(np_spei):
    out = {}
    out['Date'] = np_spei['date']
    out['Latitude'] = np.round(np_spei['lat'], 6).tolist()
    out['Longitude'] = np.round(np_spei['lon'], 6).tolist()
    out['Dimensions'] = {
        'Latitude': len(np_spei['lat']),
        'Longitude': len(np_spei['lon'])
    }

    miss = -9999.0
    out['Missing'] = miss
    data_filled = np.ma.filled(np_spei['data'], miss)
    data_filled = np.nan_to_num(
        data_filled, nan=miss, posinf=miss, neginf=miss
    )
    out['Data'] = data_filled.tolist()

    out['VariableVarId'] = np_spei['varid']
    out['VariableName'] = np_spei['long_name']
    out['VariableUnits'] = np_spei['units']
    out['Name'] = np_spei['poly']
    return out

def _get_spei_data(params):
    data_sets = GLOBAL_CONFIG['datasets'][params['dataset']]
    dset_vars = data_sets['variables']
    params_data = {
        k: params[k]
        for k in ['temporalRes', 'dataset']
    }
    if 'timeRes' in params:
        params_data['temporalRes'] = params['timeRes']

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
            params_precip['temporalRes'],
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
                params_et0['temporalRes'],
                info_et0['minfrac']
            )
        precip_da, et0_da = xr.align(
            precip_da, et0_da, join='inner'
        )

    return precip_da, et0_da
