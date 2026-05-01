import numpy as np
import xarray as xr
import pandas as pd

def aggregate_climatology(xr_ds, clim_fun, min_year,
                          percentile=0.95,
                          frequency_oper='>=',
                          frequency_thres=1.0):
    np.seterr(divide='ignore', invalid='ignore')
    nomiss = xr_ds.notnull().sum(dim='y')

    if clim_fun == 'mean':
        clim = xr_ds.mean(dim='y', skipna=True)
    elif clim_fun == 'median':
        clim = xr_ds.median(dim='y', skipna=True)
    elif clim_fun == 'min':
        clim = xr_ds.min(dim='y', skipna=True)
    elif clim_fun == 'max':
        clim = xr_ds.max(dim='y', skipna=True)
    elif clim_fun == 'stdev':
        clim = xr_ds.std(dim='y', skipna=True)
    elif clim_fun == 'percentile':
        clim = xr_ds.quantile(q=percentile, dim='y', skipna=True)
    elif clim_fun == 'cv':
        mn = xr_ds.mean(dim='y', skipna=True)
        std = xr_ds.std(dim='y', skipna=True)
        clim = (std / mn) * 100
        rzr = np.logical_and(mn != 0., clim.notnull())
        clim = clim.where(rzr, 0.)
    elif clim_fun == 'frequency':
        mask_f = f'xr_ds{frequency_oper}{frequency_thres}'
        mask = eval(mask_f)
        mask = mask.sum(dim='y', skipna=True)
        clim = 100 * mask/nomiss
    elif clim_fun == 'mean-stdev':
        mn = xr_ds.mean(dim='y', skipna=True)
        sd = xr_ds.std(dim='y', skipna=True)
        clim = xr.concat(
            [mn, sd],
            pd.Index(
                ['mean', 'stdev'],
                name='statistics',
                 dtype='<U10'
            )
        )
    else:
        clim = None

    return clim.where(nomiss >= min_year, np.nan)

def aggregate_timeSeries(xr_ds, aggr_fun, aggr_len, min_frac,
                         count_oper='>=', count_thres=1.0):
    if aggr_fun == 'sum':
        val = xr_ds.sum(dim='time', skipna=True)
    elif aggr_fun == 'mean':
        val = xr_ds.mean(dim='time', skipna=True)
    elif aggr_fun == 'median':
        val = xr_ds.median(dim='time', skipna=True)
    elif aggr_fun == 'min':
        val = xr_ds.min(dim='time', skipna=True)
    elif aggr_fun == 'max':
        val = xr_ds.max(dim='time', skipna=True)
    elif aggr_fun == 'count':
        mask_f = f'xr_ds{count_oper}{count_thres}'
        mask = eval(mask_f)
        val = mask.sum(dim='time', skipna=True)
    else:
        if len(xr_ds.shape) == 3:
            val = xr_ds[0,:,:]
            val[:,:] = np.nan
        else:
            val = np.nan

    mfrac = xr_ds.isnull().sum(dim='time') / aggr_len
    return val.where(mfrac < min_frac, np.nan)
