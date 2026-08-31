from __future__ import annotations
from typing import Any, Mapping
import numpy as np
import pandas as pd
import xarray as xr

def xr_aggregate_data(
    xr_da: xr.DataArray,
    aggr_fun: str,
    in_res: str,
    out_res: str,
    min_frac: float = 1.0
) -> xr.DataArray:
    if in_res == 'daily':
        if out_res == 'monthly':
            return _xr_daily_to_monthly(xr_da, aggr_fun, min_frac)
        elif out_res == 'dekadal':
            return _xr_daily_to_dekadal(xr_da, aggr_fun, min_frac)
        else:
            raise ValueError(
                'Unknown output data temporal resolution'
            )
    elif in_res == 'dekadal':
        if out_res == 'monthly':
            return _xr_dekadal_to_monthly(xr_da, aggr_fun, min_frac)
    else:
        raise ValueError(
            'Unknown input data temporal resolution'
        )

def _xr_daily_to_dekadal(xr_da, aggr_fun, min_frac):
    day = xr_da.time.dt.day
    year = xr_da.time.dt.year
    month = xr_da.time.dt.month
    dekad = xr.where(
        day <= 10, 1,
        xr.where(day <= 20, 2, 3)
    )
    group = year * 1000 + month * 10 + dekad
    xr_da = xr_da.assign_coords(
        dekad_group=('time', group.values)
    )
    valid_count = (
        xr_da.notnull()
        .groupby('dekad_group')
        .count(dim='time')
    )
    days_in_month = xr_da.time.dt.days_in_month
    expected = xr.where(
        dekad <= 2, 10,
        days_in_month - 20
    )
    expected = expected.assign_coords(
        dekad_group=('time', group.values)
    )
    expected_count = (
        expected
        .groupby('dekad_group')
        .first()
    )
    frac_da = valid_count / expected_count

    xr_dek = xr_da.groupby('dekad_group')
    xr_dek = _xr_aggregate_fun(xr_dek, aggr_fun, 'time')
    xr_dek = xr_dek.where(
        frac_da >= min_frac, np.nan
    )

    # Build time coordinate at 6th, 16th, 26th
    grp = xr_dek.dekad_group.values
    years = grp // 1000
    months = (grp % 1000) // 10
    dekads = grp % 10
    days = np.where(
        dekads == 1, 6,
        np.where(dekads == 2, 16, 26)
    )
    time = pd.to_datetime({
        'year': years,
        'month': months,
        'day': days
    })
    return (
        xr_dek
        .assign_coords(time=('dekad_group', time))
        .swap_dims({'dekad_group': 'time'})
        .drop_vars('dekad_group')
    )

def _xr_daily_to_monthly(xr_da, aggr_fun, min_frac):
    expected = xr_da.time.dt.days_in_month
    expected_count = (
        expected
        .resample(time='MS')
        .first()
    )
    valid_count = (
        xr_da.notnull()
        .resample(time='MS')
        .sum()
    )
    frac_da = valid_count / expected_count

    xr_mon = xr_da.resample(time='MS')
    xr_mon = _xr_aggregate_fun(xr_mon, aggr_fun)
    return xr_mon.where(frac_da >= min_frac, np.nan)

def _xr_dekadal_to_monthly(xr_da, aggr_fun, min_frac):
    valid_count = (
        xr_da.notnull()
        .resample(time='MS')
        .sum()
    )
    frac_da = valid_count / 3

    xr_mon = xr_da.resample(time='MS')
    xr_mon = _xr_aggregate_fun(xr_mon, aggr_fun)
    return xr_mon.where(frac_da >= min_frac, np.nan)

def _xr_aggregate_fun(xr_da, aggr_fun, time_dim=None):
    if aggr_fun == 'sum':
        aggr = xr_da.sum(dim=time_dim, skipna=True)
    elif aggr_fun == 'mean':
        aggr = xr_da.mean(dim=time_dim, skipna=True)
    elif aggr_fun == 'median':
        aggr = xr_da.median(dim=time_dim, skipna=True)
    elif aggr_fun == 'min':
        aggr = xr_da.min(dim=time_dim, skipna=True)
    elif aggr_fun == 'max':
        aggr = xr_da.max(dim=time_dim, skipna=True)
    else:
        raise ValueError(
            'Unknown aggregation function'
        )
    return aggr
