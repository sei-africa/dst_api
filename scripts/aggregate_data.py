import numpy as np
import xarray as xr
import pandas as pd
from scipy.stats import t as student_t

def aggregate_climatology(
    xr_ds,
    clim_fun,
    min_year,
    percentile=0.95,
    frequency_oper='>=',
    frequency_thres=1.0,
    proba_thres=10,
    proba_unit='perc',
    trend_unit='perYear',
):
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
        rzr = np.logical_and(mn != 0.0, clim.notnull())
        clim = clim.where(rzr, 0.0)
    elif clim_fun == 'frequency':
        mask_f = f'xr_ds{frequency_oper}{frequency_thres}'
        mask = eval(mask_f)
        mask = mask.sum(dim='y', skipna=True)
        clim = 100 * mask / nomiss
    elif clim_fun == 'mean-stdev':
        mn = xr_ds.mean(dim='y', skipna=True)
        sd = xr_ds.std(dim='y', skipna=True)
        clim = xr.concat(
            [mn, sd],
            dim=xr.DataArray(
                [0, 1],
                dims="statistics",
                name="statistics",
                attrs={"long_name": "statistics", "labels": "0=mean, 1=stdev"},
            ),
        )
    elif clim_fun == 'probExc':
        # clim = probability_exceeding0(xr_ds, proba_thres, proba_unit)
        clim = probability_exceeding(xr_ds, proba_thres, proba_unit)
    elif clim_fun == 'probNoExc':
        # clim = probability_exceeding0(xr_ds, proba_thres, proba_unit)
        clim = probability_exceeding(xr_ds, proba_thres, proba_unit)
        if proba_unit == 'perc':
            clim = 100.0 - clim
        else:
            clim = 1.0 - clim
    elif clim_fun == 'trend':
        clim = regression_vector(xr_ds, min_year)
        if trend_unit == 'overPeriod':
            yr = xr_ds['y'].values
            clim.loc[dict(metric='slope')] *= len(yr)
        if trend_unit == 'percPeriod':
            yr = xr_ds['y'].values
            moy = xr_ds.mean(dim='y', skipna=True)
            moy = 100 * len(yr) / moy
            clim.loc[dict(metric="slope")] *= moy
    else:
        clim = None

    return clim.where(nomiss >= min_year, np.nan)

def aggregate_timeSeries(
    xr_ds, aggr_fun, aggr_len, min_frac,
    count_oper='>=', count_thres=1.0
):
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
            val = xr_ds[0, :, :]
            val[:, :] = np.nan
        else:
            val = np.nan

    mfrac = xr_ds.isnull().sum(dim='time') / aggr_len
    return val.where(mfrac < min_frac, np.nan)

def probability_exceeding0(xr_ds, thres, unit='perc'):
    if unit == 'perc':
        mul = 100.0
    else:
        mul = 1.0

    return mul * (xr_ds > thres).mean(dim='y')

def probability_exceeding(xr_ds, thres, unit='perc'):
    xr_ds = xr_ds.chunk({'y': -1})
    if unit == 'perc':
        mul = 100.0
    else:
        mul = 1.0

    def _prob_vec(v):
        v = v[~np.isnan(v)]
        if v.size < 3:
            return np.nan

        v = np.sort(v)
        n = v.size

        # Weibull plotting position for exceedance probability
        pr = np.arange(n, 0, -1) / (n + 1)

        # Remove duplicates for interpolation
        v_unique, idx = np.unique(v, return_index=True)
        pr_unique = pr[idx]

        if thres < v[0]:
            if n > 1:
                step = np.median(np.diff(v))
            else:
                step = 0

            x1 = v[0] - step
            if thres < x1:
                return mul

            return mul * (
                pr[0] +
                (thres - v[0]) * (1.0 - pr[0]) / (x1 - v[0])
            )

        if thres > v[-1]:
            if n > 1:
                step = np.median(np.diff(v))
            else:
                step = 0

            x0 = v[-1] + step
            if thres > x0:
                return 0.0

            return mul * (
                pr[-1] +
                (thres - v[-1]) * (-pr[-1]) / (x0 - v[-1])
            )

        return mul * np.interp(
            thres,
            v_unique,
            pr_unique
        )

    return xr.apply_ufunc(
            _prob_vec,
            xr_ds,
            input_core_dims=[['y']],
            output_core_dims=[[]],
            vectorize=True,
            dask='parallelized',
            output_dtypes=[np.float32],
        )

def regression_vector(xr_ds, min_len):
    metrics = [
        'slope',
        'std.slope',
        't-value.slope',
        'p-value.slope',
        'intercept',
        'std.intercept',
        't-value.intercept',
        'p-value.intercept',
        'R2',
        'sigma',
    ]

    y = xr_ds.chunk({'y': -1})
    x = y['y'].values.astype(float)

    valid_x = ~np.isnan(x)
    x_valid = x[valid_x]

    mean_x = np.nanmean(x_valid)
    var_x = np.nanvar(x_valid, ddof=1)

    def _regress_one(v):
        out = np.full(10, np.nan, dtype=np.float32)

        v = v.astype(float)
        v[~valid_x] = np.nan

        valid = ~np.isnan(v)
        n = valid.sum()

        if n < min_len or n < 3 or not np.isfinite(var_x) or var_x == 0:
            return out

        yy = v[valid]
        xx = x[valid]

        mean_y = np.mean(yy)
        var_y = np.var(yy, ddof=1)

        x1 = xx - mean_x
        y1 = yy - mean_y

        cov_xy = np.sum(x1 * y1) / (n - 1)

        slope = cov_xy / var_x
        intercept = mean_y - slope * mean_x

        fitted = slope * xx + intercept
        sse = np.sum((fitted - yy) ** 2)
        mse = sse / (n - 2)
        sigma = np.sqrt(mse)

        sxx = (n - 1) * var_x

        std_slope = sigma / np.sqrt(sxx)
        std_intercept = sigma * np.sqrt(
            (1 / n) + (mean_x**2 / sxx)
        )

        t_slope = slope / std_slope
        t_intercept = intercept / std_intercept

        df = n - 2
        p_slope = 2 * student_t.cdf(-abs(t_slope), df)
        p_intercept = 2 * student_t.cdf(-abs(t_intercept), df)

        r2 = cov_xy**2 / (var_x * var_y) if var_y > 0 else np.nan

        out[:] = [
            slope,
            std_slope,
            t_slope,
            p_slope,
            intercept,
            std_intercept,
            t_intercept,
            p_intercept,
            r2,
            sigma,
        ]

        return out

    result = xr.apply_ufunc(
        _regress_one,
        y,
        input_core_dims=[['y']],
        output_core_dims=[['metric']],
        vectorize=True,
        dask='parallelized',
        output_dtypes=[np.float32],
        dask_gufunc_kwargs={
            'output_sizes': {'metric': 10}
        },
    )

    result = result.assign_coords(metric=metrics)
    result = result.transpose('metric', 'lat', 'lon')

    return result
