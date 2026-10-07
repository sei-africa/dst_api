from __future__ import annotations
from typing import Mapping
from collections.abc import Mapping
import calendar
import os
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import numpy as np
import xarray as xr
from scipy import special, stats

def SPEI_computation_wrapper(
    precip: xr.DataArray,
    params: Mapping[str, object],
    etp: xr.DataArray | None = None
) -> xr.DataArray:
    spei_args = SPEI_Wrapper_setup(precip, params)
    if params['analysis'] == 'spei':
        precip, etp = xr.align(precip, etp, join='inner')
        data = SPEI_Aggregate_data(
            precip - etp, spei_args['tscale']
        )
    elif params['analysis'] == 'spi':
        data = SPEI_Aggregate_data(
            precip, spei_args['tscale']
        )
    else:
        return None

    fitted = SPEI_Compute_params(
        data,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        min_non_na=5
    )
    spei = SPEI_Computation_ts(
        data,
        fitted,
        spei_args['tscale'],
        spei_args['frequency'],
        spei_args['distribution'],
        spei_args['time_res'],
        params['analysis']
    )
    start = spei_args['tscale'] - 1
    return spei.isel(time=slice(start, None))

def SPEI_Aggregate_data(
    data: xr.DataArray,
    tscale: int = 1
) -> xr.DataArray:
    values = _dataarray_values(data)
    if tscale < 1:
        raise ValueError('tscale must be at least 1')
    if tscale == 1:
        return data.astype(float).copy(deep=True)

    out = np.full(values.shape, np.nan)
    for k in range(
        max(0, values.shape[0] - tscale + 1)
    ):
        out[k + tscale - 1] = np.nansum(
            values[k : k + tscale],
            axis=0
        )
    return xr.DataArray(
        out.reshape(data.shape),
        coords=data.coords,
        dims=data.dims,
        name=data.name,
        attrs=data.attrs
    )

def SPEI_Wrapper_setup(
    precip: xr.DataArray,
    params: Mapping[str, object]
) -> tuple[int, int]:
    # _dataarray_values(precip)
    tscale = int(params['timeScale'])
    if 'timeRes' in params:
        timeres = str(params['timeRes'])
    else:
        timeres = str(params['temporalRes'])

    if timeres not in ('monthly', 'dekadal'):
        raise ValueError("time_res must be 'monthly' or 'dekadal'")
    if timeres == 'dekadal' and tscale > 1:
        raise ValueError('Time scale must be 1 for dekadal data')

    nominal = 36 if timeres == 'dekadal' else 12
    frequency = min(nominal, max(0, precip.sizes['time'] - tscale + 1))
    distribution = str(params.get('distribution', 'gamma'))
    return {
        'tscale': tscale,
        'time_res': timeres,
        'distribution': distribution,
        'frequency': frequency
    }

def SPEI_Compute_params(
    data: xr.DataArray,
    tscale: int = 1,
    frequency: int = 12,
    distribution: str = 'gamma',
    min_non_na: int = 5,
    n_jobs: int | None = None
) -> np.ndarray:
    values = _dataarray_values(data)
    if frequency < 1:
        raise ValueError('frequency must be at least 1')
    params = np.full(
        (frequency, values.shape[1]),
        None,
        dtype=object
    )
    eligible = np.flatnonzero(
        np.sum(~np.isnan(values), axis=0) >= min_non_na
    )

    if distribution in ('peasron3', 'llogistic'):
        _store_optimizer_params_parallel(
            params, values, eligible, tscale, frequency,
            distribution, min_non_na, n_jobs
        )
        return params
    else:
        for k in range(frequency):
            rows = np.arange(
                k + tscale - 1,
                values.shape[0],
                frequency
            )
            if rows.size == 0:
                continue

            if distribution == 'zscore':
                for start in range(0, eligible.size, 32_768):
                    columns = eligible[start:start + 32_768]
                    _store_zscore_params(
                        params[k], values[np.ix_(rows, columns)],
                        columns, min_non_na
                    )
                continue

            if distribution == 'gamma':
                # Chunking caps temporary memory while retaining fast vectorized
                # operations over tens of thousands of grid cells at a time.
                for start in range(0, eligible.size, 32_768):
                    columns = eligible[start:start + 32_768]
                    _store_gamma_params(
                        params[k], values[np.ix_(rows, columns)],
                        columns, min_non_na
                    )
                continue

            for j in eligible:
                sample = values[rows, j]
                sample = sample[~np.isnan(sample)]
                if sample.size < min_non_na:
                    continue

                fitted = _fit_distribution(
                    sample, distribution, min_non_na
                )
                if fitted is not None:
                    params[k, j] = fitted
        return params

def _store_optimizer_params_parallel(
    params: np.ndarray,
    values: np.ndarray,
    eligible: np.ndarray,
    tscale: int,
    frequency: int,
    distribution: str,
    min_non_na: int,
    n_jobs: int | None
) -> None:
    """
    Run independent SciPy optimizer fits in a reusable process pool.
    """
    workers = (
        min(8, os.cpu_count() or 1)
        if n_jobs is None
        else int(n_jobs)
    )
    if workers < 1:
        raise ValueError('n_jobs must be at least 1')
    if workers == 1 or eligible.size == 0:
        for k in range(frequency):
            rows = np.arange(k + tscale - 1, values.shape[0], frequency)
            if rows.size:
                columns, fitted = _fit_optimizer_block(
                    values[np.ix_(rows, eligible)], eligible,
                    distribution, min_non_na
                )
                _merge_fitted_params(params[k], columns, fitted)
        return

    # About eight jobs per worker balances uneven optimizer runtimes while
    # avoiding millions of tiny futures on large rasters.
    chunk_size = min(
        4096,
        max(256, (eligible.size + workers * 8 - 1) // (workers * 8))
    )
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for k in range(frequency):
            rows = np.arange(k + tscale - 1, values.shape[0], frequency)
            if rows.size == 0:
                continue
            starts = iter(range(0, eligible.size, chunk_size))
            pending = {}

            def submit_next() -> bool:
                try:
                    start = next(starts)
                except StopIteration:
                    return False
                columns = eligible[start:start + chunk_size]
                future = executor.submit(
                    _fit_optimizer_block,
                    values[np.ix_(rows, columns)], columns,
                    distribution, min_non_na
                )
                pending[future] = None
                return True

            for _ in range(workers * 2):
                if not submit_next():
                    break
            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    del pending[future]
                    columns, fitted = future.result()
                    _merge_fitted_params(params[k], columns, fitted)
                    submit_next()

def _fit_optimizer_block(
    seasonal: np.ndarray,
    columns: np.ndarray,
    distribution: str,
    min_non_na: int
) -> tuple[np.ndarray, list[dict[str, float] | None]]:
    """Worker entry point for a block of independent optimizer fits."""
    fitted = []
    for index in range(seasonal.shape[1]):
        sample = seasonal[:, index]
        sample = sample[~np.isnan(sample)]
        result = None
        if sample.size >= min_non_na:
            if distribution == 'peasron3':
                pzero = float(np.mean(sample == 0))
                fit_values = sample[sample > 0]
            else:
                pzero = None
                fit_values = sample
            result = _fit_distribution(
                fit_values, distribution, min_non_na
            )
            if result is not None and pzero is not None:
                result['pzero'] = pzero
        fitted.append(result)
    return columns, fitted

def _merge_fitted_params(
    target: np.ndarray,
    columns: np.ndarray,
    fitted: list[dict[str, float] | None]
) -> None:
    for column, result in zip(columns, fitted):
        if result is not None:
            target[column] = result

def _store_zscore_params(
    target: np.ndarray,
    seasonal: np.ndarray,
    columns: np.ndarray,
    min_non_na: int
) -> None:
    """
    Fit all z-score columns with NumPy instead of a Python fit loop.
    """
    present = ~np.isnan(seasonal)
    count = present.sum(axis=0)
    valid = count >= min_non_na
    if not np.any(valid):
        return
    clean = np.where(present, seasonal, 0.0)
    mean = np.divide(
        clean.sum(axis=0), count,
        out=np.full(columns.size, np.nan), where=count > 0
    )
    centered = np.where(present, seasonal - mean[None, :], 0.0)
    variance = np.divide(
        (centered * centered).sum(axis=0), count - 1,
        out=np.full(columns.size, np.nan), where=count > 1
    )
    sd = np.sqrt(variance)
    for index in np.flatnonzero(valid):
        target[columns[index]] = {
            'mean': float(mean[index]),
            'sd': float(sd[index])
        }

def _store_gamma_params(
    target: np.ndarray,
    seasonal: np.ndarray,
    columns: np.ndarray,
    min_non_na: int
) -> None:
    """
    Fit fixed-location gamma distributions for a complete spatial row.

    ``scipy.stats.gamma.fit(..., floc=0)`` solves the same likelihood
    equation separately for every series.  Solving that equation in arrays
    removes thousands of Python calls and scalar root-finder invocations.
    """
    present = ~np.isnan(seasonal)
    count = present.sum(axis=0)
    valid = count >= min_non_na
    if not np.any(valid):
        return

    pzero = np.divide(
        np.count_nonzero(present & (seasonal == 0), axis=0),
        count,
        out=np.zeros(columns.size, dtype=float),
        where=count > 0
    )
    positive = np.isfinite(seasonal) & (seasonal > 0)
    npositive = positive.sum(axis=0)
    fit = valid & (npositive >= min_non_na)
    if not np.any(fit):
        return

    selected = seasonal[:, fit]
    selected_positive = positive[:, fit]
    n = npositive[fit].astype(float)
    total = np.where(selected_positive, selected, 0.0).sum(axis=0)
    logs = np.zeros_like(selected)
    np.log(selected, out=logs, where=selected_positive)
    log_total = logs.sum(axis=0)
    mean = total / n
    equation = np.log(mean) - log_total / n

    # A constant positive series has equation == 0 and no finite MLE.  Keep
    # the previous deterministic jitter behavior for this rare case.
    regular = np.isfinite(equation) & (equation > 1e-14)
    shape = np.full(equation.shape, np.nan)
    if np.any(regular):
        e = equation[regular]
        estimate = (3.0 - e + np.sqrt((e - 3.0) ** 2 + 24.0 * e)) / (
            12.0 * e
        )
        for _ in range(12):
            step = (
                np.log(estimate) - special.digamma(estimate) - e
            ) / (1.0 / estimate - special.polygamma(1, estimate))
            updated = estimate - step
            updated = np.where(updated > 0, updated, estimate / 2.0)
            if np.all(np.abs(updated - estimate) <= 1e-12 * updated):
                estimate = updated
                break
            estimate = updated
        shape[regular] = estimate

    fit_indices = np.flatnonzero(fit)
    for local_index in np.flatnonzero(~regular):
        sample = selected[selected_positive[:, local_index], local_index]
        fitted = _fit_distribution(sample, 'gamma', min_non_na)
        if fitted is not None:
            shape[local_index] = fitted['shape']
            mean[local_index] = fitted['shape'] * fitted['scale']

    fitted_ok = np.isfinite(shape) & (shape > 0)
    for local_index in np.flatnonzero(fitted_ok):
        index = fit_indices[local_index]
        target[columns[index]] = {
            'shape': float(shape[local_index]),
            'loc': 0.0,
            'scale': float(mean[local_index] / shape[local_index]),
            'pzero': float(pzero[index])
        }

def SPEI_Computation_ts(
    data_aggr: xr.DataArray,
    distr_pars: np.ndarray,
    tscale: int = 1,
    frequency: int = 12,
    distribution: str = 'gamma',
    time_res: str = 'monthly',
    spei_type: str = 'spi'
) -> xr.DataArray:
    values_array = _dataarray_values(data_aggr)
    out = np.full(values_array.shape, np.nan)
    for k in range(frequency):
        rows = np.arange(
            k + tscale - 1,
            values_array.shape[0],
            frequency
        )
        for j in range(values_array.shape[1]):
            values = values_array[rows, j]
            valid = ~np.isnan(values)
            pars = distr_pars[k, j]
            if distribution == 'zscore':
                if pars is not None:
                    z = (values - pars['mean']) / pars['sd']
                    z[~np.isfinite(z)] = 0
                    out[rows, j] = z
                continue
            if pars is None or np.sum(valid) < 4:
                continue
            probability = _cdf_ts(values[valid], pars, distribution)
            if distribution in ('gamma', 'peasron3'):
                pzero = pars['pzero']
                probability = pzero + (1.0 - pzero) * probability
            z = stats.norm.ppf(probability)
            z[np.isneginf(z)] = -5
            z[np.isposinf(z)] = 5
            target = np.full(values.shape, np.nan)
            target[valid] = z
            out[rows, j] = target

    out[out > 5] = 5
    out[out < -5]  = -5
    attrs = dict(data_aggr.attrs)
    long_name = (
        'Standardized Precipitation Index'
        if spei_type == 'spi'
        else 'Standardized Precipitation Evapotranspiration Index'
    )
    attrs.update({
        'long_name': long_name,
        'units': '',
        'distribution': distribution,
        'time_scale': tscale,
        'time_resolution': time_res
    })
    return xr.DataArray(
        out.reshape(data_aggr.shape),
        coords=data_aggr.coords,
        dims=data_aggr.dims,
        name=spei_type,
        attrs=attrs
    )

def SPEI_Computation_sp(
    data_aggr: xr.DataArray,
    distr_pars: np.ndarray,
    tscale: int = 1,
    frequency: int = 12,
    distribution: str = 'gamma',
    time_res: str = 'monthly',
    spei_type: str = 'spi'
) -> xr.DataArray:
    values_array = _dataarray_values(data_aggr)
    out = np.full(values_array.shape, np.nan)
    index_pars = _season_index(data_aggr, time_res)

    for k in range(frequency):
        if k + 1 not in index_pars:
            continue
        rows = np.where(index_pars == k + 1)[0]
        values = values_array[rows, ]
        season_pars = distr_pars[k, ]

        keys_pars = next(x for x in season_pars if x is not None)
        season_pars = {
            key: np.array([
                x[key] if x is not None else np.nan
                for x in season_pars
            ])
            for key in keys_pars
        }

        if distribution == 'zscore':
            z = (
                values - season_pars['mean'][None, :]
            ) / season_pars['sd'][None, :]
            z[~np.isfinite(z)] = 0
            out[rows, :] = z
        else:
            probability = _cdf_sp(
                values, season_pars, distribution
            )
            if distribution in ('gamma', 'peasron3'):
                pzero = season_pars['pzero'][None, :]
                probability = pzero + (1.0 - pzero) * probability

            z = stats.norm.ppf(probability)
            z[np.isneginf(z)] = -5
            z[np.isposinf(z)] = 5
            out[rows, ] = z

    attrs = dict(data_aggr.attrs)
    long_name = (
        'Standardized Precipitation Index'
        if spei_type == 'spi'
        else 'Standardized Precipitation Evapotranspiration Index'
    )
    attrs.update({
        'long_name': long_name,
        'units': '',
        'distribution': distribution,
        'time_scale': tscale,
        'time_resolution': time_res
    })
    return xr.DataArray(
        out.reshape(data_aggr.shape),
        coords=data_aggr.coords,
        dims=data_aggr.dims,
        name=spei_type,
        attrs=attrs
    )

def SPEI_distribution_params(
    distr_pars: np.ndarray,
    template: xr.DataArray,
    parameter: str,
    period: int,
    time_res: str = 'monthly',
) -> xr.DataArray:
    """
    Extract one fitted parameter for one month/dekad as a spatial grid.
    ``period`` is 1-based: 1--12 for monthly data and 1--36 for dekadal
    data. For example, ``parameter="shape", period=1`` extracts the gamma
    shape parameter for January (or the first dekad when dekadal).
    """
    # _dataarray_values(template)
    if template.ndim != 3 or template.dims[1:] != ('lat', 'lon'):
        raise ValueError(
            "template dimensions must be ('time', 'lat', 'lon')"
        )
    if time_res not in ('monthly', 'dekadal'):
        raise ValueError(
            "time_res must be 'monthly' or 'dekadal'"
        )
    expected_periods = 12 if time_res == 'monthly' else 36
    if not 1 <= period <= expected_periods:
        raise ValueError(
            f'period must be between 1 and {expected_periods}'
        )
    if distr_pars.ndim != 2 or distr_pars.shape[0] < period:
        raise ValueError(
            'distr_pars does not contain the requested period'
        )

    spatial_shape = (
        template.sizes['lat'],
        template.sizes['lon']
    )
    if distr_pars.shape[1] != int(np.prod(spatial_shape)):
        raise ValueError(
            'distr_pars spatial size does not match the template grid'
        )

    values = np.full(distr_pars.shape[1], np.nan, dtype=float)
    for index, fitted in enumerate(distr_pars[period - 1]):
        if fitted is not None and parameter in fitted:
            values[index] = fitted[parameter]

    if time_res == 'monthly':
        period_label = calendar.month_name[period]
    else:
        month = (period - 1) // 3 + 1
        dekad = (period - 1) % 3 + 1
        period_label = f'{calendar.month_name[month]} dekad {dekad}'

    return xr.DataArray(
        values.reshape(spatial_shape),
        dims=('lat', 'lon'),
        coords={
            'lat': template['lat'],
            'lon': template['lon']
        },
        name=parameter,
        attrs={
            'long_name': f'{parameter} parameter for {period_label}'
        }
    )

def _dataarray_values(
    data: xr.DataArray
) -> np.ndarray:
    if not isinstance(data, xr.DataArray):
        raise TypeError('data must be an xarray.DataArray')
    if data.ndim < 2:
        raise ValueError(
            'data must have a time dimension and at least one spatial dimension'
        )
    if data.dims[0] != 'time':
        raise ValueError("the first DataArray dimension must be 'time'")
    values = np.asarray(data.values, dtype=float)
    return values.reshape(values.shape[0], -1)

def _season_index(
    data: xr.DataArray,
    time_res: str
) -> np.ndarray:
    if time_res == 'monthly':
        season = data.time.dt.month
    elif time_res == 'dekadal':
        dekad = xr.where(
            data.time.dt.day <= 10, 1,
            xr.where(data.time.dt.day <= 20, 2, 3)
        )
        season = (data.time.dt.month - 1) * 3 + dekad
    else:
        raise ValueError(
            "time_res must be 'monthly' or 'dekadal'"
        )
    return season.astype(np.int16).values

def _cdf_ts(
    values: np.ndarray,
    pars: Mapping[str, float],
    distribution: str
) -> np.ndarray:
    if distribution == 'gamma':
        return stats.gamma.cdf(
            values,
            pars['shape'],
            loc=pars['loc'],
            scale=pars['scale'])
    if distribution == 'peasron3':
        return stats.pearson3.cdf(
            values,
            pars['skew'],
            loc=pars['loc'],
            scale=pars['scale']
        )
    if distribution == 'llogistic':
        return stats.genlogistic.cdf(
            values,
            pars['shape'],
            loc=pars['loc'],
            scale=pars['scale']
        )
    raise ValueError(
        f'unsupported distribution: {distribution!r}'
    )

def _cdf_sp(
    values: np.ndarray,
    pars: Mapping[str, np.ndarray],
    distribution: str
) -> np.ndarray:
    if distribution == 'gamma':
        return stats.gamma.cdf(
            values,
            pars['shape'][None, :],
            loc=pars['loc'][None, :],
            scale=pars['scale'][None, :]
        )
    elif distribution == 'pearson3':
        return stats.pearson3.cdf(
            values,
            pars['skew'][None, :],
            loc=pars['loc'][None, :],
            scale=pars['scale'][None, :]
        )
    elif distribution == 'llogistic':
        return stats.genlogistic.cdf(
            values,
            pars['shape'][None, :],
            loc=pars['loc'][None, :],
            scale=pars['scale'][None, :]
        )
    else:
        raise ValueError(
            f'unsupported distribution: {distribution!r}'
        )

def _fit_distribution(
    x: np.ndarray,
    distribution: str,
    min_non_na: int
) -> dict[str, float] | None:
    x = x[np.isfinite(x)]
    if x.size < min_non_na:
        return None
    if np.unique(x).size == 1:
        runif = (
            np.random.default_rng(0)
            .uniform(0.1, 0.5, x.size)
        )
        x = x + runif
    try:
        if distribution == 'gamma':
            shape, loc, scale = stats.gamma.fit(x, floc=0)
            return {
                'shape': shape,
                'loc': loc,
                'scale': scale
            }
        if distribution == 'peasron3':
            skew, loc, scale = stats.pearson3.fit(x)
            return {
                'skew': skew,
                'loc': loc,
                'scale': scale
            }
        if distribution == 'llogistic':
            # lmomco's generalized logistic is represented by
            # SciPy's genlogistic.
            shape, loc, scale = stats.genlogistic.fit(x)
            return {
                'shape': shape,
                'loc': loc,
                'scale': scale
            }
    except (ValueError, FloatingPointError):
        return None
    raise ValueError(
        f'unsupported distribution: {distribution!r}'
    )
