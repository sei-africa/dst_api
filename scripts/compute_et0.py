import numpy as np
import xarray as xr

def _prepare_et0_inputs(
    tmax: xr.DataArray,
    tmin: xr.DataArray,
    ra: xr.DataArray,
    precip: xr.DataArray | None = None,
) -> tuple:
    """
    Prepare ET0 inputs using Tmax as the reference spatial grid.

    The function:
    - verifies that spatial dimensions have compatible sizes;
    - assigns Tmax latitude/longitude coordinates to Tmin and Precip;
    - assigns Tmax latitude coordinates to Ra;
    - aligns inputs only over common time values;
    - preserves the final dimension order: (time, lat, lon).
    """
    required_tmax_dims = {'time', 'lat', 'lon'}

    if not required_tmax_dims.issubset(tmax.dims):
        raise ValueError(
            "tmax must contain the dimensions 'time', 'lat', and 'lon'."
        )

    if not required_tmax_dims.issubset(tmin.dims):
        raise ValueError(
            "tmin must contain the dimensions 'time', 'lat', and 'lon'."
        )

    # Check spatial array sizes before replacing coordinates.
    if tmin.sizes['lat'] != tmax.sizes['lat']:
        raise ValueError(
            'tmin and tmax have different latitude sizes: '
            f"{tmin.sizes['lat']} != {tmax.sizes['lat']}"
        )

    if tmin.sizes['lon'] != tmax.sizes['lon']:
        raise ValueError(
            'tmin and tmax have different longitude sizes: '
            f"{tmin.sizes['lon']} != {tmax.sizes['lon']}"
        )

    # Use Tmax as the reference spatial grid.
    tmin = tmin.assign_coords(
        lat=tmax['lat'],
        lon=tmax['lon'],
    )

    arrays = [tmax, tmin]

    if precip is not None:
        if not required_tmax_dims.issubset(precip.dims):
            raise ValueError(
                'precip must contain the dimensions '
                "time', 'lat', and 'lon'."
            )

        if precip.sizes['lat'] != tmax.sizes['lat']:
            raise ValueError(
                'precip and tmax have different latitude sizes: '
                f"{precip.sizes['lat']} != {tmax.sizes['lat']}"
            )

        if precip.sizes['lon'] != tmax.sizes['lon']:
            raise ValueError(
                'precip and tmax have different longitude sizes: '
                f"{precip.sizes['lon']} != {tmax.sizes['lon']}"
            )

        precip = precip.assign_coords(
            lat=tmax['lat'],
            lon=tmax['lon'],
        )

        arrays.append(precip)

    # Ra normally has dimensions (time, lat).
    if 'time' not in ra.dims or 'lat' not in ra.dims:
        raise ValueError(
            "ra must contain at least the dimensions 'time' and 'lat'."
        )

    if ra.sizes['lat'] != tmax.sizes['lat']:
        raise ValueError(
            'ra and tmax have different latitude sizes: '
            f"{ra.sizes['lat']} != {tmax.sizes['lat']}"
        )

    ra = ra.assign_coords(lat=tmax['lat'])
    arrays.append(ra)

    # Align only by common time values.
    common_time = arrays[0]['time'].values

    for array in arrays[1:]:
        common_time = np.intersect1d(
            common_time,
            array['time'].values,
            assume_unique=False
        )

    if common_time.size == 0:
        raise ValueError('The input arrays have no common time values.')

    tmax = tmax.sel(time=common_time)
    tmin = tmin.sel(time=common_time)
    ra = ra.sel(time=common_time)

    if precip is not None:
        precip = precip.sel(time=common_time)

    # Ensure consistent dimension ordering.
    tmax = tmax.transpose('time', 'lat', 'lon')
    tmin = tmin.transpose('time', 'lat', 'lon')

    if precip is not None:
        precip = precip.transpose('time', 'lat', 'lon')

    # Ra remains (time, lat). It broadcasts automatically over longitude.
    ra = ra.transpose('time', 'lat')

    if precip is None:
        return tmax, tmin, ra

    return tmax, tmin, precip, ra

def extraterrestrial_radiation(
    lat: xr.DataArray | np.ndarray,
    tstep: str = 'daily'
) -> xr.DataArray:
    """
    Calculate extraterrestrial radiation Ra.

    Parameters
    ----------
    lat
        Latitude values in degrees. When an xarray.DataArray is supplied,
        its dimension should normally be named ``lat``.
    tstep
        One of: ``daily``, ``pentadal``, ``dekadal``, or ``monthly``.

    Returns
    -------
    xr.DataArray
        Extraterrestrial radiation in MJ m-2 day-1.

        Dimensions:
        - daily:    (dayofyear, lat), with dayofyear = 1..365
        - pentadal: (dayofyear, lat), selected at days 3, 8, 13, 18, 23, 28
        - dekadal:  (dayofyear, lat), selected at days 6, 16, 26
        - monthly:  (dayofyear, lat), selected at day 16 of each month
    """
    valid_tsteps = {'daily', 'pentadal', 'dekadal', 'monthly'}
    if tstep not in valid_tsteps:
        raise ValueError(
            f'Invalid tstep={tstep!r}. '
            f'Expected one of {sorted(valid_tsteps)}.'
        )

    dates = xr.DataArray(
        np.arange(
            np.datetime64('2001-01-01'),
            np.datetime64('2002-01-01'),
            dtype='datetime64[D]',
        ),
        dims='dayofyear',
    )

    dayofyear = dates.dt.dayofyear.astype(np.float64)
    day_of_month = dates.dt.day
    lat_values = np.asarray(lat, dtype=np.float64)

    if isinstance(lat, xr.DataArray):
        lat_dim = lat.dims[0]
        lat_coord = lat
    else:
        lat_dim = 'lat'
        lat_coord = xr.DataArray(
            lat_values,
            dims=lat_dim,
            coords={lat_dim: lat_values},
        )

    phi = np.deg2rad(lat_coord)

    f_j = 2.0 * np.pi * dayofyear / 365.0
    dr = 1.0 + 0.033 * np.cos(f_j)
    delta = 0.409 * np.sin(f_j - 1.39)
    acos_argument = -np.tan(phi) * np.tan(delta)
    acos_argument = acos_argument.clip(min=-1.0, max=1.0)
    ws = np.arccos(acos_argument)
    sin_term = np.sin(phi) * np.sin(delta)
    cos_term = np.cos(phi) * np.cos(delta)

    ra = (37.58603 * dr) * (
        ws * sin_term
        + cos_term * np.sin(ws)
    )
    ra = ra.clip(min=0.0)
    ra = ra.transpose('dayofyear', lat_dim)

    if tstep == 'daily':
        selector = xr.ones_like(day_of_month, dtype=bool)
    elif tstep == 'pentadal':
        selector = day_of_month.isin([3, 8, 13, 18, 23, 28])
    elif tstep == 'dekadal':
        selector = day_of_month.isin([6, 16, 26])
    else:
        # monthly
        selector = day_of_month == 16

    ra = ra.where(selector, drop=True)
    ra = ra.assign_coords(
        dayofyear=dayofyear.where(selector, drop=True).astype(np.int16)
    )
    ra.name = 'Ra'
    ra.attrs.update(
        {
            'long_name': 'Extraterrestrial radiation',
            'units': 'MJ m-2 day-1',
            'tstep': tstep,
        }
    )
    return ra

def format_ra_to_time(
    ra: xr.DataArray,
    time: xr.DataArray,
    lat_dim: str = 'lat',
) -> xr.DataArray:
    """
    Expand a 365-day Ra array to the time dimension of a dataset.

    February 29 uses the February 28 Ra value. For leap years, dates after
    February 29 are shifted by one day when indexing the 365-day Ra array.

    Parameters
    ----------
    ra
        Radiation with dimensions ``(dayofyear, lat)`` and dayofyear 1..365.
    time
        Time coordinate from Tmax, Tmin, or another daily dataset.
    lat_dim
        Latitude dimension name.

    Returns
    -------
    xr.DataArray
        Radiation with dimensions ``(time, lat)``.
    """
    if 'dayofyear' not in ra.dims:
        raise ValueError("ra must contain a 'dayofyear' dimension.")

    if lat_dim not in ra.dims:
        raise ValueError(
            f'ra must contain the latitude dimension {lat_dim!r}.'
        )

    doy = time.dt.dayofyear
    is_leap_year = time.dt.is_leap_year
    after_february = time.dt.month > 2
    is_february_29 = (
        (time.dt.month == 2)
        & (time.dt.day == 29)
    )

    lookup_doy = xr.where(
        is_leap_year & after_february,
        doy - 1,
        doy,
    )
    lookup_doy = xr.where(
        is_february_29,
        59,
        lookup_doy,
    ).astype(np.int16)

    ra_time = ra.sel(dayofyear=lookup_doy)
    ra_time = ra_time.drop_vars('dayofyear', errors='ignore')
    ra_time = ra_time.transpose(time.dims[0], lat_dim)
    ra_time.name = 'Ra'
    ra_time.attrs.update(ra.attrs)
    return ra_time

def et0_hargreaves_modified(
    tmax: xr.DataArray,
    tmin: xr.DataArray,
    precip: xr.DataArray,
    ra: xr.DataArray,
    tstep: str = 'daily',
) -> xr.DataArray:
    """
    Calculate modified Hargreaves reference evapotranspiration.

    Parameters
    ----------
    tmax, tmin
        Maximum and minimum temperature, normally in degrees Celsius.
    precip
        Precipitation.
    ra
        Extraterrestrial radiation in MJ m-2 day-1. It may have dimensions
        (time, lat) because longitude broadcasting is automatic.
    tstep
        Time step controlling the coefficients.

    Returns
    -------
    xr.DataArray
        Reference evapotranspiration.
    """
    valid_tsteps = {'daily', 'pentadal', 'dekadal', 'monthly'}

    if tstep not in valid_tsteps:
        raise ValueError(
            f'Invalid tstep={tstep!r}. '
            f'Expected one of {sorted(valid_tsteps)}.'
        )

    tmax, tmin, precip, ra = _prepare_et0_inputs(
        tmax=tmax,
        tmin=tmin,
        precip=precip,
        ra=ra,
    )

    mean_temperature = (tmax + tmin) / 2.0
    temperature_range = (tmax - tmin).clip(min=0.0)

    if tstep == 'daily':
        coefficients = (0.0019, 21.0584, 0.0874, 0.6278)
    else:
        coefficients = (0.0013, 17.0, 0.0123, 0.76)

    c1, c2, c3, c4 = coefficients
    base = temperature_range - c3 * precip
    base = xr.where(base < 0, 0, base)
    precipitation_term = base**c4
    et0 = (
        c1
        * (0.408 * ra)
        * (mean_temperature + c2)
        * precipitation_term
    )
    et0 = et0.transpose('time', 'lat', 'lon')
    et0 = et0.rename('et0')
    et0.attrs.update(
        {
            'long_name': (
                'Reference evapotranspiration calculated using '
                'the modified Hargreaves method'
            ),
            'units': 'mm day-1' if tstep == 'daily' else 'mm',
            'method': 'Modified Hargreaves',
            'tstep': tstep,
        }
    )
    return et0

def et0_hargreaves_fao(
    tmax: xr.DataArray,
    tmin: xr.DataArray,
    ra: xr.DataArray,
) -> xr.DataArray:
    '''
    Calculate FAO Hargreaves reference evapotranspiration.
    '''
    tmax, tmin, ra = _prepare_et0_inputs(
        tmax=tmax,
        tmin=tmin,
        ra=ra,
    )
    mean_temperature = (tmax + tmin) / 2.0
    temperature_range = (tmax - tmin).clip(min=0.0)
    et0 = (
        0.0023
        * (0.408 * ra)
        * (mean_temperature + 17.8)
        * np.sqrt(temperature_range)
    )
    et0 = et0.transpose('time', 'lat', 'lon')
    et0 = et0.rename('et0')

    et0.attrs.update(
        {
            'long_name': (
                'Reference evapotranspiration calculated using '
                'the FAO Hargreaves method'
            ),
            'units': 'mm day-1',
            'method': 'FAO Hargreaves',
        }
    )
    return et0
