import numpy as np
import pandas as pd

def get_daily_index_season(times, start_month, start_day,
                           end_month, end_day):
    d = times.astype('datetime64[D]')
    years = d.astype('datetime64[Y]').astype(int) + 1970
    months = d.astype('datetime64[M]').astype(int) % 12 + 1
    days = (d - d.astype('datetime64[M]')).astype(int) + 1
    same_year_season = (start_month, start_day) <= (end_month, end_day)

    if same_year_season:
        in_season = (
            ((months > start_month) | ((months == start_month) & (days >= start_day))) &
            ((months < end_month) | ((months == end_month) & (days <= end_day)))
        )
        season_year = years
    else:
        in_start_year = (
            (months > start_month) |
            ((months == start_month) & (days >= start_day))
        )

        in_end_year = (
            (months < end_month) |
            ((months == end_month) & (days <= end_day))
        )

        in_season = in_start_year | in_end_year
        season_year = np.where(in_start_year, years, years - 1)

    season_indices = {
        y: np.where(in_season & (season_year == y))[0]
        for y in np.unique(season_year[in_season])
    }

    season_days = {}
    for y in season_indices:
        start = pd.Timestamp(year=y, month=start_month, day=start_day)

        if same_year_season:
            end = pd.Timestamp(year=y, month=end_month, day=end_day)
        else:
            end = pd.Timestamp(year=y + 1, month=end_month, day=end_day)

        nb_seas = (end - start).days + 1
        nb_days = len(season_indices[y])
        season_days[y] = {
            'nb_seas': nb_seas,
            'nb_days': nb_days,
            'frac': nb_days/nb_seas
        }

    return {
        'index': season_indices,
        'length': season_days
    }

def year_daily_index_season(tindex, year):
    years = np.array([
        k for k in tindex['index'].keys()
    ])
    if not year in years:
        msg = f'No data for year: {year}'
        return {'status': -1, 'message': msg}

    return None

