#%%
# imports

import pandas as pd
import altair as alt

from datetime import date



# read in data

today = date.today()
this_month = today.strftime('%m')

previous_month_total = pd.read_csv('data/' + this_month + ' - previous month - total.csv')
previous_previous_total = pd.read_csv('data/' + this_month + ' - previous previous - total.csv')

previous_month_by_type = pd.read_csv('data/' + this_month + ' - previous month - by type.csv')
previous_previous_by_type = pd.read_csv('data/' + this_month + ' - previous previous - by type.csv')

previous_month_by_agency = pd.read_csv('data/' + this_month + ' - previous month - by agency.csv')
previous_previous_by_agency = pd.read_csv('data/' + this_month + ' - previous previous - by agency.csv')

#%%
# reformat to tidy and concat
# build plots

# totals

previous_month_total.columns = ['date checked','previous month total']
previous_previous_total.columns = ['date checked','previous previous total']

total_tidy = (
    pd.concat([
        previous_month_total.melt(id_vars='date checked'),
        previous_previous_total.melt(id_vars='date checked')
    ])
    .rename(columns={
        'variable':'data month',
        'value':'rows loaded'
    })
)

total_plot = (
    alt.Chart(total_tidy)
    .mark_point()
    .encode(
        alt.X('monthdate(date checked):O').title('date checked'),
        y='rows loaded',
        color='data month'
    )
)

total_plot.save('plots/total_plot.svg')

# by type


# by agency


previous_month_by_agency['data month'] = 'previous month'
previous_previous_by_agency['data month'] = 'previous previous'

by_agency_tidy = (
    pd.concat([
        previous_month_by_agency.melt(id_vars=['date checked','data month']),
        previous_previous_by_agency.melt(id_vars=['date checked','data month'])
    ])
    .rename(columns={
        'variable':'Agency',
        'value':'rows loaded'
    })
)

by_agency_tidy['date checked'] = pd.to_datetime(by_agency_tidy['date checked'])


ordering = (
    by_agency_tidy[
        (by_agency_tidy['data month']=='previous previous') &
        (by_agency_tidy['date checked']==by_agency_tidy['date checked'].max())
    ]
    .sort_values('rows loaded', ascending=False)
    ['Agency']
    .to_list()
)

## timeline

timeline_base = (
    alt.Chart(by_agency_tidy.dropna())
    .encode(
        alt.X('monthdate(date checked):O').title('date checked'),
        alt.Color('Agency', sort=ordering),
        y='rows loaded',
        tooltip='Agency'
    )
)

previous_month_timeline = (
    timeline_base
    .transform_filter(
        alt.FieldEqualPredicate(field='data month', equal='previous month')
    )
    .mark_line(
        point=alt.OverlayMarkDef(size=200, fill='null')
    )
)

previous_previous_timeline = (
    timeline_base
    .transform_filter(
        alt.FieldEqualPredicate(field='data month', equal='previous previous')
    )
    .mark_line(
        strokeWidth=1, 
        point=alt.OverlayMarkDef(size=200, fill='null'),
    )
)

agency_timeline = (previous_month_timeline + previous_previous_timeline).interactive()

agency_timeline.save('plots/agency_timeline.svg')

## progress bars

progress_base = (
    alt.Chart(by_agency_tidy.dropna())
    .encode(
        alt.Y('Agency', sort=ordering),
        x='rows loaded',
        tooltip='Agency'
    )
)

target_point = (
    progress_base
    .transform_filter(
        alt.FieldEqualPredicate(field='data month', equal='previous previous')
    )
    .mark_point(shape='diamond')
)

progress_tick = (
    progress_base
    .transform_filter(
        alt.FieldEqualPredicate(field='data month', equal='previous month')

    )
    .mark_tick()
    .encode(
        alt.Color('monthdate(date checked):O').title('date checked'),
    )
)

agency_progress = (target_point + progress_tick).interactive(bind_x=False)

agency_progress.save('plots/agency_progress.svg')

# save to svg

