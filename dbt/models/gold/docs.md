{% docs settlement_period %}
A 30-minute block used for trading and settling GB electricity, numbered
from 1 in LOCAL time. Most days have 48. The short clock-change day in
March (clocks forward) has 46; the long day in October (clocks back) has
50 -- because settlement periods follow the local clock, not UTC. See
`dim_date.settlement_periods_in_day` and `dim_settlement_period`.
{% enddocs %}

{% docs settlement_period_start_utc %}
The real UTC instant this settlement period starts. This is Elexon's own
`startTime` field, carried through unchanged from bronze to silver to
here -- it is never derived from the period number and a clock-change
formula. `dim_settlement_period.nominal_start_time_local` is a
convenience for display; this column is the one to trust for anything
that needs an actual timestamp.
{% enddocs %}

{% docs share_of_mix_pct %}
This fuel's share of total half-hourly generation, as a percentage.
Interconnectors are included in the denominator (net-mix semantics), so
this can read below 0% or above 100%: GB net-importing dilutes every
domestic fuel's share, and GB net-exporting inflates it, because the
denominator itself is smaller than domestic generation alone. See
`fct_generation`'s model comment for the full reasoning.
{% enddocs %}
