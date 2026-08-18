-- Singular test. Warns (or fails, past the threshold below) if any rows
-- are returned.
--
-- Reconciliation check for fct_generation's share_of_mix_pct: each
-- settlement period's non-interconnector fuels should sum to roughly
-- 100% of the (interconnector-inclusive) total. See fct_generation.sql
-- for why the denominator includes interconnectors at all.
--
-- The [15, 145] band and the error_if threshold below are not
-- placeholders; they come from actually running this exact query against
-- 90 days of this project's real bronze data (2026-05-18 to
-- 2026-08-16, 4,352 settlement periods) before picking any numbers:
--   - median domestic share ~82%, 89% of periods fell in [60, 101]
--   - it legitimately exceeds 100% when GB is a net exporter (negative
--     interconnector flow shrinks the denominator), observed up to
--     ~144%
--   - it legitimately drops toward 0% when GB is a net importer, and hit
--     exactly 0% for two consecutive periods on 2026-07-07, when every
--     domestic fuel type was reported as literal zero while
--     interconnector flows were already populated (a genuine
--     late-publish gap in Elexon's feed, not a modelling bug)
--   - only 6 of 4,352 real periods (0.14%) fell outside [15, 145]
--
-- severity: warn, with a low warn_if and a much higher error_if: an
-- isolated late-publish gap like the one above should surface (warn),
-- not fail the build, since it is a known, expected characteristic of
-- this data source rather than a defect in this project's own logic. A
-- systemic problem (the SQL itself broken, or a real upstream data
-- quality regression producing double-digit violations) still fails
-- the build outright.

{{ config(severity='warn', warn_if='>0', error_if='>20') }}

with by_period as (

    select
        g.settlement_date,
        g.settlement_period,
        sum(g.share_of_mix_pct) as domestic_share_pct
    from {{ ref('fct_generation') }} g
    join {{ ref('dim_fuel_type') }} f on g.fuel_type_key = f.fuel_type_key
    where not f.is_interconnector
    group by 1, 2

)

select *
from by_period
where domestic_share_pct < 15 or domestic_share_pct > 145
