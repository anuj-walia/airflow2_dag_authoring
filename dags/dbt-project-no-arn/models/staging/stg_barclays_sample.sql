{{ config(materialized='table') }}

-- Ultra-minimal model: static rows, no sources, no vars, no incremental logic.
select 1 as id, 'alpha' as label
union all
select 2 as id, 'beta' as label