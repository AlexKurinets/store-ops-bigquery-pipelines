{#
  The few places where BigQuery and Postgres SQL differ. Models call these macros, dbt dispatches on the target
  adapter, and the rest of the SQL is written once. Postgres is the local and CI stand-in; BigQuery is production.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {#- Layers are datasets (raw, staged, conformed, metrics), not prefixes of the target schema. -#}
  {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}


{#- Business date: local time minus the store's day-start hour, so a 01:30 sale belongs to the previous day. -#}
{% macro business_date(ts, tz, day_start_hour) %}{{ return(adapter.dispatch('business_date')(ts, tz, day_start_hour)) }}{% endmacro %}
{% macro default__business_date(ts, tz, day_start_hour) -%}
  cast((({{ ts }}) at time zone {{ tz }}) - make_interval(hours => {{ day_start_hour }}) as date)
{%- endmacro %}
{% macro bigquery__business_date(ts, tz, day_start_hour) -%}
  date(timestamp_sub({{ ts }}, interval {{ day_start_hour }} hour), {{ tz }})
{%- endmacro %}


{% macro local_date(ts, tz) %}{{ return(adapter.dispatch('local_date')(ts, tz)) }}{% endmacro %}
{% macro default__local_date(ts, tz) %}cast((({{ ts }}) at time zone {{ tz }}) as date){% endmacro %}
{% macro bigquery__local_date(ts, tz) %}date({{ ts }}, {{ tz }}){% endmacro %}


{% macro local_hour(ts, tz) %}{{ return(adapter.dispatch('local_hour')(ts, tz)) }}{% endmacro %}
{% macro default__local_hour(ts, tz) %}cast(extract(hour from (({{ ts }}) at time zone {{ tz }})) as integer){% endmacro %}
{% macro bigquery__local_hour(ts, tz) %}extract(hour from datetime({{ ts }}, {{ tz }})){% endmacro %}


{% macro hours_between(start_ts, end_ts) %}{{ return(adapter.dispatch('hours_between')(start_ts, end_ts)) }}{% endmacro %}
{% macro default__hours_between(start_ts, end_ts) %}(extract(epoch from ({{ end_ts }} - {{ start_ts }})) / 3600.0){% endmacro %}
{% macro bigquery__hours_between(start_ts, end_ts) %}(timestamp_diff({{ end_ts }}, {{ start_ts }}, second) / 3600.0){% endmacro %}


{% macro date_spine(start_date, end_date) %}{{ return(adapter.dispatch('date_spine')(start_date, end_date)) }}{% endmacro %}
{% macro default__date_spine(start_date, end_date) -%}
  select cast(d as date) as date_day
  from generate_series(cast('{{ start_date }}' as date), cast('{{ end_date }}' as date), interval '1 day') as d
{%- endmacro %}
{% macro bigquery__date_spine(start_date, end_date) -%}
  select date_day from unnest(generate_date_array('{{ start_date }}', '{{ end_date }}')) as date_day
{%- endmacro %}


{#- Finance types store names by hand; crosswalk aliases are stored in this normalized form. -#}
{% macro normalize_name(col) %}{{ return(adapter.dispatch('normalize_name')(col)) }}{% endmacro %}
{% macro default__normalize_name(col) %}trim(regexp_replace(upper({{ col }}), '[^A-Z0-9]+', ' ', 'g')){% endmacro %}
{% macro bigquery__normalize_name(col) %}trim(regexp_replace(upper({{ col }}), r'[^A-Z0-9]+', ' ')){% endmacro %}


{% macro week_start(d) %}{{ return(adapter.dispatch('week_start')(d)) }}{% endmacro %}
{% macro default__week_start(d) %}cast(date_trunc('week', {{ d }}) as date){% endmacro %}
{% macro bigquery__week_start(d) %}date_trunc({{ d }}, isoweek){% endmacro %}


{% macro month_key(d) %}{{ return(adapter.dispatch('month_key')(d)) }}{% endmacro %}
{% macro default__month_key(d) %}to_char({{ d }}, 'YYYY-MM'){% endmacro %}
{% macro bigquery__month_key(d) %}format_date('%Y-%m', {{ d }}){% endmacro %}


{% macro iso_weekday(d) %}{{ return(adapter.dispatch('iso_weekday')(d)) }}{% endmacro %}
{% macro default__iso_weekday(d) %}cast(extract(isodow from {{ d }}) as integer){% endmacro %}
{% macro bigquery__iso_weekday(d) %}(mod(extract(dayofweek from {{ d }}) + 5, 7) + 1){% endmacro %}


{#- The denominator is parenthesized: safe_ratio('a', 'b * c / d') must divide by the whole expression. -#}
{% macro safe_ratio(num, den) -%}
  case when ({{ den }}) is null or ({{ den }}) = 0 then null
       else cast({{ num }} as {{ dbt.type_numeric() }}) / ({{ den }}) end
{%- endmacro %}


{#- Postgres replaces whole business dates with delete+insert keyed on the date; BigQuery overwrites partitions. -#}
{% macro day_replace_strategy() -%}
  {{ return('insert_overwrite' if target.type == 'bigquery' else 'delete+insert') }}
{%- endmacro %}


{% macro end_of_time() %}cast('9999-12-31' as date){% endmacro %}
