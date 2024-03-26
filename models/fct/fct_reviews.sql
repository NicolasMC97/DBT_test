{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH src_reviews AS (
  SELECT * FROM {{ ref('stg_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null
and 1=1
{% if is_incremental() %}
  AND review_date > (select max(review_date) from {{ this }})
{% endif %}