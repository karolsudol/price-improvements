WITH cow_data AS (
    SELECT *
    FROM {{ ref('stg_cow_swap_data') }}
),
baseline_prices AS (
    SELECT *
    FROM {{ ref('stg_baseline_prices') }}
)

SELECT
    cow_data.*,
    baseline_prices.price AS baseline_price,
    cow_data.atoms_sold * baseline_prices.price / 1e6 AS baseline_buy_value,
    cow_data.buy_value_usd - (cow_data.atoms_sold * baseline_prices.price / 1e6) AS price_improvement
FROM cow_data
JOIN baseline_prices ON cow_data.date = baseline_prices.date