SELECT *
FROM {{ source('raw', 'cow_swap_data') }}