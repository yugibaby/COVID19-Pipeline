WITH cleaned_data AS (
    SELECT
        * -- Select all columns from the source
    FROM "raw".covid_data -- Replace with your actual table name
    WHERE last_update IS NOT NULL -- Remove rows where last_update is NULL
      AND confirmed >= 0          -- Remove rows where confirmed is negative
      AND deaths >= 0             -- Remove rows where deaths is negative
),

parsed_data AS (
    SELECT
        *, -- Select all columns
        CAST(last_update AS DATE) AS last_update_date, -- Parse last_update to date
        CAST(last_update AS TIME) AS last_update_time  -- Parse last_update to time
    FROM cleaned_data
),

diff_calculated AS (
    SELECT
        *,
        confirmed - LAG(confirmed) OVER (
            PARTITION BY country_region, province_state, admin2 ORDER BY last_update_date
        ) AS confirmed_diff, -- Difference in confirmed cases
        deaths - LAG(deaths) OVER (
            PARTITION BY country_region, province_state, admin2 ORDER BY last_update_date
        ) AS death_diff -- Difference in deaths
    FROM parsed_data
)

SELECT
    country_region,
    province_state,
    admin2,
    last_update_date,
    confirmed,
    confirmed_diff,
    deaths,
    death_diff
FROM diff_calculated
ORDER BY country_region, province_state, admin2, last_update_date
