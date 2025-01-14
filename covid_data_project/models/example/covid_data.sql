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

ranked_data AS (
    SELECT
        *, -- Select all columns, including parsed fields
        ROW_NUMBER() OVER (
            PARTITION BY admin2, province_state, country_region 
            ORDER BY last_update DESC
        ) AS row_num -- Assign a rank to each row based on the latest last_update
    FROM parsed_data
)

SELECT
    * -- Select all columns
FROM ranked_data
WHERE row_num = 1 -- Filter to only include the latest row for each group
ORDER BY country_region, province_state, admin2

