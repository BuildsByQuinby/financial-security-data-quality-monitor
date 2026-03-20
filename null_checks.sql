-- Find records with missing critical fields.
SELECT *
FROM current_day_data
WHERE ticker IS NULL OR TRIM(ticker) = ''
   OR cusip IS NULL OR TRIM(cusip) = ''
   OR security_name IS NULL OR TRIM(security_name) = ''
   OR asset_type IS NULL OR TRIM(asset_type) = ''
   OR sector IS NULL OR TRIM(sector) = ''
   OR price IS NULL;
