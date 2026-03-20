-- Find duplicate security records in the current day feed.
SELECT
    ticker,
    cusip,
    COUNT(*) AS duplicate_count
FROM current_day_data
GROUP BY ticker, cusip
HAVING COUNT(*) > 1;
