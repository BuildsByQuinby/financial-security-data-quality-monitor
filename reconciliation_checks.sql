-- Find securities that existed in the previous day feed but are missing today.
SELECT p.*
FROM previous_day_data p
LEFT JOIN current_day_data c
    ON p.ticker = c.ticker
   AND p.cusip = c.cusip
WHERE c.cusip IS NULL;
