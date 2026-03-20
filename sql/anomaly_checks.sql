-- Negative prices.
SELECT *
FROM current_day_data
WHERE price < 0;

-- Invalid asset types.
SELECT *
FROM current_day_data
WHERE asset_type NOT IN ('Equity', 'Bond', 'ETF', 'Mutual Fund', 'Preferred Stock');

-- Bad date logic.
SELECT *
FROM current_day_data
WHERE maturity_date IS NOT NULL
  AND maturity_date <> ''
  AND maturity_date < issue_date;

-- Future issue dates.
SELECT *
FROM current_day_data
WHERE issue_date > DATE('now');
