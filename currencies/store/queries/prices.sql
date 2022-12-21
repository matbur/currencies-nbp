-- name: SavePrice :exec
INSERT INTO
    prices (date, currency, price)
VALUES
    (@date, @currency, @price)
ON CONFLICT (date, currency) DO NOTHING;

-- name: GetPrices :many
SELECT
    date,
    currency,
    price
FROM
    prices
WHERE
    currency = @currency
ORDER BY
    date;