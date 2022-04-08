SELECT
        to_date(timestamp) as date,
        sum(delka) / 60 as minutes,
        sum(cena) as cena
FROM
        kafka_billings
WHERE
        _timestamp > 1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval 1 DAYS)
GROUP BY date

/* or interval 1440 MINUTES*/