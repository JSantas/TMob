SELECT
        to_date(timestamp) as date,
        sum(delka) / 60 as minutes,
        sum(cena) as cena
FROM
        kafka_billings
WHERE
        to_date(timestamp) in (to_date(date_sub(timestamp, 1)), to_date(current_timestamp()))
GROUP BY date

