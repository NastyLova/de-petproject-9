DROP TABLE IF EXISTS STV2023081257__DWH.global_metrics;
CREATE TABLE STV2023081257__DWH.global_metrics
(
    date_update timestamp NOT NULL DEFAULT now(),
    currency_from int,
    amount_total float,
    cnt_transactions int,
    avg_transactions_per_account float,
    cnt_accounts_make_transactions int
)
ORDER BY date_update
SEGMENTED BY hash(date_update) all nodes
PARTITION BY ((global_metrics.date_update)::date) 
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
