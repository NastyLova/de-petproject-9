CREATE TABLE STV2023081257__STAGING.transactions
(
    operation_id uuid,
    account_number_from int,
    account_number_to int,
    currency_code int,
    country varchar(80),
    status varchar(50),
    transaction_type varchar(50),
    amount int,
    transaction_dt timestamp
)
PARTITION BY ((transaction_dt)::date) 
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);

CREATE PROJECTION STV2023081257__STAGING.transactions_proj
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
AS
 SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 FROM STV2023081257__STAGING.transactions
 ORDER BY transactions.operation_id
SEGMENTED BY hash(transactions.operation_id) ALL NODES KSAFE 1;

CREATE TABLE STV2023081257__STAGING.currencies
(
    date_update timestamp,
    currency_code int,
    currency_code_with int,
    currency_code_div float
)
PARTITION BY ((currencies.date_update)::date) 
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION STV2023081257__STAGING.currencies_proj
(
 date_update,
 currency_code,
 currency_code_with,
 currency_code_div
)
AS
 SELECT currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_code_div
 FROM STV2023081257__STAGING.currencies
 ORDER BY currencies.date_update
SEGMENTED BY hash(currencies.date_update) ALL NODES KSAFE 1;