with tr_acc as (select  account_number_from, 
                        cast(transaction_dt as date) transaction_dt, 
                        currency_code, 
                        count(operation_id) transactions_per_account
                from STV2023081257__STAGING.transactions
                group by account_number_from, cast(transaction_dt as date), currency_code)
                
select  cast(c.date_update as date) date_update,
        t.currency_code currency_from,
        sum(t.amount) amount_total,
        count(distinct t.operation_id) cnt_transactions,
        avg(tr_acc.transactions_per_account) avg_transactions_per_account , 
        count(distinct t.account_number_from) cnt_accounts_make_transactions
from STV2023081257__STAGING.transactions as t 
inner join STV2023081257__STAGING.currencies as c on c.currency_code = t.currency_code and cast(t.transaction_dt as date) = cast(c.date_update as date)
inner join tr_acc on tr_acc.currency_code = t.currency_code and tr_acc.transaction_dt = cast(c.date_update as date)
where cast(t.transaction_dt as date) = cast(%s as date)
group by c.date_update,
        t.currency_code;