MERGE INTO STV2023081257__DWH.global_metrics AS tgt 
USING (
SELECT  cast(%s as date) date_update, 
        %s currency_from, 
        %s amount_total, 
        %s cnt_transactions, 
        %s avg_transactions_per_account, 
        %s cnt_accounts_make_transactions
) AS src 
ON (src.date_update=tgt.date_update and src.currency_from=tgt.currency_from)
WHEN MATCHED
THEN UPDATE SET amount_total=src.amount_total, 
                cnt_transactions=src.cnt_transactions, 
                avg_transactions_per_account=src.avg_transactions_per_account, 
                cnt_accounts_make_transactions=src.cnt_accounts_make_transactions 
WHEN NOT MATCHED 
THEN INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions) 
VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);