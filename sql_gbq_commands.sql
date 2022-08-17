##############################
# Instructions
##############################

These sql commands are used in google big query to create the mft_dapps_panel dataset(s)


The queries create intermediary datasets that allow to investigate the data quality and to run checks. 
Further the tables are built in a way that changes to one table does not require change all tables but only to re-run the final aggregate query. 

The tables are built by the following steps:

Table 1: mft_dapp_contract_link - is imported from G-drive and is a compound table from postgres DB storing stateofthedapp info and manually added dapps based on data from the defilama API
Table 2: mft_network_metrics_history - contains daily network metrics obtained from the etherscan API
Table 3: mft_gas_price_percentiles_daily - contains different percentiles of gas prices (can be on different aggregation levels which is indicated by the last work of the table name)
Table 4: mft_token_price_history 
Table 5: mft_token_transfers (too big to display - use in with statement) - contains all ERC20 token transfers including value in USD
Table 6: mft_dapps_txn (too big to display - use in with statement) - contains all transactions to dapps in our sample
Table 7 (Output 1): mft_dapps_panel_daily - contains daily transaction data 
Table 8 (Output 2): mft_network_metrics_and_percentiles - contains daily network level metrics and gas price percentiles


######################################
# GBQ Table 1 - mft_dapp_contract_link
######################################

/* This dataset is imported from G-Drive*/
/* It contains data scraped from state of the dapps and data from the defi lama API. 
    The defi lama data has to be manually merged since the dApp names differ in details*/

SELECT 
    *
FROM `eth-transactions.dapps_contract_link.mft_dapp_contract_link` 

###########################################
# GBQ Table 2 - mft_network_metrics_history_short
###########################################

/* create a new table that stores daily network level data*/
/* This data comes from the Etherscan pro api*/

SELECT
    n.string_field_1 as date_key,
    n.string_field_2 as blockSize_bytes,
    n.string_field_6 as gasLimit,
    n.string_field_12 as newAddressCount,
    n.string_field_13 as networkUtilization,
    n.string_field_15 as transactionCount,
    n.string_field_15 as ethValue
FROM `eth-transactions.workbench.network_metrics_history` n



##########################################
# Table 3: mft_gas_price_percentiles_daily
##########################################


/* create a table that stores the daily market gas price percentiles in GWEI -> calculated from transactions */ 


SELECT 
    DISTINCT date_key,
    TRUNC(p95_gas_price * POWER(10,-9),2) as gas_price_p95_GWEI,
    TRUNC(p75_gas_price * POWER(10,-9),2) as gas_price_p75_GWEI,
    TRUNC(p50_gas_price * POWER(10,-9),2) as gas_price_p50_GWEI,
    TRUNC(p25_gas_price * POWER(10,-9),2) as gas_price_p25_GWEI,
    TRUNC(p05_gas_price * POWER(10,-9),2) as gas_price_p05_GWEI,

    TRUNC(p95_gas_price_1559 * POWER(10,-9),2) as gas_price_1559_p95_GWEI,
    TRUNC(p75_gas_price_1559 * POWER(10,-9),2) as gas_price_1559_p75_GWEI,
    TRUNC(p50_gas_price_1559 * POWER(10,-9),2) as gas_price_1559_p50_GWEI,
    TRUNC(p25_gas_price_1559 * POWER(10,-9),2) as gas_price_1559_p25_GWEI,
    TRUNC(p05_gas_price_1559 * POWER(10,-9),2) as gas_price_1559_p05_GWEI,

    TRUNC(p95_tip * POWER(10,-9),2) as tip_p95_GWEI,
    TRUNC(p75_tip * POWER(10,-9),2) as tip_p75_GWEI,
    TRUNC(p50_tip * POWER(10,-9),2) as tip_p50_GWEI,
    TRUNC(p25_tip * POWER(10,-9),2) as tip_p25_GWEI,
    TRUNC(p05_tip * POWER(10,-9),2) as tip_p05_GWEI,

    TRUNC(p95_total_fee * POWER(10,-9),2) as total_fee_p95_GWEI,
    TRUNC(p75_total_fee * POWER(10,-9),2) as total_fee_p75_GWEI,
    TRUNC(p50_total_fee * POWER(10,-9),2) as total_fee_p50_GWEI,
    TRUNC(p25_total_fee * POWER(10,-9),2) as total_fee_p25_GWEI,
    TRUNC(p05_total_fee * POWER(10,-9),2) as total_fee_p05_GWEI,


FROM
    (SELECT 
        DATE(t.block_timestamp) as date_key,
            PERCENTILE_CONT(gas_price, 0.95) OVER (PARTITION BY DATE(t.block_timestamp)) AS p95_gas_price,
            PERCENTILE_CONT(gas_price, 0.75) OVER (PARTITION BY DATE(t.block_timestamp)) AS p75_gas_price,
            PERCENTILE_CONT(gas_price, 0.50) OVER (PARTITION BY DATE(t.block_timestamp)) AS p50_gas_price,
            PERCENTILE_CONT(gas_price, 0.25) OVER (PARTITION BY DATE(t.block_timestamp)) AS p25_gas_price,
            PERCENTILE_CONT(gas_price, 0.05) OVER (PARTITION BY DATE(t.block_timestamp)) AS p05_gas_price,

            PERCENTILE_CONT(receipt_effective_gas_price, 0.95) OVER (PARTITION BY DATE(t.block_timestamp)) AS p95_gas_price_1559,
            PERCENTILE_CONT(receipt_effective_gas_price, 0.75) OVER (PARTITION BY DATE(t.block_timestamp)) AS p75_gas_price_1559,
            PERCENTILE_CONT(receipt_effective_gas_price, 0.50) OVER (PARTITION BY DATE(t.block_timestamp)) AS p50_gas_price_1559,
            PERCENTILE_CONT(receipt_effective_gas_price, 0.25) OVER (PARTITION BY DATE(t.block_timestamp)) AS p25_gas_price_1559,
            PERCENTILE_CONT(receipt_effective_gas_price, 0.05) OVER (PARTITION BY DATE(t.block_timestamp)) AS p05_gas_price_1559,

            PERCENTILE_CONT(max_priority_fee_per_gas, 0.95) OVER (PARTITION BY DATE(t.block_timestamp)) AS p95_tip,
            PERCENTILE_CONT(max_priority_fee_per_gas, 0.75) OVER (PARTITION BY DATE(t.block_timestamp)) AS p75_tip,
            PERCENTILE_CONT(max_priority_fee_per_gas, 0.50) OVER (PARTITION BY DATE(t.block_timestamp)) AS p50_tip,
            PERCENTILE_CONT(max_priority_fee_per_gas, 0.25) OVER (PARTITION BY DATE(t.block_timestamp)) AS p25_tip,
            PERCENTILE_CONT(max_priority_fee_per_gas, 0.05) OVER (PARTITION BY DATE(t.block_timestamp)) AS p05_tip,

            PERCENTILE_CONT(max_fee_per_gas, 0.95) OVER (PARTITION BY DATE(t.block_timestamp)) AS p95_total_fee,
            PERCENTILE_CONT(max_fee_per_gas, 0.75) OVER (PARTITION BY DATE(t.block_timestamp)) AS p75_total_fee,
            PERCENTILE_CONT(max_fee_per_gas, 0.50) OVER (PARTITION BY DATE(t.block_timestamp)) AS p50_total_fee,
            PERCENTILE_CONT(max_fee_per_gas, 0.25) OVER (PARTITION BY DATE(t.block_timestamp)) AS p25_total_fee,
            PERCENTILE_CONT(max_fee_per_gas, 0.05) OVER (PARTITION BY DATE(t.block_timestamp)) AS p05_total_fee,
    FROM `bigquery-public-data.crypto_ethereum.transactions` t)


##########################################
# GBQ Table 4: - mft_token_price_history
##########################################

/* This dataset comes from the coin gecko api and contains daily prices (in USD) of ~4800 tokens on Ethereum*/
/* The full dataset is too big for one upload and this is fist split into 15 data sets which are unioned after the upload*/


SELECT * FROM `eth-transactions.mft_dapp_perspective.mft_token_price_history`



##########################################
# GBQ Table 5 - mft_token_transfers
##########################################

/* Token transfers in total USD per Hash*/
/* The output of this query for all years is too big to be displayed in GBQ therefore limit with WHERE*/
/* Use it in a WITH statement - join with transaction hash on transaction*/

SELECT 
    DISTINCT tt.transaction_hash as transaction_hash,
    tt.token_address as token_address,
    tt.block_number as block_number,
    tt.value as token_amount,
    t.decimals as token_decimals,
    token_prices.token_priceUSD as token_priceUSD,
    CAST(tt.value as float64)*POWER(10,- CAST(t.decimals as float64))*CAST(token_prices.token_priceUSD as float64) as total_token_value_usd
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` tt
INNER JOIN `eth-transactions.mft_dapp_perspective.mft_token_price_history` token_prices
ON tt.token_address = token_prices.token_address AND DATE(tt.block_timestamp) = token_prices.date_key
INNER JOIN `bigquery-public-data.crypto_ethereum.tokens` t
ON t.address = tt.token_address
WHERE DATE(tt.block_timestamp) > "2021-07-31" AND t.decimals IS NOT Null


##########################################
# GBQ Table 6 - mft_dapps_txn
##########################################

/* This query returns all transactions sent to dapps in our dataset (sotd, defilama, and gbq) */
/* For the full dataset this query is too big -> use it in WITH statement*/
SELECT
    t.hash as txn_hash,
    t.block_number as block_number,
    dapp_link.d_name as d_name,
    dapp_link.d_status as d_status,
    dapp_link.d_cat as d_cat,
    t.to_address as to_address,
    CAST(t.value as float64)*POWER(10,-9) as txn_value_GWEI,
    t.gas as txn_gas,
    t.gas_price*POWER(10,-9) as txn_gas_price_GWEI,
    t.receipt_effective_gas_price*POWER(10,-9) as txn_gas_price1559_GWEI,
    t.max_priority_fee_per_gas*POWER(10,-9) as txn_tip_per_gas_GWEI,
    t.max_fee_per_gas*POWER(10,-9) as txn_max_willingnes_per_gas_GWEI,
    CAST(t.gas as float64)*(CAST(t.max_fee_per_gas as float64)*POWER(10,-9)) as txn_willingness_to_pay_GWEI,
    CAST(t.receipt_gas_used as float64)*(CAST(t.receipt_effective_gas_price as float64)*POWER(10,-9)) as txn_fees_paid_GWEI,
    t.block_timestamp as block_timestamp

FROM `bigquery-public-data.crypto_ethereum.transactions` t 
INNER JOIN `eth-transactions.mft_dapp_perspective.dapp_contract_link_sotd_defi_gbq` dapp_link
ON LOWER(t.to_address) = LOWER(dapp_link.c_address)
WHERE DATE(t.block_timestamp) > "2021-07-31" 




##########################################
# GBQ Table 7 - mft_dapps_panel_daily
##########################################

/* Final query that joins and all prior tables and aggregated to the right level*/
/* 1. Step: Join transactions and transfers and aggregate them on a daily level*/
WITH 
     dapp_txn as (
         SELECT
                t.hash as txn_hash,
                t.block_number as block_number,
                dapp_link.d_name as d_name,
                dapp_link.d_status as d_status,
                dapp_link.d_cat as d_cat,
                t.to_address as to_address,
                t.from_address as from_address,
                CAST(t.value as float64)*POWER(10,-9) as txn_value_GWEI,
                t.gas as txn_gas_bysender,
                t.receipt_gas_used as txn_gas_used,
                t.gas_price*POWER(10,-9) as txn_gas_price_GWEI,
                t.receipt_effective_gas_price*POWER(10,-9) as txn_gas_price1559_GWEI,
                t.max_priority_fee_per_gas*POWER(10,-9) as txn_tip_per_gas_GWEI,
                t.max_fee_per_gas*POWER(10,-9) as txn_max_willingnes_per_gas_GWEI,
                CAST(t.gas as float64)*(CAST(t.max_fee_per_gas as float64)*POWER(10,-9)) as txn_willingness_to_pay_GWEI,
                CAST(t.receipt_gas_used as float64)*(CAST(t.receipt_effective_gas_price as float64)*POWER(10,-9)) as txn_fees_paid_GWEI,
                t.block_timestamp as block_timestamp

            FROM `bigquery-public-data.crypto_ethereum.transactions` t 
            INNER JOIN `eth-transactions.mft_dapp_perspective.dapp_contract_link_sotd_defi_gbq` dapp_link
            ON t.to_address = dapp_link.c_address
     ),

     token_transfers as (
            SELECT 
                DISTINCT tt.transaction_hash as txn_hash,
                tt.token_address as token_address,
                tt.block_number as block_number,
                tt.value as token_amount,
                t.decimals as token_decimals,
                token_prices.token_priceUSD as token_priceUSD,
                CAST(tt.value as float64)*POWER(10,- CAST(t.decimals as float64))*CAST(token_prices.token_priceUSD as float64) as total_token_value_usd
                FROM `bigquery-public-data.crypto_ethereum.token_transfers` tt
            INNER JOIN `eth-transactions.mft_dapp_perspective.mft_token_price_history` token_prices
            ON LOWER(tt.token_address) = LOWER(token_prices.token_address) AND DATE(tt.block_timestamp) = token_prices.date_key
            INNER JOIN `bigquery-public-data.crypto_ethereum.tokens` t
            ON LOWER(t.address) = LOWER(tt.token_address)
            WHERE t.decimals IS NOT Null AND tt.from_address != "0x0000000000000000000000000000000000000000" AND tt.from_address != tt.to_address
            
     )

     
SELECT 
    DATE(dt.block_timestamp) as date_key,
    dt.d_name as d_name,
    dt.d_status as d_status,
    dt.d_cat as d_cat,
    COUNT(dt.from_address) as transaction_activity,
    COUNT(DISTINCT dt.from_address) as unique_eoa,
    SUM(dt.txn_value_GWEI) as SUM_txn_value_GWEI,
    SUM(tt.total_token_value_usd) as SUM_token_value_USD,
    AVG(dt.txn_value_GWEI) as AVG_txn_value_GWEI,
    AVG(tt.total_token_value_usd) as AVG_token_value_USD,
    AVG(dt.txn_gas_bysender) as AVG_txn_gas_bysender,
    AVG(dt.txn_gas_used) as AVG_txn_gas_used,
    AVG(dt.txn_gas_price_GWEI) as AVG_txn_gas_price_GWEI,
    AVG(dt.txn_gas_price1559_GWEI) as AVG_txn_gas_price1559_GWEI,
    AVG(dt.txn_tip_per_gas_GWEI) as AVG_txn_tip_per_gas_GWEI,
    AVG(dt.txn_max_willingnes_per_gas_GWEI) as AVG_txn_max_willingnes_per_gas_GWEI,
    AVG(dt.txn_willingness_to_pay_GWEI) as AVG_txn_willingness_to_pay_GWEI,
    AVG(dt.txn_fees_paid_GWEI) as AVG_txn_fees_paid_GWEI
FROM dapp_txn dt
LEFT JOIN token_transfers tt
ON  dt.txn_hash = tt.txn_hash
GROUP BY 1,2,3,4




##########################################
# GBQ Table 8 - mft_network_metrics_and_percentiles
##########################################
/* these two tables are kept seperately since the transaction data does not have the full date range for all dapps*/


SELECT 
    DATE(gpp.date_key) as date_key,
    gpp.gas_price_p95_GWEI as gas_price_p95_GWEI,
    gpp.gas_price_p75_GWEI as gas_price_p75_GWEI,
    gpp.gas_price_p50_GWEI as gas_price_p50_GWEI,
    gpp.gas_price_p25_GWEI as gas_price_p25_GWEI,
    gpp.gas_price_p05_GWEI as gas_price_p05_GWEI,
    gpp.gas_price_1559_p95_GWEI as gas_price_1559_p95_GWEI,
    gpp.gas_price_1559_p75_GWEI as gas_price_1559_p75_GWEI,
    gpp.gas_price_1559_p50_GWEI as gas_price_1559_p50_GWEI,
    gpp.gas_price_1559_p25_GWEI as gas_price_1559_p25_GWEI,
    gpp.gas_price_1559_p05_GWEI as gas_price_1559_p05_GWEI,
    gpp.tip_p95_GWEI as tip_p95_GWEI,
    gpp.tip_p75_GWEI as tip_p75_GWEI,
    gpp.tip_p50_GWEI as tip_p50_GWEI,
    gpp.tip_p25_GWEI as tip_p25_GWEI,
    gpp.tip_p05_GWEI as tip_p05_GWEI,
    gpp.total_fee_p95_GWEI as total_fee_p95_GWEI,
    gpp.total_fee_p75_GWEI as total_fee_p75_GWEI,
    gpp.total_fee_p50_GWEI as total_fee_p50_GWEI,
    gpp.total_fee_p25_GWEI as total_fee_p25_GWEI,
    gpp.total_fee_p05_GWEI as total_fee_p05_GWEI,
    nh.blockSize_bytes as blockSize_bytes,
    nh.gasLimit as gasLimit,
    nh.newAddressCount as newAddressCount,
    nh.networkUtilization as networkUtilization,
    nh.transactionCount as transactionCount,
    nh.ethValue as ethValue

FROM  `eth-transactions.mft_dapp_perspective.mft_network_metrics_history` nh
JOIN `eth-transactions.mft_dapp_perspective.mft_gas_price_percentiles_daily` gpp
ON DATE(nh.date_key) = DATE(gpp.date_key)


















##########################
# Workbench
#########################


/* Union 15 parts of token price history */

SELECT * FROM `eth-transactions.workbench.test`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test2`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test3`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test4`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test5`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test6`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test7`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test8`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test9`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test10`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test11`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test12`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test13`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test14`
  UNION ALL
SELECT *
  FROM `eth-transactions.workbench.test15`







/*dApp Daily Transaction Panel Query*/

SELECT 
    c.string_field_0 dapp, 
    DATE(t.block_timestamp) block_timestamp,
    count(distinct t.from_address) unique_users, 
    count(t.hash) txn_number, 
    avg(t.value) avg_wei,  
    sum(t.value) sum_wei, 
    min(t.value) min_wei, 
    max(t.value) max_wei, 
    min(t.gas_price) min_gas_price, 
    avg(t.gas) avg_gas, 
    min(t.gas) min_gas, 
    max(t.gas) max_gas, 
    avg(t.max_priority_fee_per_gas) avg_tip,
    avg(t.receipt_effective_gas_price) avg_actual_gas
FROM `bigquery-public-data.crypto_ethereum.transactions` t 
JOIN `eth-transactions.dapps_contract_link.dapps_contract_link` c
ON LOWER(t.to_address) = LOWER(c.string_field_6)

GROUP BY 1, 2
ORDER BY 1, 2




/* Create Daily Transaction Panel including Token Value and Daily Gas Market Price */
/* Daily Gas Market Price either as Median or as second lowest price */

/* this query creates multiple tables along the way */
/* it also requires to create a historic token price table in google big query - 
        the data come from the Coin Gecko API that is queried with all (token) addresses in the stateofthedapp dapps table */



with dapp_txn as 
    (SELECT
        c.string_field_0 dapp, 
        c.string_field_2 d_category,
        c.string_field_3 d_status,
        DATE(t.block_timestamp) block_timestamp,
        count(distinct t.from_address) unique_users, 
        count(t.hash) txn_number, 
        avg(t.value) avg_wei,  
        sum(t.value) sum_wei, 
        min(t.value) min_wei, 
        max(t.value) max_wei, 
        min(t.gas_price) min_gas_price, 
        avg(t.gas) avg_gas, 
        min(t.gas) min_gas, 
        max(t.gas) max_gas, 
        avg(t.max_priority_fee_per_gas) avg_tip,
        avg(t.receipt_effective_gas_price) avg_actual_gas
    FROM `bigquery-public-data.crypto_ethereum.transactions` t 
    JOIN `eth-transactions.dapps_contract_link.dapps_contract_link` c
ON LOWER(t.to_address) = LOWER(c.string_field_6)
    ),
/* create a table that stores only transfers of tokens of dapps - select based on token address*/
    dapp_token_transfers as
    (

    ),

/* add token price info to each token transfer*/
    dapp_token_transfers as
    (

    )

/* create final table*/
SELECT 
    c.string_field_0 dapp, 
    DATE(t.block_timestamp) block_timestamp,
    count(distinct t.from_address) unique_users, 
    count(t.hash) txn_number, 
    avg(t.value) avg_wei,  
    sum(t.value) sum_wei, 
    min(t.value) min_wei, 
    max(t.value) max_wei, 
    min(t.gas_price) min_gas_price, 
    avg(t.gas) avg_gas, 
    min(t.gas) min_gas, 
    max(t.gas) max_gas, 
    avg(t.max_priority_fee_per_gas) avg_tip,
    avg(t.receipt_effective_gas_price) avg_actual_gas
FROM `bigquery-public-data.crypto_ethereum.transactions` t 
JOIN `eth-transactions.dapps_contract_link.dapps_contract_link` c
ON LOWER(t.to_address) = LOWER(c.string_field_6)

GROUP BY 1, 2
ORDER BY 1, 2






/* gas price checks*/
  /* filter transaction by weird days and safe table at workbench for tests*/

SELECT 
    DATE(t.block_timestamp) as date_key,
    t.gas_price,
    t.block_number,
    t.hash
FROM    `bigquery-public-data.crypto_ethereum.transactions` t
WHERE DATE(block_timestamp) = "2021-08-14" OR 
      DATE(block_timestamp) = "2020-12-23" OR 
      DATE(block_timestamp) = "2020-09-18" OR 
      DATE(block_timestamp) = "2020-07-04" OR 
      DATE(block_timestamp) = "2018-01-18" OR 
      DATE(block_timestamp) = "2018-08-14" OR 
      DATE(block_timestamp) = "2019-09-12" OR 
      DATE(block_timestamp) = "2019-11-22"  


  /* test query to compute percentiles*/

SELECT 
    DISTINCT date_key,
    TRUNC(p95 * POWER(10,-9),2) as gas_price_p95_GWEI,
    TRUNC(p75 * POWER(10,-9),2) as gas_price_p75_GWEI,
    TRUNC(p50 * POWER(10,-9),2) as gas_price_p50_GWEI,
    TRUNC(p25 * POWER(10,-9),2) as gas_price_p25_GWEI,
    TRUNC(p05 * POWER(10,-9),2) as gas_price_p05_GWEI
FROM
    (SELECT 
        date_key,
            PERCENTILE_CONT(gas_price, 0.95) OVER (PARTITION BY date_key) AS p95,
            PERCENTILE_CONT(gas_price, 0.75) OVER (PARTITION BY date_key) AS p75,
            PERCENTILE_CONT(gas_price, 0.50) OVER (PARTITION BY date_key) AS p50,
            PERCENTILE_CONT(gas_price, 0.25) OVER (PARTITION BY date_key) AS p25,
            PERCENTILE_CONT(gas_price, 0.05) OVER (PARTITION BY date_key) AS p05
    FROM `eth-transactions.workbench.test_gas_price_percentiles` )










/* find oddly high transaction values for tokens*/

  /* check decimals of tokens with high token transfer values*/
SELECT 
    address,
    symbol,
    name,
    decimals,
    DATE(block_timestamp)
FROM `bigquery-public-data.crypto_ethereum.tokens` 
WHERE symbol = "MANA" OR symbol = "USDT" OR symbol = "WBTC" OR symbol = "SNX" OR symbol = "LINK"



/*find mana token */

SELECT 
    address,
    symbol,
    name,
    decimals,
    DATE(block_timestamp)
FROM `bigquery-public-data.crypto_ethereum.tokens` 
WHERE address = lower("0x0F5D2fB29fb7d3CFeE444a200298f468908cC942")


0x0f5d2fb29fb7d3cfee444a200298f468908cc942
MANA
Decentraland MANA
18
2017-08-15





/* find all token transfers for dapps that have suspiciously high transfer values*/


SELECT 
                DISTINCT tt.transaction_hash as txn_hash,
                tt.token_address as token_address,
                tt.block_number as block_number,
                DATE(tt.block_timestamp) as date_key,
                tt.from_address as from_address,
                tt.to_address as to_address,
                tt.value as token_amount,
                t.decimals as token_decimals,
                token_prices.token_priceUSD as token_priceUSD,
                CAST(tt.value as float64)*POWER(10,- CAST(t.decimals as float64))*CAST(token_prices.token_priceUSD as float64) as total_token_value_usd
                FROM `bigquery-public-data.crypto_ethereum.token_transfers` tt
            INNER JOIN `eth-transactions.mft_dapp_perspective.mft_token_price_history` token_prices
            ON LOWER(tt.token_address) = LOWER(token_prices.token_address) AND DATE(tt.block_timestamp) = token_prices.date_key
            INNER JOIN `bigquery-public-data.crypto_ethereum.tokens` t
            ON LOWER(t.address) = LOWER(tt.token_address)
            WHERE t.decimals IS NOT Null 
            AND tt.from_address != "0x0000000000000000000000000000000000000000" 
            AND tt.from_address != tt.to_address
            AND tt.token_address IN (lower("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
                                          lower("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"),
                                          lower("0xE0aD1806Fd3E7edF6FF52Fdb822432e847411033"),
                                          lower("0x8d12A197cB00D4747a1fe03395095ce2A5CC6819"),
                                          lower("0x1E1EEd62F8D82ecFd8230B8d283D5b5c1bA81B55"),
                                          lower("0x8f3470A7388c05eE4e7AF3d01D8C722b0FF52374"),
                                          lower("0x514910771AF9Ca656af840dff83E8264EcF986CA"),
                                          lower("0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F")) 
            AND DATE(tt.block_timestamp) IN ("2021-09-07","2021-09-16","2017-07-23","2018-01-16","2018-01-10")


token addresses and days of tokens with high transfer values 

Tether, any ,  lower("0xdAC17F958D2ee523a2206206994597C13D831ec7")
WBTC, any , lower("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599")
OnX Finance, any , lower("0xE0aD1806Fd3E7edF6FF52Fdb822432e847411033")
Fork Delta, 2018-01-10, lower("0x8d12A197cB00D4747a1fe03395095ce2A5CC6819")
Gamma DEX, 2018-01-16, lower("0x1E1EEd62F8D82ecFd8230B8d283D5b5c1bA81B55")
VeADIR, 2017-07-23, lower("0x8f3470A7388c05eE4e7AF3d01D8C722b0FF52374")
Chainlink, 2021-09-07, lower("0x514910771AF9Ca656af840dff83E8264EcF986CA")
Synthetix, 2021-09-16, lower("0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F")










/* get all ETH transfer transactions grouped by day*/
WITH 
     selected_txn as (
         SELECT
                t.hash as txn_hash,
                t.block_number as block_number,
                t.to_address as to_address,
                t.from_address as from_address,
                CAST(t.value as float64)*POWER(10,-9) as txn_value_GWEI,
                t.gas as txn_gas,
                t.gas_price*POWER(10,-9) as txn_gas_price_GWEI,
                t.receipt_effective_gas_price*POWER(10,-9) as txn_gas_price1559_GWEI,
                t.max_priority_fee_per_gas*POWER(10,-9) as txn_tip_per_gas_GWEI,
                t.max_fee_per_gas*POWER(10,-9) as txn_max_willingnes_per_gas_GWEI,
                CAST(t.gas as float64)*(CAST(t.max_fee_per_gas as float64)*POWER(10,-9)) as txn_willingness_to_pay_GWEI,
                CAST(t.receipt_gas_used as float64)*(CAST(t.receipt_effective_gas_price as float64)*POWER(10,-9)) as txn_fees_paid_GWEI,
                t.block_timestamp as block_timestamp

            FROM `bigquery-public-data.crypto_ethereum.transactions` t 
            LEFT JOIN `bigquery-public-data.crypto_ethereum.contracts` c 
            ON t.to_address = c.address
            WHERE c.address IS NULL)


SELECT 
    DATE(st.block_timestamp) as date_key,
    COUNT(st.from_address) as transaction_activity,
    COUNT(DISTINCT st.from_address) as unique_eoa,
    SUM(st.txn_value_GWEI) as SUM_txn_value_GWEI,
    AVG(st.txn_value_GWEI) as AVG_txn_value_GWEI,
    AVG(st.txn_gas) as AVG_txn_gas,
    AVG(st.txn_gas_price_GWEI) as AVG_txn_gas_price_GWEI,
    AVG(st.txn_gas_price1559_GWEI) as AVG_txn_gas_price1559_GWEI,
    AVG(st.txn_tip_per_gas_GWEI) as AVG_txn_tip_per_gas_GWEI,
    AVG(st.txn_max_willingnes_per_gas_GWEI) as AVG_txn_max_willingnes_per_gas_GWEI,
    AVG(st.txn_willingness_to_pay_GWEI) as AVG_txn_willingness_to_pay_GWEI,
    AVG(st.txn_fees_paid_GWEI) as AVG_txn_fees_paid_GWEI
FROM selected_txn st
GROUP BY 1


/* get all contract creation transactions*/
WITH 
     selected_txn as (
         SELECT
                t.hash as txn_hash,
                t.block_number as block_number,
                t.to_address as to_address,
                t.from_address as from_address,
                CAST(t.value as float64)*POWER(10,-9) as txn_value_GWEI,
                t.gas as txn_gas,
                t.gas_price*POWER(10,-9) as txn_gas_price_GWEI,
                t.receipt_effective_gas_price*POWER(10,-9) as txn_gas_price1559_GWEI,
                t.max_priority_fee_per_gas*POWER(10,-9) as txn_tip_per_gas_GWEI,
                t.max_fee_per_gas*POWER(10,-9) as txn_max_willingnes_per_gas_GWEI,
                CAST(t.gas as float64)*(CAST(t.max_fee_per_gas as float64)*POWER(10,-9)) as txn_willingness_to_pay_GWEI,
                CAST(t.receipt_gas_used as float64)*(CAST(t.receipt_effective_gas_price as float64)*POWER(10,-9)) as txn_fees_paid_GWEI,
                t.block_timestamp as block_timestamp

            FROM `bigquery-public-data.crypto_ethereum.transactions` t 
            WHERE to_address = Null


SELECT 
    DATE(st.block_timestamp) as date_key,
    COUNT(st.from_address) as transaction_activity,
    COUNT(DISTINCT st.from_address) as unique_eoa,
    SUM(st.txn_value_GWEI) as SUM_txn_value_GWEI,
    AVG(st.txn_value_GWEI) as AVG_txn_value_GWEI,
    AVG(st.txn_gas) as AVG_txn_gas,
    AVG(st.txn_gas_price_GWEI) as AVG_txn_gas_price_GWEI,
    AVG(st.txn_gas_price1559_GWEI) as AVG_txn_gas_price1559_GWEI,
    AVG(st.txn_tip_per_gas_GWEI) as AVG_txn_tip_per_gas_GWEI,
    AVG(st.txn_max_willingnes_per_gas_GWEI) as AVG_txn_max_willingnes_per_gas_GWEI,
    AVG(st.txn_willingness_to_pay_GWEI) as AVG_txn_willingness_to_pay_GWEI,
    AVG(st.txn_fees_paid_GWEI) as AVG_txn_fees_paid_GWEI
FROM selected_txn st
GROUP BY 1



/* get all transactions that were sent to smart contracts not in our sample*/
WITH 
     selected_txn as (
         SELECT
                t.hash as txn_hash,
                t.block_number as block_number,
                t.to_address as to_address,
                t.from_address as from_address,
                CAST(t.value as float64)*POWER(10,-9) as txn_value_GWEI,
                t.gas as txn_gas,
                t.gas_price*POWER(10,-9) as txn_gas_price_GWEI,
                t.receipt_effective_gas_price*POWER(10,-9) as txn_gas_price1559_GWEI,
                t.max_priority_fee_per_gas*POWER(10,-9) as txn_tip_per_gas_GWEI,
                t.max_fee_per_gas*POWER(10,-9) as txn_max_willingnes_per_gas_GWEI,
                CAST(t.gas as float64)*(CAST(t.max_fee_per_gas as float64)*POWER(10,-9)) as txn_willingness_to_pay_GWEI,
                CAST(t.receipt_gas_used as float64)*(CAST(t.receipt_effective_gas_price as float64)*POWER(10,-9)) as txn_fees_paid_GWEI,
                t.block_timestamp as block_timestamp

            FROM `bigquery-public-data.crypto_ethereum.transactions` t 
            JOIN `bigquery-public-data.crypto_ethereum.contracts` c 
            ON t.to_address = c.address
            LEFT JOIN `eth-transactions.mft_dapp_perspective.mft_dapp_contract_link` dapp_link
            ON LOWER(t.to_address) = LOWER(dapp_link.c_address)
            WHERE dapp_link.c_address IS NULL)


SELECT 
    DATE(st.block_timestamp) as date_key,
    COUNT(st.from_address) as transaction_activity,
    COUNT(DISTINCT st.from_address) as unique_eoa,
    SUM(st.txn_value_GWEI) as SUM_txn_value_GWEI,
    AVG(st.txn_value_GWEI) as AVG_txn_value_GWEI,
    AVG(st.txn_gas) as AVG_txn_gas,
    AVG(st.txn_gas_price_GWEI) as AVG_txn_gas_price_GWEI,
    AVG(st.txn_gas_price1559_GWEI) as AVG_txn_gas_price1559_GWEI,
    AVG(st.txn_tip_per_gas_GWEI) as AVG_txn_tip_per_gas_GWEI,
    AVG(st.txn_max_willingnes_per_gas_GWEI) as AVG_txn_max_willingnes_per_gas_GWEI,
    AVG(st.txn_willingness_to_pay_GWEI) as AVG_txn_willingness_to_pay_GWEI,
    AVG(st.txn_fees_paid_GWEI) as AVG_txn_fees_paid_GWEI
FROM selected_txn st
GROUP BY 1





/* find contracts that are not in the sample but received the most transactions*/
WITH 
     selected_txn as (
         SELECT
                t.hash as txn_hash,
                t.block_number as block_number,
                t.to_address as to_address,
                t.from_address as from_address,
                CAST(t.value as float64)*POWER(10,-9) as txn_value_GWEI,
                t.gas as txn_gas,
                t.gas_price*POWER(10,-9) as txn_gas_price_GWEI,
                t.receipt_effective_gas_price*POWER(10,-9) as txn_gas_price1559_GWEI,
                t.max_priority_fee_per_gas*POWER(10,-9) as txn_tip_per_gas_GWEI,
                t.max_fee_per_gas*POWER(10,-9) as txn_max_willingnes_per_gas_GWEI,
                CAST(t.gas as float64)*(CAST(t.max_fee_per_gas as float64)*POWER(10,-9)) as txn_willingness_to_pay_GWEI,
                CAST(t.receipt_gas_used as float64)*(CAST(t.receipt_effective_gas_price as float64)*POWER(10,-9)) as txn_fees_paid_GWEI,
                t.block_timestamp as block_timestamp

            FROM `bigquery-public-data.crypto_ethereum.transactions` t 
            JOIN `bigquery-public-data.crypto_ethereum.contracts` c 
            ON t.to_address = c.address
            LEFT JOIN `eth-transactions.mft_dapp_perspective.mft_dapp_contract_link` dapp_link
            ON LOWER(t.to_address) = LOWER(dapp_link.c_address)
            WHERE dapp_link.c_address IS NULL)

SELECT
  to_address, 
  count(from_address) from_count,
  count(DISTINCT from_address) from_count_distinct
FROM selected_txn st
GROUP BY 1
ORDER BY 2 DESC 





/* rename and save dapp contract link after import*/
SELECT 
    string_field_0 as d_name,
    string_field_1 as d_cat,
    string_field_2 as d_status,
    string_field_3 as d_tag_list,
    string_field_4 as c_address,
    string_field_5 as source
FROM `eth-transactions.workbench.dapp_contract_link_sotd_defi_gbq` 
WHERE string_field_0 != "d_name"






#########################################
# useful functions 
#########################################
/* https://stackoverflow.com/questions/30684920/how-can-i-extract-date-from-epoch-time-in-bigquery-sql*/
  /*extract date from Milliseconds timestamp*/
SELECT
    DATE(TIMESTAMP_MILLIS(CAST(token_date AS int64)))
FROM `eth-transactions.mft_dapp_perspective.token_price_history_labeled` token_prices
WHERE token_date != "token_date"