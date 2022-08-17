# This file specifies all sql commands necessary to create tables, insert in tables, and to drop tables
    # if you need to make changes to this file, proceed in the following steps
    # 1. Add drop table command for new table
    # 2. Add create table command for new table
    # 3. Add insert into table command
    # 4. Add new drop table and create table to drop and create table list at the end

# DROP TABLES

dapp_data_drop = "DROP TABLE IF EXISTS dapp_data;"
contract_data_drop = "DROP TABLE IF EXISTS contract_data;"
txn_external_data_drop = "DROP TABLE IF EXISTS txn_external_data;"
txn_internal_data_drop = "DROP TABLE IF EXISTS txn_internal_data;"
dapp_contract_link_drop = "DROP TABLE IF EXISTS dapp_contract_link;"
dapp_token_price_history_drop = "DROP TABLE IF EXISTS dapp_token_price_history;"


# CREATE TABLES

dapp_data_create = ("""CREATE TABLE IF NOT EXISTS dapp_data (
                            d_name varchar PRIMARY KEY,
                            d_teaser varchar, 
                            d_platform varchar, 
                            d_cat varchar,
                            d_link varchar,
                            d_descr varchar,
                            d_status varchar,
                            d_author varchar,
                            d_license varchar,
                            d_updated varchar,
                            d_submitted varchar,
                            d_website varchar,
                            d_profile_str varchar,
                            d_push varchar,
                            d_pull varchar,
                            d_users_d varchar,
                            d_users_w varchar,
                            d_users_m varchar,
                            d_txn_1 varchar,
                            d_txn_7 varchar,
                            d_txn_30 varchar,
                            d_eth_1 varchar,
                            d_eth_7 varchar,
                            d_eth_30 varchar,
                            d_contract_list varchar,
                            d_review_list varchar,
                            d_views varchar,
                            d_clicks varchar,
                            d_ctr varchar,
                            d_rel_dapp_list varchar,
                            d_social_list varchar,
                            d_reaction_pos varchar,
                            d_reaction_neu varchar,
                            d_reaction_neg varchar,
                            d_tag_list varchar,
                            d_metamask_recom varchar,
                            d_ponzi_warning varchar,
                            added timestamp
                            );
""")



contract_data_create = ("""CREATE TABLE IF NOT EXISTS contract_data (
                            c_address varchar PRIMARY KEY,
                            c_name varchar, 
                            c_creator varchar,
                            c_token_traker varchar,
                            c_compiler varchar,
                            c_verified varchar,
                            c_creation varchar,
                            c_source_code varchar
                            );
""")
# add google big query data e.g. ERC20/721 etc. 


dapp_contract_link_create = ("""CREATE TABLE IF NOT EXISTS dapp_contract_link (
                            c_address varchar PRIMARY KEY,
                            d_name varchar 
                            );
""")



txn_external_data_create = ("""CREATE TABLE IF NOT EXISTS txn_external_data (
                            t_id varchar PRIMARY KEY,
                            t_value varchar NOT NULL,
                            t_timestamp timestamp,
                            t_from_address varchar,
                            t_to_address varchar
                            );
""")


txn_internal_data_create = ("""CREATE TABLE IF NOT EXISTS txn_internal_data (
                            t_id varchar PRIMARY KEY,
                            t_value varchar NOT NULL,
                            t_timestamp timestamp,
                            t_from_address varchar,
                            t_to_address varchar
                            );
""")


dapp_token_price_history_create = ("""CREATE TABLE IF NOT EXISTS dapp_token_price_history (
                            token_address varchar PRIMARY KEY,
                            token_dapp_name varchar,
                            token_name varchar,
                            token_price,
                            token_market_cap,
                            token_total_volume,
                            token_date timestamp
                            );
""")


# INSER RECORDS 

dapp_data_insert = ("""INSERT INTO dapp_data (
                            d_name,
                            d_teaser, 
                            d_platform, 
                            d_cat,
                            d_link,
                            d_descr,
                            d_status,
                            d_author,
                            d_license,
                            d_updated,
                            d_submitted,
                            d_website,
                            d_profile_str,
                            d_push,
                            d_pull,
                            d_users_d,
                            d_users_w,
                            d_users_m,
                            d_txn_1,
                            d_txn_7,
                            d_txn_30,
                            d_eth_1,
                            d_eth_7,
                            d_eth_30,
                            d_contract_list,
                            d_review_list,
                            d_views,
                            d_clicks,
                            d_ctr,
                            d_rel_dapp_list,
                            d_social_list,
                            d_reaction_pos,
                            d_reaction_neu,
                            d_reaction_neg,
                            d_tag_list,
                            d_metamask_recom,
                            d_ponzi_warning,
                            added) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (d_name) DO NOTHING
""")


contract_data_insert = ("""INSERT INTO contract_data (
                            c_address,
                            c_name, 
                            c_creator,
                            c_token_traker,
                            c_compiler,
                            c_verified,
                            c_creation,
                            c_source_code) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (c_address) DO NOTHING
""")

dapp_contract_link_insert = ("""INSERT INTO dapp_contract_link (
                            c_address,
                            d_name
                            ) \
                            VALUES (%s, %s)
                            ON CONFLICT (c_address) DO NOTHING
""")


txn_external_data_insert = ("""INSERT INTO txn_external_data (
                            t_id,
                            t_value,
                            t_timestamp,
                            t_from_address,
                            t_to_address) \
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (t_id) DO NOTHING
""")


txn_internal_data_insert = ("""INSERT INTO txn_internal_data (
                            t_id,
                            t_value,
                            t_timestamp,
                            t_from_address,
                            t_to_address) \
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (t_id) DO NOTHING
""")

dapp_token_price_history_insert = ("""INSERT INTO dapp_token_price_history (
                            token_address,
                            token_dapp_name,
                            token_name,
                            token_price,
                            token_market_cap,
                            token_total_volume,
                            token_date \
                            VALUES (%s,%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (token_address) DO NOTHING
""")


# QUERY LISTS
create_table_queries = [dapp_data_create, contract_data_create, dapp_contract_link_create, txn_external_data_create,txn_internal_data_create,dapp_token_price_history_create]
drop_table_queries = [dapp_data_drop, contract_data_drop, txn_external_data_drop,txn_internal_data_drop,dapp_token_price_history_drop]