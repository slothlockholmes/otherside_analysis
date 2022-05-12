use rusqlite::{params, Connection, Result};
use ethers::prelude::*;

pub fn clean_db(conn: &Connection) -> Result<()> {
    conn.execute(
        "DROP TABLE IF EXISTS orders_matched_logs",
        [],
    )?;
    conn.execute(
        "DROP TABLE IF EXISTS transfer_from_logs",
        [],
    )?;
    conn.execute(
        "DROP TABLE IF EXISTS tx",
        [],
    )?;
    conn.execute(
        "DROP TABLE IF EXISTS receipt",
        [],
    )?;
    conn.execute(
        "CREATE TABLE orders_matched_logs (
                id                          INTEGER PRIMARY KEY,
                block_number                TEXT,
                transaction_hash            TEXT,
                seller                      TEXT,
                buyer                       TEXT
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE transfer_from_logs (
                id                          INTEGER PRIMARY KEY,
                block_number                TEXT,
                transaction_hash            TEXT,
                sender                      TEXT,
                recipient                   TEXT,
                token_id                    TEXT
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE tx (
                id                          INTEGER PRIMARY KEY,
                hash                        TEXT,
                nonce                       TEXT,
                block_hash                  TEXT,
                block_number                TEXT,
                transaction_index           TEXT,
                sender                      TEXT,
                recipient                   TEXT,
                value                       TEXT,
                gas_price                   TEXT,
                gas                         TEXT,
                input                       TEXT,
                v                           TEXT,
                r                           TEXT,
                s                           TEXT,
                transaction_type            TEXT,
                access_list                 TEXT,
                max_priority_fee_per_gas    TEXT,
                max_fee_per_gas             TEXT,
                chain_id                    TEXT
        )",
        [],
    )?;
    // Note that I only chose the fields that I wanted,
    conn.execute(
        "CREATE TABLE receipt (
                id                          INTEGER PRIMARY KEY,
                transaction_hash            TEXT,
                cumulative_gas_used         TEXT,
                effective_gas_price         TEXT,
                gas_used                    TEXT,
                status                      TEXT
        )",
        [],
    )?;
    Ok(())
}

pub fn get_conn_to_db() -> Result<Connection> {
    let conn = Connection::open("data.db")?;
    Ok(conn)
}

pub fn add_txs_to_db(conn: &Connection, txs: &Vec<Transaction>) -> Result<()> {
    for tx in txs.iter() {
        conn.execute(
            "INSERT into tx (hash, nonce, block_hash, block_number, transaction_index, sender, recipient, value, gas_price, gas, input, v, r, s, transaction_type, access_list, max_priority_fee_per_gas, max_fee_per_gas, chain_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            params![
                format!("{:?}", tx.hash),
                format!("{:?}", tx.nonce),
                format!("{:?}", tx.block_hash.unwrap()),
                format!("{:?}", tx.block_number.unwrap()),
                format!("{:?}", tx.transaction_index.unwrap()),
                format!("{:?}", tx.from),
                format!("{:?}", tx.to.unwrap()),
                format!("{:?}", tx.value),
                format!("{:?}", tx.gas_price.unwrap()),
                format!("{:?}", tx.gas),
                format!("{:?}", tx.input.to_string()),
                format!("{:?}", tx.v),
                format!("{:?}", tx.r),
                format!("{:?}", tx.s),
                format!("{:?}", tx.transaction_type.map_or_else(|| U64::zero(), |v: U64| v)),
                format!("{:?}", tx.access_list.as_ref().map_or_else(|| 0, |_v| 1)),
                format!("{:?}", tx.max_priority_fee_per_gas.map_or_else(|| U256::zero(), |v: U256| v)),
                format!("{:?}", tx.max_fee_per_gas.map_or_else(|| U256::zero(), |v: U256| v)),
                format!("{:?}", tx.chain_id.map_or_else(|| U256::zero(), |v: U256| v)),
            ],
        )?;
    }
    Ok(())
}

pub fn add_receipt_to_db(conn: &Connection, receipt: &TransactionReceipt) -> Result<()> {
    conn.execute(
        "INSERT into receipt (transaction_hash, cumulative_gas_used, effective_gas_price, gas_used, status) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            format!("{:?}", receipt.transaction_hash),
            format!("{:?}", receipt.cumulative_gas_used),
            format!("{:?}", receipt.effective_gas_price.unwrap()),
            format!("{:?}", receipt.gas_used.map_or_else(|| U256::zero(), |v: U256| v)),
            format!("{:?}", receipt.status.unwrap()),
        ],
    )?;
    Ok(())
}

pub fn add_tf_log_to_db(conn: &Connection, tf_log: &Log, i: i32) -> Result<()> {
    conn.execute(
        "INSERT into transfer_from_logs (block_number, transaction_hash, sender, recipient, token_id) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            format!("{:?}", tf_log.block_number.unwrap()),
            format!("{:?}", tf_log.transaction_hash.unwrap()),
            format!("{:?}", H160::from(tf_log.topics[1])),
            format!("{:?}", H160::from(tf_log.topics[2])),
            format!("{:?}", i), // can probably make cleaner by parsing topics[2] instead of passing i
        ],
    )?;
    Ok(())
}

pub fn add_om_log_to_db(conn: &Connection, om_log: &Log) -> Result<()> {
    conn.execute(
        "INSERT into orders_matched_logs (block_number, transaction_hash, seller, buyer) VALUES (?1, ?2, ?3, ?4)",
        params![
            format!("{:?}", om_log.block_number.unwrap()),
            format!("{:?}", om_log.transaction_hash.unwrap()),
            format!("{:?}", H160::from(om_log.topics[1])),
            format!("{:?}", H160::from(om_log.topics[2])),
        ],
    )?;
    Ok(())
}