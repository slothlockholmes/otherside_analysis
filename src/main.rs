use std::str::FromStr;
use ethers::prelude::{*};
use eyre::Result;
mod environment;
mod web3;
mod db;


#[tokio::main]
async fn main() -> Result<()>{
    environment::load();

    // Setup info
    let key: String = environment::get_value("ALCHEMY_API_KEY");
    let provider = web3::get_provider(&key).await?;

    // // To filter for transferFrom
    let start: U64 = U64::from(14688876);          // When start public sale was called
    let current: U64 = U64::from(14689098);
    let latest: U64 = U64::from(14752568);         // Latest block number
    let otherside_address = H160::from_str("0x34d85c9CDeB23FA97cb08333b511ac86E1C4E258")?;
    let t0 = H256::from_str("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")?; // this is topic0 of PublicSaleMint event
    let conn = db::get_conn_to_db()?;
    db::clean_db(&conn)?;

    // Script to get TransferFrom events
    for i in 45000..=99999 {
        println!("{}", i);
        let hex = format!("0x{:0>64}", format!("{:x}", i));
        let t3 = H256::from_str(hex.as_str())?;
        let filter = Filter::new()
            .from_block(BlockNumber::Number(start))
            .to_block(BlockNumber::Number(latest))
            .address(otherside_address)
            .topic0(ValueOrArray::Value(t0))
            .topic3(ValueOrArray::Value(t3));
        let logs = provider.get_logs(&filter).await?;
        for log in &logs {
            db::add_tf_log_to_db(&conn, &log, i)?;
        }
    }

    // Script to get OrdersMatched events
    let opensea_address = H160::from_str("0x7f268357a8c2552623316e2562d90e642bb538e5")?;
    let t0 = H256::from_str("0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9")?; // this is topic0 of OrdersMatched event
    let mut i = 14688876;
    let mut j = i;
    while i < 14752568 {
        j = i + 100;
        println!("{},{}", i, j);
        let filter = Filter::new()
            .from_block(BlockNumber::Number(U64::from(i)))
            .to_block(BlockNumber::Number(U64::from(j)))
            .address(opensea_address)
            .topic0(ValueOrArray::Value(t0));
        let logs = provider.get_logs(&filter).await?;
        for log in &logs {
            db::add_om_log_to_db(&conn, &log)?;
        }
        i += 100;
    }

    let creation: u64 = 14672945;       // When the contract was created
    let start: u64 = 14688876;          // When start public sale was called
    let unofficial_end: u64 = 14689597; // When all the lands were minted
    let official_end: u64 = 14689607;   // When stop public sale was called
    let last: u64 = 14752568;
    let now: u64 = 14752568;            // Current block now (this is to look for gas wasted on calling mintLands even though the auction is already closed.)
    let otherside_address = H160::from_str("0x34d85c9CDeB23FA97cb08333b511ac86E1C4E258")?;
    let conn = db::get_conn_to_db()?;

    for n in start..=last {
        println!("{}", n);
        if let Some(block) = provider.get_block_with_txs(n).await? {
            let txs: Vec<Transaction> = block.transactions
                .into_iter()
                .filter(|x| x.to == Some(otherside_address))
                .collect();
            // for Transactionss
            db::add_txs_to_db(&conn, &txs)?;
            // for Transaction Receipts
            for tx in &txs {
                if let Some(receipt) = provider.get_transaction_receipt(tx.hash).await? {
                    db::add_receipt_to_db(&conn, &receipt)?;
                }
            }
        }
    }
    Ok(())
}
