use ethers::prelude::*;
use eyre::Result;

pub async fn get_provider(key: &str)-> Result<ethers::providers::Provider<ethers::providers::Http>> {
    let url = format!("https://eth-mainnet.alchemyapi.io/v2/{}", key);
    let provider = Provider::<Http>::try_from(url).expect("could not instantiate HTTP Provider");
    Ok(provider)
}