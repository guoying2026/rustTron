use serde::Deserialize;
use tokio::task;
use std::time::Duration;
use config::{Config, File};

#[derive(Debug, Deserialize)]
struct Settings {
    address: String,
}

#[derive(Deserialize, Debug)]
struct TransactionData {
    data: Vec<TransactionDetail>,
    next: Option<String>,
}

#[derive(Deserialize, Debug)]
struct TransactionDetail {
    #[serde(rename = "txID", alias = "transaction_id")]
    tx_id: String,
    raw_data: Option<RawData>,
    ret: Option<Vec<Ret>>,
    token_info: Option<TokenInfo>,
    from: Option<String>,
    to: Option<String>,
    value: Option<String>,
}

#[derive(Deserialize, Debug)]
struct RawData {
    contract: Vec<Contract>,
}

#[derive(Deserialize, Debug)]
struct Contract {
    parameter: Parameter,
}

#[derive(Deserialize, Debug)]
struct Parameter {
    value: Value,
}

#[derive(Deserialize, Debug)]
struct Value {
    amount: Option<u64>,
}

#[derive(Deserialize, Debug)]
struct Ret {
    fee: Option<u64>,
    net_fee: Option<u64>,
}

#[derive(Deserialize, Debug)]
struct TokenInfo {
    symbol: String,
    decimals: u8,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // 读取配置文件
    let settings = Config::builder()
        .add_source(File::with_name("config"))
        .build()?
        .try_deserialize::<Settings>()?;

    let address = settings.address;

    let trx_url = format!("https://nile.trongrid.io/v1/accounts/{}/transactions?limit=10", address);
    let trc20_url = format!("https://nile.trongrid.io/v1/accounts/{}/transactions/trc20?limit=10", address);

    // 创建两个并发任务
    let trx_task = task::spawn(fetch_and_process_transactions(trx_url, "TRX"));
    let trc20_task = task::spawn(fetch_and_process_transactions(trc20_url, "TRC20"));

    // 等待任务并正确传播错误
    trx_task.await??;
    trc20_task.await??;

    Ok(())
}

// 异步任务处理函数
async fn fetch_and_process_transactions(
    url: String,
    tx_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut page = 1;
    let mut current_url = url;

    loop {
        let response = reqwest::get(&current_url).await?;
        if response.status().is_success() {
            let transaction_data: TransactionData = response.json().await?;

            for (i, transaction) in transaction_data.data.iter().enumerate() {
                if tx_type == "TRX" {
                    if let Some(raw_data) = &transaction.raw_data {
                        let amount_trx = raw_data.contract[0]
                            .parameter
                            .value
                            .amount
                            .unwrap_or(0) as f64
                            / 1_000_000.0;
                        let fee_trx = transaction.ret.as_ref().unwrap()[0].fee.unwrap_or(0) as f64
                            / 1_000_000.0;
                        let net_fee_trx =
                            transaction.ret.as_ref().unwrap()[0].net_fee.unwrap_or(0) as f64
                                / 1_000_000.0;
                        let total_cost_trx = amount_trx + fee_trx + net_fee_trx;

                        println!(
                            "[TRX] 第{}页第{}笔交易：总花费 {:.6} TRX, 手续费 {:.6} TRX, 转账金额 {:.6} TRX, 网络费用 {:.6} TRX, 交易ID: {}",
                            page,
                            i + 1,
                            total_cost_trx,
                            fee_trx,
                            amount_trx,
                            net_fee_trx,
                            transaction.tx_id
                        );
                    }
                } else if tx_type == "TRC20" {
                    let from = transaction
                        .from
                        .clone()
                        .unwrap_or_else(|| "未知地址".to_owned());
                    let to = transaction.to.clone().unwrap_or_else(|| "未知地址".to_owned());
                    let value = transaction.value.clone().unwrap_or_else(|| "0".to_owned());
                    let token_info = transaction.token_info.as_ref().unwrap();
                    let token_amount = value.parse::<f64>().unwrap_or(0.0)
                        / 10f64.powi(token_info.decimals as i32);

                    println!(
                        "[TRC20] 第{}页第{}笔交易：代币 {} 转账金额 {:.6}, 从: {}, 到: {}, 交易ID: {}",
                        page,
                        i + 1,
                        token_info.symbol,
                        token_amount,
                        from,
                        to,
                        transaction.tx_id
                    );
                }
            }

            if let Some(next_page) = transaction_data.next {
                current_url = format!("https://nile.trongrid.io{}", next_page);
                page += 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            } else {
                break;
            }
        } else {
            println!("获取交易失败：HTTP 状态码 {}", response.status());
            break;
        }
    }

    Ok(())
}