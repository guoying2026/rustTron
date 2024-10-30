use sqlx::{mysql::{MySqlConnectOptions, MySqlPoolOptions}, MySqlPool};
use serde::Deserialize;
use std::time::Duration;
use config::{Config, File};
use dotenv::dotenv;
use std::env;
use bigdecimal::BigDecimal;
use std::str::FromStr;
use sqlx::types::time::PrimitiveDateTime;

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
    token_info: Option<TokenInfo>,
    from: Option<String>,
    to: Option<String>,
    value: Option<String>,
}

#[derive(Deserialize, Debug)]
struct TokenInfo {
    symbol: String,
    decimals: u32, // 添加 decimals 字段
}

#[derive(Debug)]
struct PendingRecord {
    id: i64,
    pay_token: BigDecimal,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    dotenv().ok(); // 加载 .env 文件

    // 读取配置文件
    let settings = Config::builder()
        .add_source(File::with_name("config"))
        .build()?
        .try_deserialize::<Settings>()?;

    // 获取数据库连接字符串
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // 解析数据库连接字符串为 MySqlConnectOptions
    let options = database_url.parse::<MySqlConnectOptions>()?;

    // 创建数据库连接池，使用 connect_with 方法
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    let address = settings.address.clone();

    // 无限循环，持续监测 is_pay = 0 的记录
    loop {
        // 获取最早的未支付记录的创建时间
        let min_create_time: Option<PrimitiveDateTime> = sqlx::query_scalar!(
            "SELECT create_time FROM pay_records WHERE is_pay = 0 ORDER BY id ASC LIMIT 1"
        )
            .fetch_optional(&pool)
            .await?;

        if let Some(min_time) = min_create_time {
            // 打印人类可读的日期和时间
            println!("最早的未支付记录创建时间: {}", min_time);

            let min_timestamp = min_time.assume_utc().unix_timestamp();
            println!("最小创建时间的 UNIX 时间戳: {}", min_timestamp); // 打印 min_timestamp
            // 正式网的链接
            // let trc20_url = format!(
            //     "https://api.trongrid.io/v1/accounts/{}/transactions/trc20?only_confirmed=true&limit=10",
            //     address
            // );
            let trc20_url = format!(
                "https://nile.trongrid.io/v1/accounts/{}/transactions/trc20?only_confirmed=true&limit=10&min_timestamp={}",
                address,
                min_timestamp
            );

            println!("发现未支付的记录，开始处理...");
            fetch_and_process_transactions(trc20_url.clone(), pool.clone(), address.clone()).await?;
        } else {
            println!("没有未支付的记录，等待中...");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

// 异步任务处理函数
async fn fetch_and_process_transactions(
    url: String,
    pool: MySqlPool,
    address: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut page = 1;
    let mut current_url = url;

    // 1. 获取所有 is_pay = 0 的记录，按 id 升序排序
    let pending_records = sqlx::query_as!(
        PendingRecord,
        "SELECT id, pay_token FROM pay_records WHERE is_pay = 0 ORDER BY id ASC"
    )
        .fetch_all(&pool)
        .await?;

    if pending_records.is_empty() {
        println!("没有未支付的记录，结束本次处理。");
        return Ok(());
    }

    // 获取 is_pay = 0 的最小 id 记录
    let min_id = pending_records[0].id;

    // 获取小于 min_id 的最大 id 且 is_pay = 1 的 transaction_id
    let last_transaction = sqlx::query!(
        "SELECT transaction_id FROM pay_records WHERE id < ? AND is_pay = 1 ORDER BY id DESC LIMIT 1",
        min_id
    )
        .fetch_optional(&pool)
        .await?;

    let last_transaction_id = last_transaction.map(|record| record.transaction_id);

    // 将未支付的记录存入一个可变的集合，方便后续匹配和移除
    // 使用 BigDecimal 类型的金额
    let mut pending_amounts: Vec<(i64, BigDecimal)> = pending_records
        .iter()
        .map(|record| {
            (
                record.id,
                record.pay_token.clone(),
            )
        })
        .collect();

    loop {
        let response = reqwest::get(&current_url).await?;
        if response.status().is_success() {
            let transaction_data: TransactionData = response.json().await?;
            // 打印解析后的 `transaction_data`
            // println!("解析后的交易数据: {:#?}", transaction_data);
            for (i, transaction) in transaction_data.data.iter().enumerate() {
                // 如果当前交易的 tx_id 等于上一个记录的 transaction_id，则停止遍历
                if let Some(ref last_tx_id) = last_transaction_id {
                    if &transaction.tx_id == last_tx_id {
                        println!("已达到已处理的最后一条交易记录，停止遍历。");
                        return Ok(());
                    }
                }

                // 安全地解构 Option 值
                let from = match &transaction.from {
                    Some(f) => f.clone(),
                    None => continue, // 如果 from 为空，跳过此交易
                };

                let to = match &transaction.to {
                    Some(t) => t.clone(),
                    None => continue, // 如果 to 为空，跳过此交易
                };

                // 只处理转入指定地址的交易
                if to != address {
                    continue;
                }

                let value_str = match &transaction.value {
                    Some(v) => v.clone(),
                    None => continue, // 如果 value 为空，跳过此交易
                };
                // 检查 token_info 是否存在，并且 symbol 是否为 "USDT"。这个是只获取usdt的链接
                let token_info = match transaction.token_info.as_ref() {
                    Some(info) if info.symbol == "USDT" => info,
                    _ => continue, // 如果 token_info 不存在或 symbol 不是 "USDT"，跳过此交易
                };
                // let token_info = match transaction.token_info.as_ref() {
                //     Some(info) => info,
                //     None => continue, // 如果 token_info 为空，跳过此交易
                // };

                // 获取 decimals 值
                let decimals = token_info.decimals;

                // 将 value_str 转换为 BigDecimal
                let value_decimal = BigDecimal::from_str(&value_str).unwrap_or_else(|_| BigDecimal::from(0));

                // 计算可读金额
                let divisor = BigDecimal::from(10u64.pow(decimals));
                let readable_amount = &value_decimal / &divisor;

                // 打印可读金额
                println!(
                    "[TRC20] 第{}页第{}笔交易：代币 {} 转账金额 {:.6}, 从: {}, 到: {}, 交易ID: {}",
                    page,
                    i + 1,
                    token_info.symbol,
                    readable_amount,
                    from,
                    to,
                    transaction.tx_id
                );
                for (id, amount) in &pending_amounts {
                    println!("记录ID: {}, 金额: {}", id, amount);
                    println!("当前交易金额: {}, 钱包地址: {}", readable_amount, from);
                }
                // 检查当前交易金额和发送者是否在未支付记录中
                if let Some(pos) = pending_amounts.iter().position(|(_, amount)| {
                    // 判断 readable_amount 是否在 amount - 2 的范围内
                    let in_range = &readable_amount >= &(amount - BigDecimal::from(2)) && &readable_amount < amount;

                    // 比较 amount 和 readable_amount 的前三位小数
                    let amount_truncated = amount.with_scale(3); // 取 amount 的前三位小数
                    let readable_truncated = readable_amount.with_scale(3); // 取 readable_amount 的前三位小数
                    let decimals_match = amount_truncated == readable_truncated;

                    // 满足范围和小数匹配条件
                    in_range && decimals_match
                }) {
                    let (id, _) = pending_amounts.remove(pos);

                    // 开启事务
                    let mut tx = pool.begin().await?;

                    // 获取 user_id 和当前的 gold_coins
                    let (user_id, current_gold_coins): (i64, BigDecimal) = sqlx::query_as::<_, (i64, BigDecimal)>(
                        "SELECT user.id as user_id, user.gold_coins as gold_coins FROM user
     INNER JOIN pay_records ON user.id = pay_records.user_id
     WHERE pay_records.id = ?"
                    )
                        .bind(id) // 绑定 `id` 参数
                        .fetch_one(&mut *tx)
                        .await?;
                    println!("开始更新记录，交易ID: {}, 用户ID: {}, 金额: {}", transaction.tx_id, user_id, readable_amount);

                    // 计算 pay_before_gold_coins 和 pay_after_gold_coins
                    let pay_before_gold_coins = current_gold_coins.clone();
                    let pay_after_gold_coins = &current_gold_coins + &readable_amount;

                    // 更新 pay_records 表
                    sqlx::query!(
                        "UPDATE pay_records
                         SET transaction_id = ?, pay_token = ?, is_pay = 1,
                             pay_before_gold_coins = ?, pay_after_gold_coins = ?
                         WHERE id = ?",
                        transaction.tx_id,
                        readable_amount.to_string(),
                        pay_before_gold_coins.to_string(),
                        pay_after_gold_coins.to_string(),
                        id
                    )
                        .execute(&mut *tx)
                        .await?;
                    // 更新 user 表中的 gold_coins
                    sqlx::query!(
                        "UPDATE user SET gold_coins = ? WHERE id = ?",
                        pay_after_gold_coins.to_string(),
                        user_id
                    )
                        .execute(&mut *tx)
                        .await?;

                    // 提交事务
                    tx.commit().await?;

                    println!("成功更新记录，id: {}", id);
                }
            }

            // 如果 pending_amounts 已空，说明所有待处理记录已更新，可停止遍历
            if pending_amounts.is_empty() {
                println!("所有待处理记录已更新，停止遍历。");
                return Ok(());
            }

            if let Some(next_page) = transaction_data.next.clone() {
                current_url = format!("https://nile.trongrid.io{}", next_page);
                page += 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            } else {
                println!("没有更多页面，停止遍历。");
                break;
            }
        } else {
            println!("获取交易失败：HTTP 状态码 {}", response.status());
            break;
        }
    }

    Ok(())
}