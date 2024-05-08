use std::{process::exit, time::Duration};

use log::error;
use sqlx::{prelude::FromRow, MySql, Pool};

use crate::config::Server;


#[derive(Debug, Default)]
pub struct DBInfo {
    pub db_host: String,
    pub db_port: String,
    pub db_username: String,
    pub db_password: String

}

#[derive(Debug, FromRow)]
pub struct YRBA {
    LSCN: Option<String>,
    UCMT_SCN: Option<String>,
}

pub const DB_NAME: &'static str = "dataxone_pmon";

#[derive(Debug, Clone)]
pub struct Client {
    pool: Pool<MySql>
}

impl Client {

    pub async fn new(db_info: &DBInfo) -> Self {

        let port: u16 = db_info.db_port.parse().unwrap();
        let options = sqlx::mysql::MySqlConnectOptions::new()
            .host(&db_info.db_host)
            .port(port as u16)
            .username(&db_info.db_username)
            .password(&db_info.db_password)
            .ssl_mode(sqlx::mysql::MySqlSslMode::Disabled)
            .timezone(Some(String::from("+08:00")))
            .database(&DB_NAME);

        let result = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .max_lifetime(None)
            .acquire_timeout(Duration::from_secs(10800))
            .idle_timeout(Duration::from_secs(10800))
            .connect_with(options).await;
        
        
        Client{pool: result.unwrap()}

    }

    pub async fn query_log_pos(&self, s: &Server) -> Option<String> {
        let sql = format!("select LSCN, UCMT_SCN from {}.yrba where qnm = ?", DB_NAME);

        // thread 'monica' panicked at src\db\mod.rs:54:102:
        // called `Result::unwrap()` on an `Err` value: PoolTimedOut
        // 1、cause: error returned from database: 1159 (08S01): Got timeout reading communication packets
        // 2、Database data fetch failed, cause: pool timed out while waiting for an open connection
        // 连接超时，需保持长连接
        let rows = match sqlx::query_as::<_, YRBA>(&sql).bind(&s.service_name).fetch_all(&self.pool).await {
            Ok(r) => r,
            Err(e) => {
                error!("xlsx:Line: {:<2} Database data fetch failed, cause: {}", s.rid, e);
                exit(-1);
            }
        };

        if rows.len() == 0 {
            None
        } else {
            let row: &YRBA = rows.iter().next().unwrap();
            let mut yrba = match &row.LSCN {
                Some(value) => format!("{},", value),
                None => String::from(",")
            };
    
            yrba = match &row.UCMT_SCN {
                Some(value) => format!("{}{}", yrba, value),
                None => format!("{}", yrba),
            };

            if yrba == "," {
                return None;
            }
    
            Some(yrba)
        }

    }
    
}
