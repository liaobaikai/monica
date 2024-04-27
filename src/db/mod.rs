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
    // id: u64,
    // p: String,
    // q: String, 
    // l_rba: Option<String>,
    LSCN: Option<String>,
    UCMT_SCN: Option<String>,
    // qno: Option<usize>,
    // l_sct: Option<String>,
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
            .max_connections(4)
            .min_connections(1)
            .connect_with(options).await;
        
        
        Client{pool: result.unwrap()}

    }

    pub async fn query_log_pos(&self, s: &Server) -> Option<String> {
        let sql = format!("select LSCN, UCMT_SCN from {}.yrba where qnm = ?", DB_NAME);
        let rows = sqlx::query_as::<_, YRBA>(&sql).bind(&s.service_name).fetch_all(&self.pool).await.unwrap();

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
    
            Some(yrba)
        }

    }
    
}
