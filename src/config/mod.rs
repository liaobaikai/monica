use std::{collections::HashMap, fs, process::exit};

use chrono::Local;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use calamine::{open_workbook, Reader, Xlsx};

lazy_static! {
    pub static ref METADATA: Metadata = get_metadata().unwrap();
    pub static ref GLOBAL_CONFIG: GlobalConfig = getconfig().unwrap();
}

pub const BACKUPUP_INDEX_FILENAME: &str = "monica.backupset.index";

#[derive(Debug, Default)]
pub struct GlobalConfig {
    pub servers: Vec<Server>
}

// 主机名	端口	协议	用户名	密码	基础目录	服务名
#[derive(Debug, Default)]
pub struct Server {
    pub hostname: String,
    pub port: String,
    pub protocol: String, // SSH2
    pub username: String, 
    pub password: Option<String>, 
    pub service_base_path: String, // 服务基础安装目录
    pub service_name: String, // 服务名
    pub src_type: Option<String>,  // 源端数据库类型
    pub dst_type: Option<String>,  // 目标端类型
}

impl Server {
    pub fn to_hash(&self) -> String {
        let src_type = match &self.src_type {
            Some(s) => s,
            None => "",
        };
        let dst_type = match &self.dst_type {
            Some(s) => s,
            None => "",
        };
        let s = format!("{}_{}_{}_{}_{}_{}_{}_{}", self.hostname, self.port, self.protocol, self.username, self.service_base_path, self.service_name, src_type, dst_type);
        s
    }

    pub fn to_hash_with_dst(&self) -> String {
        let dst_type = match &self.dst_type {
            Some(s) => s,
            None => "",
        };
        let s = format!("{}_{}_{}_{}_{}_{}__{}", self.hostname, self.port, self.protocol, self.username, self.service_base_path, self.service_name, dst_type);
        s
    }

    pub fn to_hash_with_src(&self) -> String {
        let src_type = match &self.src_type {
            Some(s) => s,
            None => "",
        };
        let s = format!("{}_{}_{}_{}_{}_{}_{}_", self.hostname, self.port, self.protocol, self.username, self.service_base_path, self.service_name, src_type);
        s
    }
}

// 更新元数据
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Metadata {
    pub ds: HashMap<String, Manifest>,
    pub dt: HashMap<String, Manifest>,
}

// 需要更新的包
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Manifest {
    pub package: String, 
    pub dir: String,
    pub file: Vec<String>, // 需升级的文件
}

// // 源端
// #[derive(Clone, Debug, Default, Deserialize, Serialize)]
// pub struct DataSource {
//     polardb_x: Package,
//     mysql: Package,
//     oracle11g: Package,
//     oracle12c: Package,
//     oracle19c: Package
// }

// // 目标端
// #[derive(Clone, Debug, Default, Deserialize, Serialize)]
// pub struct DataTarget {
//     kafka: Package,
//     kafka_java: Package,
//     mysql: Package,
//     oracle11g: Package,
//     oracle12c: Package,
//     oracle19c: Package
// }




#[derive(Debug, StructOpt)]
#[structopt(name = "goman", about = "An example of StructOpt usage.")]
pub struct Opt {
    #[structopt(short, long)]
    pub debug: bool,

    /// upgrade, rollback, precheck
    #[structopt(
        long,
        default_value = "precheck"
    )]
    pub action: String, 

    /// config file
    #[structopt(
        short = "D",
        long,
        default_value = "/Users/lbk/Downloads"
    )]
    pub dataxone_base: String,

    /// config file
    #[structopt(
        short = "i",
        long,
        default_value = "/Users/lbk/Desktop/123.xlsx"
    )]
    pub input: String,

    #[structopt(
        short = "h",
        long,
        // default_value = "127.0.0.1"
        default_value = "192.168.6.251"
    )]
    pub mysql_host: String,

    #[structopt(
        short = "P",
        long,
        default_value = "3306"
    )]
    pub mysql_port: String,

    #[structopt(
        short = "u",
        long,
        default_value = "root"
    )]
    pub mysql_username: String,

    #[structopt(
        short = "p",
        long,
        default_value = "dsgdata@000"
        // default_value = ""
    )]
    pub mysql_password: String,


    #[structopt(
        short = "m",
        long,
        default_value = "/Users/lbk/monica/metadata.json"
        // default_value = ""
    )]
    pub meta: String,
    
}

// read config file
fn getconfig() -> Option<GlobalConfig> {
    let opt = Opt::from_args();
    let mut config = GlobalConfig::default();
    let mut workbook: Xlsx<_>  = open_workbook(opt.input.clone()).expect("Cannot open file");
    config.servers = Vec::new();
    for sheet in workbook.sheet_names() {
        let range = workbook.worksheet_range(&sheet).unwrap();
        for (rindex, row) in range.rows().enumerate() {
            if rindex == 0 {
                // 排除表头
                let now = Local::now().format("%m %d %H:%M:%S").to_string();
                println!("{} localhost INFO  Open {}, Sheet {}, Skip table header", now, opt.input, sheet.to_string());
                continue;
            }
            let mut s = Server::default();
            for (index, cell) in row.iter().enumerate() {
                // println!("cell{}:{}", index, cell)
                // ell0:主机名
                // cell1:端口
                // cell2:协议
                // cell3:用户名
                // cell4:密码
                // cell5:基础目录
                // cell6:服务名
                // cell7:源库类型
                // cell8:目标端类型
                match index {
                    0 => s.hostname = cell.to_string(),
                    1 => s.port = cell.to_string(),
                    2 => s.protocol = cell.to_string(),
                    3 => s.username = cell.to_string(),
                    4 => {
                        s.password = if cell.to_string().is_empty() {
                            None
                        } else {
                            Some(cell.to_string())
                        };
                    },
                    5 => s.service_base_path = cell.to_string(),
                    6 => s.service_name = cell.to_string(),
                    7 => {
                        s.src_type = if cell.to_string().is_empty() {
                            None
                        } else {
                            Some(cell.to_string().to_uppercase())
                        };
                        
                    },
                    8 => {
                        s.dst_type = if cell.to_string().is_empty() {
                            None
                        } else {
                            Some(cell.to_string().to_uppercase())
                        };
                    }
                    _ => {
                        println!("Invalid index: {} on row: {}", index, rindex+1);
                        exit(-1);
                    }
                }

                
            }

            match &s.src_type {
                Some(src_type) => {
                    if src_type == "ORACLE" {

                    } else if !METADATA.ds.contains_key(src_type) {
                        println!("Invalid src_type: {} on row: {}", src_type, rindex+1);
                        exit(-1);
                    }
                },
                None => {}
            }
            
            match &s.dst_type {
                Some(dst_type) => {
                    if dst_type == "ORACLE" {

                    } else if !METADATA.dt.contains_key(dst_type) {
                        println!("Invalid dst_type: {} on row: {}", dst_type, rindex+1);
                        exit(-1);
                    }
                },
                None => {}
            }

            config.servers.push(s);
        }
    }

    // println!("{:?}", config);

    Some(config)
}


// 
fn get_metadata() -> Option<Metadata> {
    let opt = Opt::from_args();
    
    let json = fs::read_to_string(opt.meta.clone()).unwrap();
    let meta: Metadata = serde_json::from_str(&json).unwrap();

    Some(meta)
}