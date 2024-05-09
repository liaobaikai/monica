use std::{collections::HashMap, fs::{self, File}, io::{self, Error}, process::exit};

use lazy_static::lazy_static;
use log::error;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use calamine::{open_workbook, Reader, Xlsx};

use crate::db::{self, DBInfo};

lazy_static! {
    pub static ref METADATA: Metadata = get_metadata().unwrap();
    pub static ref GLOBAL_CONFIG: GlobalConfig = get_config().unwrap();
}

pub const BACKUPUP_DIR: &str = ".monica";
pub const BACKUPUP_RECYCLE_BIN_DIR: &str = ".monica/.recyclebin";
pub const BACKUPUP_TMP_DIR: &str = ".monica/.tmp";
pub const BACKUPUP_FILE_PREFIX: &str = "backupset";
pub const BACKUPUP_INDEX_FILENAME: &str = "backupset.index";
pub const BACKUPUP_SHA256SUM_FILENAME: &str = "monica.sha256sum.txt";
pub const YRBA_FILENAME: &str = "yrba.dat";
pub const LOCAL_INVENTORY_DIR: &str = "inventory";
pub const KFK_TYPE: &str = "KAFKA";

pub const ROLE_DS: usize = 0;
pub const ROLE_DT: usize = 10;
pub const ROLE_JDDM: usize = 11;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct GlobalConfig {
    pub servers: Vec<Server>
}

// 主机名	端口	协议	用户名	密码	基础目录	服务名
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Server {
    pub rid: usize,
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

    fn to_a_str(&self, src_type: &str, dst_type: &str) -> String {
        let s = format!("{}_{}_{}_{}_{}_{}_{}_{}", self.hostname, self.port, self.protocol, self.username, self.service_base_path, self.service_name, src_type, dst_type);
        s
    }

    pub fn to_hash(&self) -> String {
        return self.to_a_str(match &self.src_type {
            Some(s) => s,
            None => "",
        }, match &self.dst_type {
            Some(s) => s,
            None => "",
        });
    }

    pub fn to_hash_with_dst(&self) -> String {
        return self.to_a_str("", match &self.dst_type {
            Some(s) => s,
            None => "",
        });
    }

    pub fn to_hash_with_src(&self) -> String {
        return self.to_a_str(match &self.src_type {
            Some(s) => s,
            None => "",
        }, "");
    }
}

// 更新元数据
#[derive(Deserialize, Serialize)]
pub struct Metadata {
    pub ds: HashMap<String, Manifest>,
    pub dt: HashMap<String, Manifest>,
}

// 需要更新的包
#[derive(Deserialize, Serialize)]
pub struct Manifest {
    pub package: String, 
    pub dir: String,
    pub file: Vec<String>, // 需升级的文件
}

// 通用参数
#[derive(Debug, StructOpt)]
pub struct ComArgument {

    #[structopt(short, long)]
    pub debug: bool,

    /// DataXone install dir
    #[structopt(short = "D", long, default_value = "/data/dataxone")]
    pub basedir: String,

    /// <Current dir>/.monica
    #[structopt(long, default_value = ".monica")]
    pub datadir: String,

    /// User input file
    #[structopt(short, long, parse(try_from_str=parse_file_path))]
    pub input_file: String,
    
    /// Manifest file, Read only.
    #[structopt(short, long, parse(try_from_str=parse_file_path))]
    pub manifest_file: String,
    
    /// Parallel workers
    #[structopt(short, long, default_value="4")]
    pub worker_threads: usize,

    /// User input file read start with number. 
    #[structopt(short, long, default_value="2")]
    pub xlsx_start_with: usize,

    /// read the latest log location on database.
    #[structopt(short="l", long)]
    pub current_log_position: bool,

}

// 通用参数
#[derive(Debug, StructOpt)]
pub struct PreCheckArgument {

    #[structopt(short, long)]
    pub debug: bool,

    /// DataXone install dir
    #[structopt(short = "D", long, default_value = "/data/dataxone")]
    pub basedir: String,

    /// <Current dir>/.monica
    #[structopt(long, default_value = ".monica")]
    pub datadir: String,

    /// User input file
    #[structopt(short, long, parse(try_from_str=parse_file_path))]
    pub input_file: String,
    
    /// Manifest file, Read only.
    #[structopt(short, long, parse(try_from_str=parse_file_path))]
    pub manifest_file: String,

    /// Parallel workers
    #[structopt(short, long, default_value="4")]
    pub worker_threads: usize,

    /// User input file read start with number. 
    #[structopt(short, long, default_value="2")]
    pub xlsx_start_with: usize,

}

// 补丁升级专用参数
#[derive(Debug, StructOpt)]
pub struct PatchArgument {

    ////////////////////////////////////////////////////////////////////////////
    // #[structopt(subcommand)]
    // pub com: ComArgument,
    #[structopt(short, long)]
    pub debug: bool,

    /// DataXone install dir
    #[structopt(short = "D", long, default_value = "/data/dataxone")]
    pub basedir: String,

    /// <Current dir>/.monica
    #[structopt(long, default_value = ".monica")]
    pub datadir: String,

    /// User input file
    #[structopt(short, long, parse(try_from_str=parse_file_path))]
    pub input_file: String,
    
    /// Manifest file, Read only.
    #[structopt(short, long, parse(try_from_str=parse_file_path))]
    pub manifest_file: String,

    /// Parallel workers
    #[structopt(short, long, default_value="4")]
    pub worker_threads: usize,

    /// User input file read start with number. 
    #[structopt(short, long, default_value="2")]
    pub xlsx_start_with: usize,
    ////////////////////////////////////////////////////////////////////////////

    #[structopt(short = "c", long, default_value = "16384")]
    pub chunk_size: usize,

    #[structopt(short, long)]
    pub quiet: bool,

    #[structopt(short, long = "--skip-check")]
    pub skip_check: bool,

    /// DataXone platform database host
    #[structopt(short = "h", long)]
    pub mysql_host: String,

    /// DataXone platform database port
    #[structopt(short = "P", long, default_value = "3306")]
    pub mysql_port: String,

    /// DataXone platform database username
    #[structopt(short = "u", long, default_value = "dataxone")]
    pub mysql_username: String,

    /// DataXone platform database user password
    #[structopt( short = "p", long)]
    pub mysql_password: String,

    /// Skip backup and backup again, Skip appied patch and apply patch again.
    #[structopt(short, long)]
    pub force: bool,

    /// Read the latest log location from DataXone database.
    #[structopt(short="l", long)]
    pub current_log_position: bool,

}

#[derive(Debug, StructOpt)]
pub enum Command {
    Patch(PatchArgument),
    Rollback(PatchArgument),
    Precheck(PreCheckArgument),
    Lsinventory(ComArgument),
    Backup(PatchArgument),
}

#[derive(Debug, StructOpt)]
#[structopt(name = "monica", about = "SuperSync tasks upgrade tools on dataxone environment.")]
pub struct Opt {
    #[structopt(subcommand)]
    pub command: Command, 
}

fn parse_file_path(p: &str) -> Result<String, io::Error> {
    match File::open(p) {
        Ok(_) => {Ok(String::from(p))},
        Err(e) => {
            Err(Error::new(io::ErrorKind::NotFound, format!("{}: {}", p, e)))
        }
    }
}

pub fn get_input_file() -> String {
    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) | Command::Rollback(a) => {
            a.input_file
        },
        Command::Precheck(a) => {
            a.input_file
        },
        Command::Lsinventory(a) => {
            a.input_file
        },
    }
}

pub fn get_manifest_file() -> String {
    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) | Command::Rollback(a) => {
            a.manifest_file
        },
        Command::Precheck(a) => {
            a.manifest_file
        },
        Command::Lsinventory(a) => {
            a.manifest_file
        },
    }
}

pub fn get_basedir() -> String {
    
    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) | Command::Rollback(a) => {
            a.basedir
        },
        Command::Precheck(a) => {
            a.basedir
        },
        Command::Lsinventory(a)  => {
            a.basedir
        },
    }

}


pub fn get_datadir() -> String {
    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) | Command::Rollback(a) => {
            a.datadir
        },
        Command::Precheck(a) => {
            a.datadir
        },
        Command::Lsinventory(a) => {
            a.datadir
        },
    }
}

pub fn get_debug() -> bool {
    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) | Command::Rollback(a) => {
            a.debug
        },
        Command::Precheck(a) => {
            a.debug
        },
        Command::Lsinventory(a) => {
            a.debug
        },
    }
}

pub fn get_xlsx_start_with() -> usize {
    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) | Command::Rollback(a) => {
            a.xlsx_start_with
        },
        Command::Precheck(a) => {
            a.xlsx_start_with
        },
        Command::Lsinventory(a) => {
            a.xlsx_start_with
        },
    }
}

// pub fn get_worker_threads() -> usize {
//     match Opt::from_args().command {
//         Command::Patch(a) => {
//             a.worker_threads
//         },
//         Command::Check(a) | Command::Rollback(a) => {
//             a.worker_threads
//         },
//     }
// }

pub fn get_chunk_size() -> usize {
    match Opt::from_args().command {
        Command::Patch(a) => {
            a.chunk_size
        },
        _ => 16384,
    }
}

pub fn is_force() -> bool {
    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) => {
            a.force
        },
        _ => false,
    }
}

pub fn current_log_position() -> bool {
    match Opt::from_args().command {
        Command::Patch(a) => {
            a.current_log_position
        },
        Command::Backup(_) => {
            true
        },
        Command::Rollback(a) => {
            a.current_log_position
        },
        _ => false,
    }
}

pub fn get_db_info() -> Option<DBInfo> {

    match Opt::from_args().command {
        Command::Patch(a) | Command::Backup(a) | Command::Rollback(a) => {
            let dbi = db::DBInfo{
                db_host: a.mysql_host,
                db_port: a.mysql_port,
                db_username: a.mysql_username,
                db_password: a.mysql_password
            };
            Some(dbi)
        },
        _ => {
            None
        },
    }
}

pub fn get_local_inventory_dir() -> String {
    let dir = get_datadir();
    let local_inventory_dir = format!("{}/{}", dir, LOCAL_INVENTORY_DIR);
    local_inventory_dir
}

// read config file
fn get_config() -> Option<GlobalConfig> {
    let mut config = GlobalConfig::default();
    let mut workbook: Xlsx<_>  = open_workbook(get_input_file()).expect("Cannot open file");
    config.servers = Vec::new();
    let mut data: HashMap<String, usize> = HashMap::new();
    let xlsx_start_with = get_xlsx_start_with();

    for sheet in workbook.sheet_names() {
        let range = workbook.worksheet_range(&sheet).unwrap();
        for (rindex, row) in range.rows().enumerate() {
            let rid = rindex + 1;
            if rid < xlsx_start_with {
                // 排除表头
                println!("Open {}, Sheet {}, start with {}, skip Line {}", get_input_file(), sheet.to_string(), xlsx_start_with, rid);
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
                    0 => {
                        if cell.to_string().is_empty() {
                            abnormal_exit_data_empty(rid, index);
                        } else {
                            s.hostname = cell.to_string();
                        }
                    },
                    1 => {
                        if cell.to_string().is_empty() {
                            abnormal_exit_data_empty(rid, index);
                        } else {
                            s.port = cell.to_string();
                        }
                    }
                    2 => {
                        if cell.to_string().is_empty() {
                            abnormal_exit_data_empty(rid, index);
                        } else {
                            s.protocol = cell.to_string();
                        }
                    },
                    3 => {
                        if cell.to_string().is_empty() {
                            abnormal_exit_data_empty(rid, index);
                        } else {
                            s.username = cell.to_string();
                        }
                    },
                    4 => s.password = if cell.to_string().is_empty() {
                        None
                    } else {
                        Some(cell.to_string())
                    },
                    5 => {
                        if cell.to_string().is_empty() {
                            abnormal_exit_data_empty(rid, index);
                        } else {
                            s.service_base_path = cell.to_string();
                        }
                    },
                    6 => {
                        if cell.to_string().is_empty() {
                            abnormal_exit_data_empty(rid, index);
                        } else {
                            s.service_name = cell.to_string();
                        }
                    },
                    7 => s.src_type = if cell.to_string().is_empty() {
                        None
                    } else {
                        Some(cell.to_string().to_uppercase())
                    },
                    8 => s.dst_type = if cell.to_string().is_empty() {
                        None
                    } else {
                        Some(cell.to_string().to_uppercase())
                    },
                    _ => {
                        error!("Data check failed, invalid index {} on row {}", index, rid);
                        exit(-1);
                    }
                }

                
            }

            match &s.src_type {
                Some(src_type) => {
                    if src_type == "ORACLE" {

                    } else if !METADATA.ds.contains_key(src_type) {
                        error!("Data check failed, invalid src_type {} on row {}", src_type, rid);
                        exit(-1);
                    }
                },
                None => {}
            }
            
            match &s.dst_type {
                Some(dst_type) => {
                    if dst_type == "ORACLE" {

                    } else if !METADATA.dt.contains_key(dst_type) {
                        error!("Data check failed, invalid dst_type {} on row {}", dst_type, rid);
                        exit(-1);
                    }
                },
                None => {
                    if s.src_type.is_none() {
                        // 非法输出
                        error!("Data check failed, src_type and dst_type Cannot be empty");
                        abnormal_exit_precheck("Column H and column I cannot be empty");
                    }
                }
            }

            // 检查重复数据
            let key = s.to_hash();

            let v = match data.get(&key) {
                Some(v) => {
                    Some(v)
                }
                None => {
                    match data.get(&s.to_hash_with_src()) {
                        Some(v) => {
                            Some(v)
                        },
                        None => {
                            match data.get(&s.to_hash_with_dst()) {
                                Some(v) => {
                                    Some(v)
                                },
                                None => None
                            }
                        }
                    }
                }
            };

            if v.is_some() {
                error!("Data check failed");
                // The third and fourth rows conflict
                abnormal_exit_precheck(&format!("The data in the {} and {} rows conflict", rid, v.unwrap()));
                break;
            }

            data.insert(key.clone(), rid);
            if s.src_type.is_some() && s.dst_type.is_some() {
                data.insert(s.to_hash_with_src(), rid);
                data.insert(s.to_hash_with_dst(), rid);
            }

            // 行编号
            s.rid = rid;
            config.servers.push(s);
        }
    }
    
    Some(config)
}


// 
fn get_metadata() -> Option<Metadata> {

    let json = fs::read_to_string(get_manifest_file()).unwrap();
    let meta: Metadata = serde_json::from_str(&json).unwrap();

    Some(meta)
}


pub fn get_ds_manifest(input: &str, version: Option<String>) -> Option<&Manifest> {
    
    let m;
    match input {
        "ORACLE" => match version {
            Some(v) => m = METADATA.ds.get(&format!("{}_{}", input, v)).unwrap(),
            None => {
                return None;
            }
        },
        &_ => {
            // 其他类型
            m = METADATA.ds.get(input).unwrap();
        }
    }

    Some(m)
}

pub fn get_dt_manifest(input: &str, version: Option<String>) -> Option<&Manifest> {
    
    let m;
    match input {
        "ORACLE" => match version {
            Some(v) => m = METADATA.dt.get(&format!("{}_{}", input, v)).unwrap(),
            None => {
                return None;
            }
        },
        &_ => {
            // 其他类型
            m = METADATA.dt.get(input).unwrap();
        }
    }

    Some(m)
}

pub fn get_jddm_manifest(input: &str) -> &Manifest {
    let manifest = METADATA.dt.get(&format!("{}_JAVA", input)).unwrap();
    return manifest;
}

pub fn get_yrba_file_name() -> String {
    let s = format!("{}.yrba.dat", env!("CARGO_PKG_NAME"));
    s
}

// pub fn get_backup_cache_file() -> String {
//     let s = format!("{}/backup.log", get_datadir());
//     s
// }

// pub fn get_patch_cache_file() -> String {
//     let s = format!("{}/patch.log", get_datadir());
//     s
// }

pub fn abnormal_exit_precheck(cause: &str){
    println!("PreChecks failure:");
    println!("  CAUSE: {}", cause);
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}

// pub fn abnormal_exit_patch(flag: &str,cause: &str){
//     println!("Patch failure:");
//     println!("  CAUSE: {}", cause);
//     println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
//     println!("Bye.");
//     exit(-1);
// }

pub fn abnormal_exit_data_empty(rid: usize, index: usize){
    error!("Data check failed: Data empty");
    println!("PreChecks failure:");
    println!("  CAUSE: {}", format!("Row {} column {} cannot be empty", rid, char::from(index as u8 +65)));
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}


pub fn abnormal_exit_backup(cause: &str){
    println!("Backup failure:");
    println!("  CAUSE: {}", cause);
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}

pub fn abnormal_exit_patch(cause: &str){
    println!("Patch apppy failure:");
    println!("  CAUSE: {}", cause);
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}