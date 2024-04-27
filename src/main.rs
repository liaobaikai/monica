use std::{collections::HashMap, path::Path, process::exit};

use config::Server;
use flexi_logger::{DeferredNow, Duplicate, Logger};
use log::{debug, error, info, Record};
use structopt::StructOpt;

use crate::config::Manifest;
mod config;
mod db;
mod ssh;
mod filex;

////////////////////////////////////////////////////////////////////////////////
// 日志打印
pub fn log_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    let level = record.level();
    write!(
        w,
        "{} localhost {:5} {}",
        now.format("%m %d %H:%M:%S"),
        &level.to_string(),
        &record.args()
    )
}
////////////////////////////////////////////////////////////////////////////////




// 启动任务
async fn start_upgrade_worker(c: &db::Client, s: &Server) {
    let opt = config::Opt::from_args();

    // 从数据库中获取位点信息
    let mut yrba_dat = String::new();
    let valid_log_pos = match c.query_log_pos(&s).await {
        Some(yrba) => {
            yrba_dat = yrba;
            true
        }
        None => false
    };
    info!("HOST: {}, SERVICE: {}, DS::YRBA:: {}", &s.hostname, &s.service_name, yrba_dat);

    // 连接到复制机，需考虑异机部署
    let ssh = ssh::Client::new("upgrade", s);
    match &s.src_type {
        Some(src_type) => {
            // 源端
            match ssh.ds_dbps_home(s) {
                Some(dbps_home) => {
                    
                    info!("Host: {}, Service: {}, DS::DBPS_HOME:: {}", &s.hostname, &s.service_name, dbps_home);
                    // 停止程序
                    let (starting, stopped) = ssh.kill_ps(&format!("{}/bin/", dbps_home));
                    info!("Host: {}, Service: {}, DS::DBPS_HOME:: {}, before-starting: {}, after-starting: {}.", &s.hostname, &s.service_name, dbps_home, starting, stopped);

                    // oracle类型需要获取版本
                    let _src_type = match src_type.starts_with("ORACLE"){
                        true => format!("{}_{}", src_type, ssh.get_vagentd_version(&dbps_home)),
                        false => src_type.to_string()
                    };
                    debug!("_src_type: {}", _src_type);
                    let manifest = config::METADATA.ds.get(&_src_type).unwrap();
                    upgrade_remote_files(manifest, &dbps_home, &ssh, &s);
                },
                None => {
                    if !src_type.is_empty() {
                        error!("Host: {}, Service: {}, DS::DBPS_HOME:: <NONE> : No such directory", &s.hostname, &s.service_name);
                        return ;
                    }
                }
            }


        },
        None => {}
        
    };

    
    match &s.dst_type {
        Some(dst_type) => {
            
            // 目标端
            match ssh.dt_dbps_home(s) {
                Some(dbps_home) => {
                    info!("Host: {}, Service: {}, DT::DBPS_HOME:: {}", &s.hostname, &s.service_name, dbps_home);
                    // 停止程序
                    let (starting, stopped) = ssh.kill_ps(&format!("{}/bin/", dbps_home));
                    info!("Host: {}, Service: {}, DT::DBPS_HOME:: {}, before-starting: {}, after-starting: {}.", &s.hostname, &s.service_name, dbps_home, starting, stopped);
                
                    let _dst_type = match dst_type.starts_with("ORACLE"){
                        true => format!("{}_{}", dst_type, ssh.get_vagentd_version(&dbps_home)),
                        false => dst_type.to_string()
                    };
                    debug!("_dst_type: {}", _dst_type);
                    let manifest = config::METADATA.ds.get(&_dst_type).unwrap();
                    upgrade_remote_files(manifest, &dbps_home, &ssh, &s);
                },
                None => {
                    if !dst_type.is_empty() {
                        error!("Host: {}, Service: {}, DT::DBPS_HOME:: <NONE> : No such directory", &s.hostname, &s.service_name);
                        return ;
                    }
                }
            }

            // kafka类型
            if !dst_type.starts_with("KAFKA") {
                return ;
            }

            // dt dy
            match ssh.jddm_home(s) {
                Some(dbps_home) => {
                    // 记录前面是否正在运行的，如果是正在运行的话，则更新程序后需要启动
                    // ./startJddmKafkaEngine.sh start <service_name> <jddm_state>
                    let jddm_start_with = ssh.get_jddm_start_with(&dbps_home);
                    info!("Host: {}, Service: {}, JDDM_HOME:: {}, Jddm_start_with: {}", &s.hostname, &s.service_name, dbps_home, jddm_start_with);
                    let (starting, starting2) = ssh.kill_ps(&format!("DPath={} ", dbps_home));
                    info!("Host: {}, Service: {}, JDDM_HOME:: {}, before-starting: {}, after-starting: {}.", &s.hostname, &s.service_name, dbps_home, starting, starting2);
                    let manifest = config::METADATA.dt.get(&format!("{}_JAVA", dst_type)).unwrap();
                    upgrade_remote_files(manifest, &dbps_home, &ssh, &s);
                },
                None => {
                    if !dst_type.is_empty() {
                        error!("Host: {}, Service: {}, JDDM_HOME:: <NONE> : No such directory", &s.hostname, &s.service_name);
                        return ;
                    }
                }
            }

        },
        None => {}
    }
    

}

// 预检查：本地文件检查
fn precheck_local_files(manifest: &Manifest){
    let opt = config::Opt::from_args();

    // 解压本地文件            
    filex::extract_compressed_files(&opt.dataxone_base, &manifest.package, &manifest.dir);
    for f in manifest.file.iter() {
        let local_file = Path::new(&opt.dataxone_base).join(&manifest.dir).join(f);

        if !local_file.exists() {
            error!("[PreChecks]:: File {} : No Found <<<", local_file.display());
            abnormal_exit_not_found();
        }
        info!("[PreChecks]:: File {} : Found", local_file.display());
    }
    
}

// 升级文件：上传文件
fn upgrade_remote_files(manifest: &Manifest, dbps_home: &str, ssh: &ssh::Client, s: &Server){
    let opt = config::Opt::from_args();

    // 备份远端程序
    if ssh.exec_gen_backupset(&dbps_home, &manifest.file.join(" ")) {
        // 备份成功
        info!("Host: {}, Service: {}, DT::DBPS_HOME:: {}, Backup OK, Backupset record in {}", &s.hostname, &s.service_name, dbps_home, config::BACKUPUP_INDEX_FILENAME);
    }
    
    for f in manifest.file.iter() {
        let local_file = Path::new(&opt.dataxone_base).join(&manifest.dir).join(f);
        let remote_file = Path::new(dbps_home).join(f);
        ssh.scp_send(local_file, remote_file);
    }
}

// 预检查
async fn start_precheck_worker(s: &Server){
    // 连接到复制机，需考虑异机部署
    let ssh = ssh::Client::new("PreChecks", s);

    match &s.src_type {
        Some(src_type) => {
            // 源端
            match ssh.ds_dbps_home(s) {
                Some(dbps_home) => {
                    info!("[PreChecks]:: Host: {}, Service: {}, DS::DBPS_HOME:: {} : Found", &s.hostname, &s.service_name, dbps_home);

                    // oracle类型需要获取版本
                    let _src_type = match src_type.starts_with("ORACLE"){
                        true => format!("{}_{}", src_type, ssh.get_vagentd_version(&dbps_home)),
                        false => src_type.to_string()
                    };
                    debug!("[PreChecks]:: Src_type: {}", _src_type);
                    let ds_manifest = config::METADATA.ds.get(&_src_type).unwrap();
                    precheck_local_files(ds_manifest);
                },
                None => {
                    if !src_type.is_empty() {
                        error!("[PreChecks]:: Host: {}, Service: {}, DS::DBPS_HOME:: <NONE> : Not Found <<<", &s.hostname, &s.service_name);
                        abnormal_exit_not_found();
                    }
                }
            }
        },
        None => {}
        
    };

    
    match &s.dst_type {
        Some(dst_type) => {
            
            // 目标端
            match ssh.dt_dbps_home(s) {
                Some(dbps_home) => {
                    info!("[PreChecks]:: Host: {}, Service: {}, DT::DBPS_HOME:: {} : Found", &s.hostname, &s.service_name, dbps_home);
                    // oracle类型需要获取版本
                    let _dst_type = match dst_type.starts_with("ORACLE"){
                        true => format!("{}_{}", dst_type, ssh.get_vagentd_version(&dbps_home)),
                        false => dst_type.to_string()
                    };
                    debug!("[PreChecks]:: Dst_type: {}", _dst_type);
                    let dt_manifest = config::METADATA.dt.get(&_dst_type).unwrap();
                    precheck_local_files(dt_manifest);
                    
                },
                None => {
                    if !dst_type.is_empty() {
                        error!("[PreChecks]:: Host: {}, Service: {}, DT::DBPS_HOME:: <NONE> : No Found <<<", &s.hostname, &s.service_name);
                        abnormal_exit_not_found();
                    }
                }
            }

            // kafka类型
            if !dst_type.starts_with("KAFKA") {
                return ;
            }

            // dt dy
            match ssh.jddm_home(s) {
                Some(dbps_home) => {
                    info!("[PreChecks]:: Host: {}, Service: {}, JDDM_HOME:: {} : Found", &s.hostname, &s.service_name, dbps_home);
                    let java_manifest = config::METADATA.dt.get(&format!("{}_JAVA", dst_type)).unwrap();
                    precheck_local_files(java_manifest);
                },
                None => {
                    if !dst_type.is_empty() {
                        error!("Host: {}, Service: {}, JDDM_HOME:: <NONE> : No Found <<<", &s.hostname, &s.service_name);
                        abnormal_exit_not_found();
                    }
                }
            }

        },
        None => {}
    }
    
}

fn abnormal_exit_not_found(){
    println!("PreChecks failure:::");
    println!("  CAUSE: No such file or directory");
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}

fn abnormal_exit_data_duplication(cause: &str){
    println!("PreChecks failure:::");
    println!("  CAUSE: {}", cause);
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}

#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    let opt: config::Opt = config::Opt::from_args();

    // Logger::try_with_str(if opt.debug { "debug" } else { "info" })?
    Logger::try_with_str("debug").unwrap()
    .duplicate_to_stderr(Duplicate::Debug)
    // .log_to_file(FileSpec::default())
    // .write_mode(WriteMode::BufferAndFlush)
    .format(log_format)
    .start().unwrap();

    if opt.action == "precheck" {
        // 预检查：数据重复
        info!("[PreChecks]:: Data duplication check ...");
        {
            let mut data: HashMap<String, usize> = HashMap::new();
            for (index, server) in config::GLOBAL_CONFIG.servers.iter().enumerate() {
                let key = server.to_hash();
                if !data.get(&key).is_none() {
                    let index2 = data.get(&key).unwrap();
                    error!("[PreChecks]:: Data duplication check failed");
                    abnormal_exit_data_duplication(&format!("Data violated unique, current row seq#{}, duplication row seq#{}", index+2, index2+2));
                    break;
                }
                data.insert(key, index);
                if server.src_type.is_some() && server.dst_type.is_some() {
                    data.insert(server.to_hash_with_src(), index);
                    data.insert(server.to_hash_with_dst(), index);
                }
            }
        }

        // 升级或回退
        for (rindex, server) in config::GLOBAL_CONFIG.servers.iter().enumerate() {
            info!("[PreChecks]:: Scaning row seq#{} ...", rindex+2);
            start_precheck_worker(server).await;
        }

    } else if opt.action == "rollback" {
        // 回退
        // for server in config::GLOBAL_CONFIG.servers.iter() {
        //     start_worker(, server).await;
        // }

    } else if opt.action == "upgrade" {
        let mut db_info = db::DBInfo::default();
        db_info.db_host = opt.mysql_host;
        db_info.db_port = opt.mysql_port;
        db_info.db_username = opt.mysql_username;
        db_info.db_password = opt.mysql_password;

        // 升级或回退
        let dbc = db::Client::new(&db_info).await;
        for server in config::GLOBAL_CONFIG.servers.iter() {
            start_upgrade_worker(&dbc, server).await;
        }
    } else {
        // 不支持
        println!("Invalid parameter: --action={}", opt.action);
    }

    Ok(())
}
