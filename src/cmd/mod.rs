use std::sync::{Arc, Mutex};

use comfy_table::Table;
use log::{error, info};

use crate::{config::{self, current_log_position, Server, YRBA_FILENAME}, db, file::read_local_inventory_index, ssh};

pub mod apply;
pub mod rollback;
pub mod precheck;
pub mod lsinventory;
pub mod backup;

pub const START_SERVICE_SCRIPT: &str = "start_flow.sh";
pub const START_JDDM_M_SCRIPT: &str = "startMonitorJddmEngine.sh";
pub const START_JDDM_SCRIPT: &str = "startJddmKafkaEngine.sh";
pub const JDDM_START_WITH_FILE: &str = "bin/monica.started";

// 通用步骤
pub fn clean_ds(s: &Server, dbps_home: &str, ssh: &ssh::Client){
    clean(s, dbps_home, "ds_clean", ssh);
}

pub fn clean_dt(s: &Server, dbps_home: &str, ssh: &ssh::Client){
    clean(s, dbps_home, "dt_clean", ssh);
}

// 重置任务、清理任务
fn clean(s: &Server, dbps_home: &str, script: &str, ssh: &ssh::Client){
    let cmd = format!("export DBPS_HOME={} && cd $DBPS_HOME/scripts && sh ./{}.sh", dbps_home, script);
    let (status, _, stderr) = ssh.exec_cmd_with_status(&cmd);
    if status == 0 {
        log(s, dbps_home, "Cleanup command has been issued");
    } else {
        error(s, dbps_home, &format!("Cleanup command issuance failed, cause: {}", stderr));
    }
}

// 重置任务、清理任务
pub fn clean_jddm(s: &Server, dbps_home: &str, ssh: &ssh::Client){
    let cmd = format!("export DBPS_HOME={} && rm -rf $DBPS_HOME/table/* && rm -rf $DBPS_HOME/cache/* ", dbps_home);
    let (status, _, stderr) = ssh.exec_cmd_with_status(&cmd);
    if status == 0 {
        log(s, dbps_home, "Cleanup command has been issued");
    } else {
        error(s, dbps_home, &format!("Cleanup command issuance failed, cause: {}", stderr));
    }
}


// 启动任务
pub fn startup(s: &Server, dbps_home: &str, ssh: &ssh::Client){
    let cmd = format!("export DBPS_HOME={} && cd $DBPS_HOME && rm bin/monica.* && cd $DBPS_HOME/scripts && sh ./{}", 
        dbps_home, START_SERVICE_SCRIPT);
    let (status, _, stderr) = ssh.exec_cmd_with_status(&cmd);
    if status == 0 {
        log(s, dbps_home, "Startup command has been issued");
    } else {
        error(s, dbps_home, &format!("Startup command issuance failed, cause: {}", stderr));
    }
}

// 清理垃圾文件
// bin/monica.*     --ALL
// lib/monica.*     --JDDM
// module/monica.*  --JDDM
pub fn clean_monica_cache_file(dbps_home: &str, ssh: &ssh::Client){
    ssh.exec_cmd_with_status(&format!("cd {} && rm bin/monica.* lib/monica.* module/monica.* 2>/dev/null", dbps_home));
}

// 启动任务
// 如果脚本启动不加 >/dev/null的话，会话会一直等待数据返回，导致无法下一步。
pub fn startup_jddm(s: &Server, dbps_home: &str, ssh: &ssh::Client){
    
    let mut cmd = format!("export DBPS_HOME={} && cd $DBPS_HOME", dbps_home);
    cmd = format!("{} export START_WITH=\"$(cat $DBPS_HOME/{} 2>/dev/null)\" && ", cmd, JDDM_START_WITH_FILE);
    // 清理垃圾文件
    cmd = format!("{} rm {{bin,lib,module}}/monica.* 2>/dev/null && ", cmd);
    cmd = format!("{} ./{} start {} \"$START_WITH\" >/dev/null && ", cmd, START_JDDM_M_SCRIPT, s.service_name);
    cmd = format!("{} ./{} start {} \"$START_WITH\" >/dev/null", cmd, START_JDDM_SCRIPT, s.service_name);
    
    let (status, _, stderr) = ssh.exec_cmd_with_status(&cmd);
    if status == 0 {
        log(s, dbps_home, "Startup command has been issued");
    } else {
        error(s, dbps_home, &format!("Startup command issuance failed, cause: {}", stderr));
    }
}   

// 只需要更新源端
pub fn update_yrba_file(s: &Server, dbps_home: &str, yrba_dat: &str, ssh: &ssh::Client) {
    let cmd: String = format!("export DBPS_HOME={} && cd $DBPS_HOME/rmp && echo \"{}\" > {}", dbps_home, yrba_dat, YRBA_FILENAME);
    let (status, _, stderr) = ssh.exec_cmd_with_status(&cmd);
    if status == 0 {
        log(s, dbps_home, &format!("Written log position ({}) to file rmp/{}", yrba_dat, YRBA_FILENAME));
    } else {
        error(s, dbps_home, &format!("Write failed, cause: {}", stderr));
    }
}

pub fn log(s: &Server, dbps_home: &str, msg: &str){
    info!("xlsx:Line: {:<2} Host: {}, Service: {}, DBPS_HOME: {}, {}", &s.rid, &s.hostname, &s.service_name, dbps_home, msg);
}

pub fn error(s: &Server, dbps_home: &str, msg: &str){
    error!("xlsx:Line: {:<2} Host: {}, Service: {}, DBPS_HOME: {}, {}", &s.rid, &s.hostname, &s.service_name, dbps_home, msg);
}

// 打印计数器
pub fn print_counter(c: Arc<Mutex<usize>>){
    let size = config::GLOBAL_CONFIG.servers.len();
    let mut lock = c.lock().unwrap();
    *lock -= 1;
    let remain_count: usize = lock.to_string().parse().unwrap();
    info!("xlsx:Processing:{}/{}", (size - remain_count), size);
}


// 从远端服务器获取位点信息
pub fn read_log_position(ssh: &ssh::Client, dbps_home: &str, s: &Server) -> (bool, String) {

    let mut yrba_dat = String::new();
    // 从远端文件中获取位点信息
    let valid_log_pos = match ssh.get_log_pos(&dbps_home) {
        Some(yrba) => {
            yrba_dat = yrba;
            info!("xlsx:Line: {:<2} Host: {}, Service: {}, Read YRBA(log position): {}", &s.rid, &s.hostname, &s.service_name, yrba_dat);
            true
        }
        None => {
            info!("xlsx:Line: {:<2} Host: {}, Service: {}, Read YRBA(log position) is empty <<<", &s.rid, &s.hostname, &s.service_name);
            false
        }
    };
    
    (valid_log_pos, yrba_dat)

}


// 从数据库中获取位点信息
async fn query_log_position(s: &Server, c: db::Client) -> (bool, String) {

    // 从数据库中获取位点信息
    let mut yrba_dat: String = String::new();
    let valid_log_pos = match c.query_log_pos(&s).await {
        Some(yrba) => {
            yrba_dat = yrba;
            info!("xlsx:Line: {:<2} Host: {}, Service: {}, Query YRBA(log position): {}", &s.rid, &s.hostname, &s.service_name, yrba_dat);
            true
        }
        None => {
            info!("xlsx:Line: {:<2} Host: {}, Service: {}, Query YRBA(log position) is empty <<<", &s.rid, &s.hostname, &s.service_name);
            false
        }
    };

    (valid_log_pos, yrba_dat)

}


// 打印备份表
pub fn print_local_inventory_tab() -> String {

    let mut table = Table::new();
    table.set_header(vec!["BackupSet ID", "Date Time", "BackupSet", "Valid Line"]);

    let contents = read_local_inventory_index();

    for line in contents.lines(){
        let mut arr = line.split(":");
        table.add_row(vec![arr.next().unwrap(), &line[line.len()-19..], arr.next().unwrap(), arr.next().unwrap()]);

    }
    table.to_string()
}

// 获取最后的日期
// 886021f16bfa:886021f16bfadb194defb77bb67e0774b1ec3a2b2630700f4db01155f373909d:3:2024-05-08 11:32:00
pub fn get_last_datetime(line: &str) -> String {
    if line.is_empty() {
        return String::new();
    }
    let mut v = line.trim_end_matches("\n").rsplit(":");
    format!("{2}:{1}:{0}", v.next().unwrap(), v.next().unwrap(), v.next().unwrap())
}