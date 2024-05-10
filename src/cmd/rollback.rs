use std::{io, process::exit, sync::{Arc, Mutex}};

use dialoguer::{theme::ColorfulTheme, Select};
use log::info;
use tokio::runtime;

use crate::{cmd::{clean_ds, clean_dt, clean_jddm, error, get_last_datetime, log, query_log_position, startup, startup_jddm, update_yrba_file}, config::{self, current_log_position, get_db_info, Server, KFK_TYPE}, db, file::{clean_local_inventory, read_local_inventory_index}, ssh};

use super::{print_counter, read_log_position};

// 回退操作
pub async fn handle_command_rollback(worker_threads: usize) {

    
    let contents: String = read_local_inventory_index();
    println!("");

    // 文件不存在
    if contents.is_empty() {
        println!("There are no Interim patches applied in this inventory home.");
        println!("");
        return ;
    }
    
    let mut options = Vec::new();
    for line in contents.lines(){
        if line.is_empty() {
            continue;
        }
        let mut arr = line.split(":");
        options.push(format!("{}   {}   {}   {:>10}", arr.next().unwrap(), get_last_datetime(&line), arr.next().unwrap(), arr.next().unwrap()))
    }

    // 无有效的备份
    if options.len() == 0 {
        println!("There are no Interim patches applied in this inventory home.");
        println!("");
        return ;
    }

    println!("Choose BackupSet for rollback");
    println!("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    println!("  BackupSet ID   Date Time             BackupSet                                                          Valid Line ");
    println!(" -------------- --------------------- ------------------------------------------------------------------ ------------");

        
    // 创建Select实例
    let selection = Select::with_theme(&ColorfulTheme::default())
        .default(options.len() - 1)
        .items(&options[..])
        .interact()
        .unwrap();

    println!("* {}", options[selection]);
    

    println!("");
    // 回退
    for i in 0..3 {
        let mut input = String::new();
        println!("Do you want to continue rollback change? ");
        println!("WARNING: There is no UNDO for this change. [y|n] ");
        io::stdin().read_line(&mut input).unwrap();
        if input.starts_with("y") {
            break;
        }
        if i == 2 || input.starts_with("n") {
            println!("Bye.");
            exit(-1);
        } 
    }

    let xlsx_checksum = String::from(options[selection].split_whitespace().nth(3).unwrap());

    let dbc = db::Client::new(&get_db_info().unwrap()).await;

    // 创建线程池
    let rt = runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_io()
            .enable_time()
            .thread_name("monica")
            .build()
            .unwrap();

    let counter = Arc::new(Mutex::new(config::GLOBAL_CONFIG.servers.len()));
    let mut handles = vec![];
    for server in config::GLOBAL_CONFIG.servers.iter() {
        let counter: Arc<Mutex<usize>> = Arc::clone(&counter);
        let _dbc = dbc.clone();
        let checksum = xlsx_checksum.clone();
        let handle = rt.spawn(async move {
            start_rollback_worker(&checksum, &_dbc, counter, server).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    rt.shutdown_background();

    // 删除目录
    clean_local_inventory(&xlsx_checksum, &contents);

    info!("Rollback completed. Great!");
    println!("");

}


// 回退操作
async fn start_rollback_worker(checksum: &str, c: &db::Client, c0: Arc<Mutex<usize>>, s: &Server) {
    // 连接到复制机，需考虑异机部署
    let ssh = ssh::Client::new(s);
    // 打印进度条
    print_counter(c0);

    start_ds_worker(&ssh, c, s, &checksum).await;
    start_dt_worker(&ssh, c, s, &checksum).await;
    start_jddm_worker(&ssh, c, s, &checksum).await;

    info!("xlsx:Line: {:<2} Host: {}, Service: {}, Rollback completed", &s.rid, &s.hostname, &s.service_name);
}


async fn start_dt_worker(ssh: &ssh::Client, c: &db::Client, s: &Server, xlsx_checksum: &str){

    if s.dst_type.is_none() {
        return;
    }

    let dbps_home = match ssh.dt_dbps_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 先判断远端是否有备份
    let ls = ssh.list_remote_backupset(&dbps_home);
    if ls.len() == 0 {
        log(s, &dbps_home, "There are no Interim patches installed in this dbps home");
        return;
    }

    // 检查文件是否存在
    let (exists, remote_backupset_file) = ssh.exists_backupset(xlsx_checksum, &dbps_home);
    if !exists {
        // 备份文件已存在
        error(s, &dbps_home, &format!("BackupSet: {} not exists", remote_backupset_file));
        return;
    }

    // 从远端文件中获取位点信息
    let valid_log_pos;
    if current_log_position() {
        (valid_log_pos, _) = query_log_position(s, c.clone()).await;
    } else {
        (valid_log_pos, _) = read_log_position(&ssh, &dbps_home, s);
    }


    // 停止程序
    let (starting, starting2) = ssh.kill_ps(&format!("{}/bin/", dbps_home));
    log(s, &dbps_home, &format!("B-Start: {}, A-Start: {}", starting, starting2));

    // 回退远端文件
    rollback_remote_files(&dbps_home, &ssh, xlsx_checksum);

    // 文件上传完成后重置任务
    clean_dt(s, &dbps_home, &ssh);

    if !valid_log_pos {
        log(s, &dbps_home, "YRBA(log position) is empty, Skip start");
        return;        
    }

    if starting {
        startup(s, &dbps_home, &ssh);
    } else {
        log(s, &dbps_home, "Non-Start, Skip start");
    }

}


async fn start_jddm_worker(ssh: &ssh::Client, c: &db::Client, s: &Server, xlsx_checksum: &str){

    let input = match &s.dst_type {
        Some(s) => s,
        None => return
    };

    // kafka类型
    if !input.starts_with(KFK_TYPE) {
        return ;
    }

    let dbps_home = match ssh.jddm_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 先判断远端是否有备份
    let ls = ssh.list_remote_backupset(&dbps_home);
    if ls.len() == 0 {
        log(s, &dbps_home, "There are no Interim patches installed in this dbps home");
        return;
    }

    // 检查文件是否存在
    let (exists, remote_backupset_file) = ssh.exists_backupset(xlsx_checksum, &dbps_home);
    if !exists {
        // 备份文件已存在
        error(s, &dbps_home, &format!("BackupSet: {} not exists", remote_backupset_file));
        return;
    }

    // 从远端文件中获取位点信息
    let valid_log_pos;
    if current_log_position() {
        (valid_log_pos, _) = query_log_position(s, c.clone()).await;
    } else {
        (valid_log_pos, _) = read_log_position(&ssh, &dbps_home, s);
    }


    // 停止程序
    // ./startJddmKafkaEngine.sh start <service_name> <jddm_state>
    let (starting, starting2) = ssh.kill_ps(&format!("DPath={} ", dbps_home));
    log(s, &dbps_home, &format!("B-Start: {}, A-Start: {}", starting, starting2));

    // 回退远端文件
    rollback_remote_files(&dbps_home, &ssh, xlsx_checksum);

    // 文件上传完成后重置任务
    clean_jddm(s, &dbps_home, &ssh);

    if !valid_log_pos {
        log(s, &dbps_home, "YRBA(log position) is empty, Skip start");
        return;        
    }

    if starting {
        startup_jddm(s, &dbps_home, &ssh);
    } else {
        log(s, &dbps_home, "Non-Start, Skip start");
    }
        
}

async fn start_ds_worker(ssh: &ssh::Client, c: &db::Client, s: &Server, xlsx_checksum: &str){

    if s.src_type.is_none() {
        return;
    }
    
    let dbps_home = match ssh.ds_dbps_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 先判断远端是否有备份
    let ls = ssh.list_remote_backupset(&dbps_home);
    if ls.len() == 0 {
        log(s, &dbps_home, "There are no Interim patches installed in this dbps home");
        return;
    }

    // 检查文件是否存在
    let (exists, remote_backupset_file) = ssh.exists_backupset(xlsx_checksum, &dbps_home);
    if !exists {
        // 备份文件已存在
        error(s, &dbps_home, &format!("BackupSet: {} not exists", remote_backupset_file));
        return;
    }

    // 从远端文件中获取位点信息
    let valid_log_pos;
    let yrba_dat;
    if current_log_position() {
        (valid_log_pos, yrba_dat) = query_log_position(s, c.clone()).await;
    } else {
        (valid_log_pos, yrba_dat) = read_log_position(&ssh, &dbps_home, s);
    }


    // 停止程序
    let (starting, starting2) = ssh.kill_ps(&format!("{}/bin/", dbps_home));
    log(s, &dbps_home, &format!("B-Start: {}, A-Start: {}", starting, starting2));

    rollback_remote_files(&dbps_home, &ssh, xlsx_checksum);

    // 文件上传完成后重置任务
    clean_ds(s, &dbps_home, &ssh);

    if !valid_log_pos {
        log(s, &dbps_home, "YRBA(log position) is empty, Skip start");
        return;
    }

    // 写入yrba文件
    update_yrba_file(s, &dbps_home, &yrba_dat, &ssh);

    if starting {
        startup(s, &dbps_home, &ssh);
    } else {
        log(s, &dbps_home, "Non-Start, Skip start");
    }
        
}


fn rollback_remote_files(dbps_home: &str, ssh: &ssh::Client, xlsx_checksum: &str){

    // 通过备份文件恢复远端程序
    // 通过备份目录中的 sha256sum.txt 检查文件是否有效
    // tar -xf monica.backupset/monica.backupset-<date>.tar
    // sha256sum -c bin/monica.sha256sum.txt
    if !ssh.exec_rollback_backupset(dbps_home, xlsx_checksum) {
        // 回退失败
        abnormal_exit_rollback("Some files sha256sum did not pass");
    }
}


pub fn abnormal_exit_rollback(cause: &str){
    println!("Rollback failed:");
    println!("  CAUSE: {}", cause);
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}