use std::{path::Path, sync::{Arc, Mutex}};

use log::info;
use structopt::StructOpt;
use tokio::runtime;
use crate::{cmd::query_log_position, config::{self, Command, Manifest, Opt, Server}, db, file, ssh};

use super::{error, get_last_datetime, log, print_counter, JDDM_START_WITH_FILE};


// 备份事件处理
pub async fn handle_command_backup(worker_threads: usize){

    let dbc = db::Client::new(&config::get_db_info().unwrap()).await;
    // 创建线程池
    let rt = runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_io()
            .enable_time()
            .thread_name("monica")
            .build()
            .unwrap();

    let counter = Arc::new(Mutex::new(config::GLOBAL_CONFIG.servers.len()));
    let xlsx_checksum = file::sha256sum(Path::new(&config::get_input_file()).to_path_buf());
    let mut handles = vec![];
    for server in config::GLOBAL_CONFIG.servers.iter() {
        let counter: Arc<Mutex<usize>> = Arc::clone(&counter);
        let _dbc = dbc.clone();
        let checksum = xlsx_checksum.clone();
        let handle = rt.spawn(async move {
            start_backup_worker(&checksum, &_dbc, counter, server).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    rt.shutdown_background();

    info!("Backup completed. Great!");
    println!("");


}

// 备份后生成 sha256sum 
// backup-<sha256sum>
// 文件序号，ds , dt
// 2, ok, ok, ok       ## 到kafka
// 3, , ok             ## 非集中部署
// 4, ok,              ## 非集中部署
// 
// 如果上传过程中失败，则需要自动回退操作
async fn start_backup_worker(xlsx_checksum: &str, c: &db::Client, c0: Arc<Mutex<usize>>, s: &Server){
    
    
    // if current_log_position() {
    // }
    // 连接到复制机，需考虑异机部署
    let ssh = ssh::Client::new(s);
    
    print_counter(c0);

    start_ds_worker(&ssh, c, s, xlsx_checksum).await;
    start_dt_worker(&ssh, s, xlsx_checksum);
    start_jddm_worker(&ssh, s, xlsx_checksum);

    info!("xlsx:Line: {:<2} Host: {}, Service: {}, Backup completed", &s.rid, &s.hostname, &s.service_name);

}


fn start_dt_worker(ssh: &ssh::Client, s: &Server, xlsx_checksum: &str){

    let input = match &s.dst_type {
        Some(s) => s,
        None => return
    };

    if let Some(ckp) = file::read_backup_checkpoint(s, config::ROLE_DT, xlsx_checksum) {
        // 查询到检查点
        log(s, &ckp.split(":").nth(0).unwrap(), &format!("Backed up on {}", get_last_datetime(&ckp)));
        if !config::is_force() {
            return;
        }
        log(s, &ckp.split(":").nth(0).unwrap(), "Covering backed up");
    }

    let dbps_home = match ssh.dt_dbps_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 判断远端是否有备份集
    let (exists, remote_backupset_file) = ssh.exists_backupset(xlsx_checksum, &dbps_home);
    if exists {
        // 备份文件已存在
        log(s, &dbps_home, &format!("BackupSet: {} exists", remote_backupset_file));
    } else {

        if let Some(manifest) = config::get_dt_manifest(input, ssh.get_ss_version(&input, &dbps_home)) {
            backup_remote_files(xlsx_checksum, manifest, &dbps_home, &ssh, &s, false);
        } else {
            error(s, &dbps_home, "Oracle version read failed <<<")
        }

    }

    // 写入检查点
    file::write_backup_checkpoint(&dbps_home, s, config::ROLE_DT, xlsx_checksum);

}


fn start_jddm_worker(ssh: &ssh::Client, s: &Server, xlsx_checksum: &str){

    let input = match &s.dst_type {
        Some(s) => s,
        None => return
    };

    // kafka类型
    if !input.starts_with(config::KFK_TYPE) {
        return ;
    }

    if let Some(ckp) = file::read_backup_checkpoint(s, config::ROLE_JDDM, xlsx_checksum) {
        // 查询到检查点
        log(s, &ckp.split(":").nth(0).unwrap(), &format!("Backed up on {}", get_last_datetime(&ckp)));
        if !config::is_force() {
            return;
        }
        log(s, &ckp.split(":").nth(0).unwrap(), "Covering backed up");
    }

    let dbps_home = match ssh.jddm_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 将启动参数写入到 $dbps_home/bin/monica.started 中
    // ./startJddmKafkaEngine.sh start <service_name> <jddm_state>
    if ssh.write_jddm_starts_with(&dbps_home) {
        log(s, &dbps_home, &format!("Jddm_starts_with written to {}", JDDM_START_WITH_FILE));
    }

    // 判断远端是否有备份集
    let (exists, remote_backupset_file) = ssh.exists_backupset(xlsx_checksum, &dbps_home);
    if exists {
        // 备份文件已存在
        log(s, &dbps_home, &format!("BackupSet: {} exists", remote_backupset_file));

    } else {
        let manifest = config::get_jddm_manifest(input);
        backup_remote_files(xlsx_checksum, manifest, &dbps_home, &ssh, &s, false);
    }

    // 写入检查点
    file::write_backup_checkpoint(&dbps_home, s, config::ROLE_JDDM, xlsx_checksum);

}


async fn start_ds_worker(ssh: &ssh::Client, c: &db::Client, s: &Server, xlsx_checksum: &str){

    let input = match &s.src_type {
        Some(s) => s,
        None => return
    };

    if let Some(ckp) = file::read_backup_checkpoint(s, config::ROLE_DS, xlsx_checksum) {
        // 查询到检查点
        log(s, &ckp.split(":").nth(0).unwrap(), &format!("Backed up on {}", get_last_datetime(&ckp)));
        if !config::is_force() {
            return;
        }
        log(s, &ckp.split(":").nth(0).unwrap(), "Covering backed up");
    }

    let dbps_home = match ssh.ds_dbps_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 判断远端是否有备份集
    let (exists, remote_backupset_file) = ssh.exists_backupset(xlsx_checksum, &dbps_home);
    if exists {
        // 备份文件已存在
        log(s, &dbps_home, &format!("BackupSet: {} exists", remote_backupset_file));
        file::write_backup_checkpoint(&dbps_home, s, config::ROLE_DS, xlsx_checksum);
        return;
    }

    // 从数据库中查询位点信息
    let (_, yrba_dat) = query_log_position(s, c.clone()).await;

    // 将位点信息写入备份目录中：$DBPS_HOME/bin/monica.yrba.dat
    match ssh.write_log_pos(&dbps_home, &yrba_dat) {
        Ok(log_pos_written) => match config::get_ds_manifest(input, ssh.get_ss_version(&input, &dbps_home)) {
            Some(manifest) => {
                if backup_remote_files(xlsx_checksum, manifest, &dbps_home, &ssh, &s, log_pos_written) {
                    // 写入检查点
                    file::write_backup_checkpoint(&dbps_home, s, config::ROLE_DS, xlsx_checksum);
                }
            },
            None => error(s, &dbps_home, "Oracle version read failed <<<")
        },
        Err(e) => config::abnormal_exit_backup(&e)
    }

}


// 备份文件，计算sha256sum
// 备份远端程序，备份时先生成临时文件 .monica/.tmp/<sha256sum>.tar，当文件上传成功后，将备份文件挪出目录.monica中，并写入backupset.index
fn backup_remote_files(xlsx_checksum: &str, manifest: &Manifest, 
    dbps_home: &str, ssh: &ssh::Client, s: &Server, log_pos_written: bool) -> bool {

    let mut file_list = manifest.file.join(" ");
    if log_pos_written {
        file_list = format!("{} bin/{}", file_list, config::get_yrba_file_name());
    }
    log(s, dbps_home, &format!("Generated remote bin/{}", config::get_yrba_file_name()));

    // 生成 sha256sum.txt 文件
    // sha256sum <file_list>
    match ssh.exec_gen_sha256sum_file(dbps_home, &file_list) {
        Ok(sha256sum_file_name) => {
            // 执行成功，并将该文件打包到备份文件中
            file_list = format!("{} {}", file_list, sha256sum_file_name);
        },
        Err(e) => config::abnormal_exit_backup(&e)
    }

    let file_name = format!("bin/{}", config::BACKUPUP_SHA256SUM_FILENAME);
    log(s, dbps_home, &format!("Generated remote {}", file_name));
    
    // 备份远端程序，备份时先生成临时文件
    match ssh.gen_tmp_backupset(xlsx_checksum, &dbps_home, &file_list) {

        Ok(backupset_file_name) => {
            log(s, dbps_home, &format!("Generated remote temporary BackupSet {}", backupset_file_name));

            match Opt::from_args().command {
                // Command::Patch(_) => {
                //     // 先上传文件再移动为正式文件
                //     return true;
                // },
                Command::Backup(_) => {
                    // 只执行备份命令
                    match ssh.gen_backupset(xlsx_checksum, dbps_home) {
                        Ok(backupset_file_name) => {
                            log(s, dbps_home, &format!("Generated BackupSet {}, BackupSet record in {}", backupset_file_name, config::BACKUPUP_INDEX_FILENAME));
                            return true;
                        },
                        Err(e) => config::abnormal_exit_backup(&e)
                        
                    }
                },
                _ => { },
            }
        },

        Err(e) => config::abnormal_exit_backup(&e)
    }

    false

}
