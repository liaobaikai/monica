use std::{path::Path, sync::{Arc, Mutex}};

use log::info;
use tokio::runtime;
use crate::{cmd, config::{self, Manifest, Server}, db, file, ssh};


// 升级事件处理
pub async fn handle_command_xpatch(worker_threads: usize) {

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
            start_xpatch_worker(&checksum, &_dbc, counter, server).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    rt.shutdown_background();

    // 写入本地清单文件
    file::write_local_inventory(&xlsx_checksum);

    info!("Patch applied. Great!");
    println!("");

}

// 启动任务
async fn start_xpatch_worker(xlsx_checksum: &str, c: &db::Client, c0: Arc<Mutex<usize>>, s: &Server) {
    // 连接到复制机，需考虑异机部署
    let ssh = ssh::Client::new("", s);
    // 打印进度
    cmd::print_counter(c0);

    start_ds_worker(&ssh, c, s, xlsx_checksum).await;
    start_dt_worker(&ssh, c, s, xlsx_checksum).await;
    start_jddm_worker(&ssh, c, s, xlsx_checksum).await;

    info!("xlsx:Line: {:<2} Host: {}, Service: {}, Patch completed", &s.rid, &s.hostname, &s.service_name);
}


async fn start_dt_worker(ssh: &ssh::Client, c: &db::Client, s: &Server, xlsx_checksum: &str){

    let input = match &s.dst_type {
        Some(s) => s,
        None => return
    };

    if let Some(ckp) = file::read_checkpoint(s, config::ROLE_DT, xlsx_checksum) {
        // 查询到检查点
        cmd::log(s, &ckp.split(":").nth(0).unwrap(), &format!("Patch applied on {}", cmd::get_last_datetime(&ckp)));
        if !config::is_force() {
            return;
        }
        cmd::log(s, &ckp.split(":").nth(0).unwrap(), "Covering applied patch");
    }

    let dbps_home = match ssh.dt_dbps_home(s) {
        Some(s) => s,
        None => {
            cmd::error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 从远端文件中获取位点信息
    let valid_log_pos;
    if cmd::current_log_position() {
        (valid_log_pos, _) = cmd::query_log_position(s, c.clone()).await;
    } else {
        (valid_log_pos, _) = cmd::read_log_position(&ssh, &dbps_home, s);
    }

    // 停止程序
    let (starting, starting2) = ssh.kill_ps(&format!("{}/bin/", dbps_home));
    cmd::log(s, &dbps_home, &format!("B-Start:{}, A-Start:{}", starting, starting2));

    match config::get_dt_manifest(input, ssh.get_ss_version(&input, &dbps_home)) {
        Some(manifest) => patch_remote_files(config::ROLE_DT, manifest, &dbps_home, &ssh, &s, xlsx_checksum),
        None => cmd::error(s, &dbps_home, "Oracle version read failure <<<")
    }

    // 写入检查点文件
    file::write_checkpoint(&dbps_home, s, config::ROLE_DT, xlsx_checksum);

    // 文件上传完成后重置任务
    cmd::clean_dt(s, &dbps_home, &ssh);

    if !valid_log_pos {
        cmd::log(s, &dbps_home, "YRBA(log position) is empty, Skip start");
        return;
    }

    if starting {
        cmd::startup(s, &dbps_home, &ssh);
    } else {
        cmd::log(s, &dbps_home, "Non-Start, Skip start");
    }

}

async fn start_jddm_worker(ssh: &ssh::Client, c: &db::Client, s: &Server, xlsx_checksum: &str){

    let input = match &s.dst_type {
        Some(s) => s,
        None => return
    };

    // kafka类型
    if !input.starts_with(config::KFK_TYPE) {
        return ;
    }

    if let Some(ckp) = file::read_checkpoint(s, config::ROLE_JDDM, xlsx_checksum) {
        // 查询到检查点
        cmd::log(s, &ckp.split(":").nth(0).unwrap(), &format!("Patch applied on {}", cmd::get_last_datetime(&ckp)));
        if !config::is_force() {
            return;
        }
        cmd::log(s, &ckp.split(":").nth(0).unwrap(), "Covering applied patch");
    }

    let dbps_home = match ssh.jddm_home(s) {
        Some(s) => s,
        None => {
            cmd::error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 从远端文件中获取位点信息
    let valid_log_pos;
    if cmd::current_log_position() {
        (valid_log_pos, _) = cmd::query_log_position(s, c.clone()).await;
    } else {
        (valid_log_pos, _) = cmd::read_log_position(&ssh, &dbps_home, s);
    }

    // 停止程序
    let (starting, starting2) = ssh.kill_ps(&format!("DPath={} ", dbps_home));
    cmd::log(s, &dbps_home, &format!("B-Start: {}, A-Start: {}", starting, starting2));

    let manifest = config::get_jddm_manifest(input);
    patch_remote_files(config::ROLE_JDDM, manifest, &dbps_home, &ssh, &s, xlsx_checksum);

    // 写入检查点文件
    file::write_checkpoint(&dbps_home, s, config::ROLE_JDDM, xlsx_checksum);

    // 文件上传完成后重置任务
    cmd::clean_jddm(s, &dbps_home, &ssh);

    if !valid_log_pos {
        cmd::log(s, &dbps_home, "YRBA(log position) is empty, Skip start");
        return;        
    }

    if starting {
        cmd::startup_jddm(s, &dbps_home, &ssh);
    } else {
        cmd::log(s, &dbps_home, "Non-Start, Skip start");
    }


}

async fn start_ds_worker(ssh: &ssh::Client, c: &db::Client, s: &Server, xlsx_checksum: &str){

    let input = match &s.src_type {
        Some(s) => s,
        None => return
    };
    
    // if let Some(input) = &s.src_type {
    if let Some(ckp) = file::read_checkpoint(s, config::ROLE_DS, xlsx_checksum) {
        // 查询到检查点
        cmd::log(s, &ckp.split(":").nth(0).unwrap(), &format!("Patch applied on {}", cmd::get_last_datetime(&ckp)));
        if !config::is_force() {
            return;
        }
        cmd::log(s, &ckp.split(":").nth(0).unwrap(), "Covering applied patch");
    }

    let dbps_home = match ssh.ds_dbps_home(s) {
        Some(s) => s,
        None => {
            cmd::error(s, "<NONE>", "No such directory <<<");
            return;
        }
    };

    // 从远端文件中获取位点信息
    let valid_log_pos;
    let yrba_dat;
    if cmd::current_log_position() {
        (valid_log_pos, yrba_dat) = cmd::query_log_position(s, c.clone()).await;
    } else {
        (valid_log_pos, yrba_dat) = cmd::read_log_position(&ssh, &dbps_home, s);
    }
        
    // 停止程序
    let (starting, starting2) = ssh.kill_ps(&format!("{}/bin/", dbps_home));
    cmd::log(s, &dbps_home, &format!("B-Start:{}, A-Start:{}", starting, starting2));

    match config::get_ds_manifest(input, ssh.get_ss_version(&input, &dbps_home)) {
        Some(manifest) => patch_remote_files(config::ROLE_DS, manifest, &dbps_home, &ssh, &s, xlsx_checksum),
        None => cmd::error(s, &dbps_home, "Oracle version read failure <<<")
    }

    // 写入检查点文件
    file::write_checkpoint(&dbps_home, s, config::ROLE_DS, xlsx_checksum);

    // 文件上传完成后重置任务
    cmd::clean_ds(s, &dbps_home, &ssh);

    if !valid_log_pos {
        cmd::log(s, &dbps_home, "YRBA(log position) is empty, Skip start");
        return;
    }

    // 写入yrba文件
    cmd::update_yrba_file(s, &dbps_home, &yrba_dat, &ssh);

    if starting {
        cmd::startup(s, &dbps_home, &ssh);
    } else {
        cmd::log(s, &dbps_home, "Non-Start, Skip start");
    }
        
}


// 升级文件：上传文件
// 本地生成sha256sum.txt文件
fn patch_remote_files(role: usize, manifest: &Manifest, dbps_home: &str, ssh: &ssh::Client, s: &Server, xlsx_checksum: &str){

    // 里面记录了文件上传的断点信息
    ssh.remove_sha256sum_file(dbps_home);

    // 上传文件
    let counter = manifest.file.len();
    for (index, f) in manifest.file.iter().enumerate() {
        let local_file = Path::new(&config::get_basedir()).join(&manifest.dir).join(f);
        let s_local_file = local_file.to_string_lossy().to_string();
        let remote_file = Path::new(&file::path_join(dbps_home, f)).to_path_buf();
        let local_file_path = local_file.to_string_lossy().to_string();
        let current = index+1;

        // 判断文件是否已经上传
        if let Some(_) = file::file_checkpoint(s, role, xlsx_checksum, &s_local_file) {
            // 文件已上传
            cmd::log(s, dbps_home, &format!("Upload [{}/{}] \"{}\" completed (disk cache)", current, counter, local_file_path));
            continue;
        }

        cmd::log(s, dbps_home, &format!("Upload [{}/{}] \"{}\"", current, counter, local_file_path));
        if ssh.scp_send(local_file, remote_file, current, counter) {
            cmd::log(s, dbps_home, &format!("Upload [{}/{}] \"{}\" completed", current, counter, local_file_path));
        }

        // 写入断点文件
        file::write_file_checkpoint(s, role, xlsx_checksum, &s_local_file);

    }
    // 所有文件上传后，开始对比文件的sha256sum.txt文件
    // 如：sha256sum.txt
    // f7dac4ade9ab40000593bbc7fde9f12f7350d6447e1f275d240333313a178570 bin/aaaa.monica   
    // xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx bin/bbbb.monica   
    // 校验通过后，可以将备份文件挪出到.monica目录中并写入 backupset.index 文件
    if ssh.verify_sha256sum_file(dbps_home)  {
        cmd::log(s, dbps_home, "Files sha256sum verify passed");

        // 检查文件是否存在
        let (exists, backupset_file_name) = ssh.exists_backupset(xlsx_checksum, dbps_home);
        if exists {
            // 已经存在了，则不用再次mv，且不用写入备份的检查文件
            cmd::log(s, dbps_home, &format!("Generated backupset {}", backupset_file_name));
            return;
        }

        // 上传成功
        match ssh.gen_backupset(xlsx_checksum, dbps_home) {
            Ok(backupset_file_name) => {
                cmd::log(s, dbps_home, &format!("Generated backupset {}", backupset_file_name));
            },
            Err(e) => config::abnormal_exit_patch(&e)
        }
    } else {
        cmd::log(s, dbps_home, "Files sha256sum verify failed");
    }

    // ssh.remove_sha256sum_file(dbps_home);

}
