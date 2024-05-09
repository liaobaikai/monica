use std::{path::Path, sync::{Arc, Mutex}};

use log::info;
use tokio::runtime;

use crate::{cmd::print_counter, config::{self, get_basedir, Manifest, Server, KFK_TYPE}, file::{self, path_join}, ssh};

use super::{error, log};


// 预检查事件处理
pub async fn handle_command_precheck(worker_threads: usize){

    let size = config::GLOBAL_CONFIG.servers.len();

    if size == 0 {
        println!("\nWarning: The xlsx file has no valid lines\n");
        return ;
    }

    // 创建线程池
    // https://rust-book.junmajinlong.com/ch100/01_understand_tokio_runtime.html
    let rt = runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_io()
            .enable_time()
            .thread_name("monica")
            .build()
            .unwrap();

    let counter = Arc::new(Mutex::new(size));

    // 预检查
    let mut handles = vec![];
    for server in config::GLOBAL_CONFIG.servers.iter() {
        let counter: Arc<Mutex<usize>> = Arc::clone(&counter);
        let handle: tokio::task::JoinHandle<()> = rt.spawn(async move {
            start_precheck_worker(counter, server).await;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    rt.shutdown_background();

    info!("PreChecks passed. Great!");
    println!("");

}

// 预检查
pub async fn start_precheck_worker(c: Arc<Mutex<usize>>, s: &Server){

    print_counter(c);

    // 连接到复制机，需考虑异机部署
    let ssh = ssh::Client::new("", s);
    start_ds_worker(&ssh, s);
    start_dt_worker(&ssh, s);
    start_jddm_worker(&ssh, s);

    info!("xlsx:Line: {:<2} Host: {}, Service: {}, PreChecks passed", &s.rid, &s.hostname, &s.service_name);
    
}


fn start_dt_worker(ssh: &ssh::Client, s: &Server){

    let input = match &s.dst_type {
        Some(s) => s,
        None => return
    };

    let dbps_home = match ssh.dt_dbps_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            abnormal_exit_not_found();
            return;
        }
    };

    log(s, &dbps_home, "Found");
    match config::get_dt_manifest(input, ssh.get_ss_version(&input, &dbps_home)) {
        Some(manifest) => do_precheck_files(s, &dbps_home, manifest, &ssh),
        None => {
            error(s, &dbps_home, "Oracle version read failure");
            config::abnormal_exit_precheck("Oracle version read failure");
        }
    }
        
}


fn start_jddm_worker(ssh: &ssh::Client, s: &Server){

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
            abnormal_exit_not_found();
            return;
        }
    };

    log(s, &dbps_home, "Found");
    let manifest = config::get_jddm_manifest(input);
    do_precheck_files(s, &dbps_home, manifest, &ssh);
        
}



fn start_ds_worker(ssh: &ssh::Client, s: &Server){

    let input = match &s.src_type {
        Some(s) => s,
        None => return
    };

    let dbps_home = match ssh.ds_dbps_home(s) {
        Some(s) => s,
        None => {
            error(s, "<NONE>", "No such directory <<<");
            abnormal_exit_not_found();
            return;
        }
    };

    log(s, &dbps_home, "Found");
    match config::get_ds_manifest(input, ssh.get_ss_version(&input, &dbps_home)) {
        Some(manifest) => do_precheck_files(s, &dbps_home, manifest, &ssh),
        None => {
            error(s, &dbps_home, "Oracle version read failure");
            config::abnormal_exit_precheck("Oracle version read failure");
        }
    }
        
}


// 预检查：本地文件检查 和 远程文件
fn do_precheck_files(s: &Server, dbps_home: &str, manifest: &Manifest, ssh: &ssh::Client){

    // 解压本地文件 
    file::extract_compressed_files(&get_basedir(), &manifest.package, &manifest.dir, s.rid);
    for f in manifest.file.iter() {
        let local_file = Path::new(&get_basedir()).join(&manifest.dir).join(f);
        if !local_file.exists() {
            error!("xlsx:Line: {:<2} File {}, No Found <<<", &s.rid, local_file.display());
            abnormal_exit_not_found();
        }
        info!("xlsx:Line: {:<2} File {}, Found", &s.rid, local_file.display());

        let remote_file = &path_join(dbps_home, f);
        if !ssh.is_file(remote_file) {
            error!("xlsx:Line: {:<2} Remote File {}, No Found <<<", &s.rid, remote_file);
            abnormal_exit_not_found();
        }
        info!("xlsx:Line: {:<2} Remote File {}, Found", &s.rid, remote_file);
    }
    
}

// 
fn abnormal_exit_not_found(){
    config::abnormal_exit_precheck("No such file or directory");
}