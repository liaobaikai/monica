
use std::{fs::File, net::TcpStream, path::{Path, PathBuf}, process::exit, time::{Duration, Instant}};

use chrono::Local;
use log::{debug, info, error};
use ssh2::Session;
use std::io::{prelude::*, BufReader};

use crate::config::{self, Server};

#[derive(Clone)]
pub struct Client {
    sess: Session,
    host: String,
    flag: String
}

impl Client {

    pub fn new(flag: &str, s: &Server) -> Self {
        // 新建连接
        let tcp = TcpStream::connect(format!("{}:{}", s.hostname, s.port)).unwrap();
        let mut sess = Session::new().unwrap();
        sess.set_tcp_stream(tcp);
        match sess.handshake() {
            Ok(()) => {},
            Err(e) => {
                error!("[{}]:: Host: {}:{}, Session create fail.", flag, s.hostname, s.port);
                abnormal_exit(e);
            }
        }
        let pwd = &s.password.clone().unwrap();
        match sess.userauth_password(&s.username, pwd) {
            Ok(()) => {},
            Err(e) => {
                error!("[{}]:: Host: {}:{}, Session create fail.", flag, s.hostname, s.port);
                abnormal_exit(e);
            }
        }
        info!("[{}]:: Connected to server {}:{}.", flag, s.hostname, s.port);

        Client{ sess: sess, host: s.hostname.clone(), flag: flag.to_string()}
    }

    // pub fn set_flag(&mut self, flag: String){
    //     self.flag = flag;
    // }

    pub fn exec_cmd(&self, command: &str) -> String {

        debug!("[{}]:: Host: {}, Exec_ssh_cmd: `{}`", self.flag, self.host, command);
        let mut channel = self.sess.channel_session().unwrap();
        channel.exec(command).unwrap();
        let mut s = String::new();
        channel.read_to_string(&mut s).unwrap();
        channel.wait_close().unwrap();

        s
    }

    // 远端生成备份文件
    pub fn exec_gen_backupset(&self, base: &str, backup_file_list: &str) -> bool {

        let suffix = Local::now().format("%Y%m%d_%H%M").to_string();
        let backupset_file_name = format!("monica.backupset-{}.tar", suffix);
        let backup_dir = "monica.backupset";

        let mut cmd = format!("cd {} && ", base);
        cmd = format!("{} mkdir -p {} && ", cmd, backup_dir);
        cmd = format!("{} tar -cf {}/{} {} && ", cmd, backup_dir, backupset_file_name, backup_file_list);
        cmd = format!("{} echo \"{}/{}\" >> {}/{} && ", cmd, backup_dir, backupset_file_name, backup_dir, config::BACKUPUP_INDEX_FILENAME);
        cmd = format!("{} tail -1 {}/{} ", cmd, backup_dir, config::BACKUPUP_INDEX_FILENAME);
        let stdout = self.exec_cmd(&cmd);

        stdout.trim_end_matches("\n") == backupset_file_name
    }

    // 检查进程是否存在
    pub fn check_valid_ps(&self, dir_prefix: &str) -> bool {
        let stdout = self.exec_cmd(&format!("ps -ef --cols 10240 | grep \"{}\" | egrep -v 'grep|pmon' | wc -l", dir_prefix));
        stdout != "0\n"
    }

    // 获取正在运行的jddm参数
    pub fn get_jddm_start_with(&self, dir_prefix: &str) -> String {
        let stdout = self.exec_cmd(&format!("ps -ef --cols 10240 | grep \"DPath={} \" | grep -v grep | awk '{{print $NF}}'", dir_prefix));
        stdout.trim_end_matches("\n").to_string()
    }

    // 检查进程是否存在
    pub fn kill_ps(&self, dir_prefix: &str) -> (bool, bool) {
        let starting = self.check_valid_ps(dir_prefix);
        self.exec_cmd(&format!("ps -ef --cols 10240 | grep \"{}\" | egrep -v 'grep|pmon' | awk '{{print $2}}' | xargs kill -9", dir_prefix));
        let starting2 = self.check_valid_ps(dir_prefix);
        (starting, starting2)
    }

    pub fn dbps_home(&self, s: &Server, dir_prefix: &str) -> Option<String> {

        // /data/dataxone/sync/<service_name>
        let base = Path::new(&s.service_base_path).join(&s.service_name);

        // ds_<service_name>
        let dir = self.exec_cmd(&format!("ls {} | egrep \"^({}){}\"", base.display(), dir_prefix, &s.service_name));
        let dir1 = dir.trim_end_matches("\n");
        debug!("[{}]:: Host: {}, Exec_ssh_cmd: {}", self.flag, self.host, dir1);
        if dir1 == "" {
            None
        } else {
            let dbps_home = base.join(dir1);
            Some(dbps_home.to_string_lossy().to_string())
        }
       
    }

    pub fn ds_dbps_home(&self, s: &Server) -> Option<String> {
        let s = self.dbps_home(s, &"ds_");
        s
    }

    pub fn dt_dbps_home(&self, s: &Server) -> Option<String> {
        let s = self.dbps_home(s, &"dy_");
        s
    }

    pub fn jddm_home(&self, s: &Server) -> Option<String> {
        let s = self.dbps_home(s, &"dt_");
        s
    }

    // oracle的版本获取: 
    // example: 19.3.0.0.0.Linux.x86_64
    pub fn get_vagentd_version(&self, dbps_home: &str) -> String {
        let stdout = self.exec_cmd(&format!("{}/bin/vagentd -v | grep 'for oracle version' | awk '{{print $6\".\"$NF}}'", dbps_home));
        stdout.trim_end_matches("\n").to_string()
    }

    // 向远程服务器发送文件
    pub fn scp_send(&self, file: PathBuf, rfile: PathBuf) {

        info!("[{}]:: Upload    {:?}", self.flag, file.display());
        info!("[{}]:: Uploading {:?} to {}", self.flag, file.file_name().unwrap(), rfile.display());
        // 获取本地文件的基础信息
        let local_file = File::open(&file).unwrap();
        let local_file_metadata = local_file.metadata().unwrap();
        let local_file_size = local_file_metadata.len();

        // Write the file
        // called `Result::unwrap()` on an `Err` value: Error { code: Session(-28), msg: "failed to send file" }
        let mut remote_file;
        loop {
            match self.sess.scp_send(&rfile, 0o755, local_file_size, None) {
                Ok(c) => {
                    remote_file = c;
                    break;
                },
                Err(e) => {
                    error!("[{}]:: Upload error: {}, retry again, sleep 3s.", self.flag, e);
                    std::thread::sleep(Duration::from_secs(3));
                }
            };
        }

        let mut send_byte_length: u64 = 0;
        let mut send_byte_length_secs = 0;
        let mut reader = BufReader::with_capacity(16384, local_file);
        let start = Instant::now();
        let mut secs = 0;
        // 文件大小
        let file_kb = local_file_size  as f64 / 1024.0;

        loop {
            let buffer = reader.fill_buf().unwrap();
            let buffer_length = buffer.len();
            if buffer_length == 0 {
                break;
            }

            remote_file.write(buffer).unwrap();
            // 冲缓冲区中消耗所有字节
            reader.consume(buffer_length);

            send_byte_length += buffer_length as u64;
            send_byte_length_secs += buffer_length as u64;

            // 耗时计算
            let duration = start.elapsed();
            if duration.as_secs() >= secs && secs > 0 {
                let p = send_byte_length as f64 / local_file_size as f64 * 100.0;
                let send_kb = send_byte_length  as f64/ 1024.0;
                let speed = send_byte_length_secs as f64/ 1024.0;
                // 剩余传输秒数=剩余文件大小/速度
                let eta_secs = (file_kb - send_kb) / speed;
                // let d = Duration::seconds(eta_secs as i64);
                // Local.from_utc_datetime(Date);

                info!("[{}]:: Uploading {:>3.0}%{:10.0}KiB / {:.0}KiB {:8.0}KiB/s {} ETA", self.flag, p, send_kb, file_kb, speed, eta_format(eta_secs as u64));
                send_byte_length_secs = 0;
                secs += 1;
            }
        }

        // Close the channel and wait for the whole content to be transferred
        remote_file.send_eof().unwrap();
        remote_file.wait_eof().unwrap();
        remote_file.close().unwrap();
        remote_file.wait_close().unwrap();

        let p = send_byte_length as f64 / local_file_size as f64 * 100.0;
        let send_kb = send_byte_length  as f64/ 1024.0;
        let speed = send_byte_length_secs as f64/ 1024.0;
        info!("[{}]:: Uploading {:>3.0}%{:10.0}KiB / {:.0}KiB {:8.0}KiB/s OK.", self.flag, p, send_kb, file_kb, speed);

    }


    
}


fn eta_format(secs: u64) -> String {
    let remaining_seconds = secs % 60;
    let minutes = (secs % 3600) / 60;
    let hours = secs / 3600;
    let s = format!("{:02.0}:{:02.0}:{:02.0}", hours, minutes, remaining_seconds);
    s
}



fn abnormal_exit(e: ssh2::Error){
    println!("PreChecks failure:::");
    println!("  CAUSE: {}", e);
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}