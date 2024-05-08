
use std::{fs::File, net::TcpStream, path::{Path, PathBuf}, process::exit, time::Duration};

use chrono::Local;
use log::{debug, info, error};
use ssh2::Session;
use std::io::prelude::*;

use crate::{config::{self, get_chunk_size, get_yrba_file_name, Server, BACKUPUP_DIR, BACKUPUP_FILE_PREFIX, BACKUPUP_RECYCLE_BIN_DIR, BACKUPUP_SHA256SUM_FILENAME, BACKUPUP_TMP_DIR}, file::{self, get_filesize, path_join}};

#[derive(Clone)]
pub struct Client {
    sess: Session,
    host: String,
    rid: usize
}

impl Client {

    pub fn new(flag: &str, s: &Server) -> Self {
        // 新建连接
        let tcp = match TcpStream::connect(format!("{}:{}", s.hostname, s.port)) {
            Ok(tcp) => tcp,
            Err(e) => {
                // 无法链接到对应的端口
                error!("xlsx:Line: {:<2} Host: {}:{}, Session create failure, Cause: {}", s.rid, s.hostname, s.port, e);
                exit(-1);
            }
        };
        let mut sess = Session::new().unwrap();
        sess.set_tcp_stream(tcp);
        match sess.handshake() {
            Ok(()) => {},
            Err(e) => {
                error!("xlsx:Line: {:<2} Host: {}:{}, Session create failure", s.rid, s.hostname, s.port);
                abnormal_exit(flag, e);
            }
        }
        let pwd = &s.password.clone().unwrap();
        match sess.userauth_password(&s.username, pwd) {
            Ok(()) => {},
            Err(e) => {
                error!("xlsx:Line: {:<2} Host: {}:{}, Session create failure", s.rid, s.hostname, s.port);
                abnormal_exit(flag, e);
            }
        }
        info!("xlsx:Line: {:<2} Connected to server {}:{}", s.rid, s.hostname, s.port);

        Client{ sess: sess, host: s.hostname.clone(), rid: s.rid}
    }

    // pub fn set_flag(&mut self, flag: String){
    //     self.flag = flag;
    // }

    pub fn exec_cmd_with_status(&self, command: &str) -> (i32, String, String) {

        debug!("xlsx:Line: {:<2} Host: {}, Exec_ssh_cmd: `{}`", self.rid, self.host, command);
        let mut channel = self.sess.channel_session().unwrap();
        channel.exec(command).unwrap();
        let mut stdout = String::new();
        channel.read_to_string(&mut stdout).unwrap();
        channel.wait_close().unwrap();
        
        let mut stderr = String::new();
        channel.stderr().read_to_string(&mut stderr).unwrap();
        
        let status = channel.exit_status().unwrap();

        debug!("xlsx:Line: {:<2} Host: {}, Exec_ssh_cmd: status={}, stdout={}, stderr={}", self.rid, self.host, status, stdout.replace("\n", "\\n"), stderr.replace("\n", "\\n"));
        (status, stdout, stderr)
    }

    pub fn exec_cmd(&self, command: &str) -> String {
        let (_, stdout, _) = self.exec_cmd_with_status(command);
        stdout
    }

    // 远端生成备份文件
    // 备份时先生成临时文件 .monica/.tmp/<sha256sum>.tar，
    pub fn gen_tmp_backupset(&self, xlsx_checksum: &str, base: &str, backup_file_list: &str) -> Result<String, String> {
        let backupset_file_name = format!("{}-{}.tar", BACKUPUP_FILE_PREFIX, xlsx_checksum);

        let mut cmd = format!("cd {} && ", base);
        cmd = format!("{} mkdir -p {} && ", cmd, BACKUPUP_TMP_DIR);
        cmd = format!("{} tar -cf {}/{} {}", cmd, BACKUPUP_TMP_DIR, backupset_file_name, backup_file_list);
        let (status, _, stderr) = self.exec_cmd_with_status(&cmd);

        if status > 0 {
            Err(format!("Temporary backupset generate failed, cause: {}", stderr))
        } else {
            Ok(backupset_file_name)
        }
    }

    // 移动临时备份集成为正式备份集
    // 生成备份集
    // 将备份文件挪出目录.monica中，并写入backupset.index
    pub fn gen_backupset(&self, xlsx_checksum: &str, base: &str) -> Result<String, String> {

        let backupset_file_name = format!("{}-{}.tar", BACKUPUP_FILE_PREFIX, xlsx_checksum);
        let index_file = get_index_file();

        let mut cmd = format!("cd {} && ", base);
        // mv .monica/.tmp/backupset-f7dac4ade9ab40000593bbc7fde9f12f7350d6447e1f275d240333313a178570.tar .monica
        cmd = format!("{} mv {}/{} {} && ", cmd, BACKUPUP_TMP_DIR, backupset_file_name, BACKUPUP_DIR);
        // echo .monica/backupset-f7dac4ade9ab40000593bbc7fde9f12f7350d6447e1f275d240333313a178570.tar .monica
        cmd = format!("{} echo \"{}/{}\" >> {} && ", cmd, BACKUPUP_DIR, backupset_file_name, index_file);
        cmd = format!("{} tail -1 {} ", cmd, index_file);
        let (_, stdout, stderr) = self.exec_cmd_with_status(&cmd);

        let fin_backupset_file_path = format!("{}/{}", BACKUPUP_DIR, backupset_file_name);

        if stdout.trim_end_matches("\n") == fin_backupset_file_path {
            Ok(fin_backupset_file_path)
        } else {
            Err(format!("Backupset generate failed, cause: {}", stderr))
        }
    }

    // 检查备份集是否存在，如果存在，则跳过备份
    pub fn exists_backupset(&self, xlsx_checksum: &str, base: &str) -> (bool, String) {
        let backupset_file_name = format!("{}/{}-{}.tar", BACKUPUP_DIR, BACKUPUP_FILE_PREFIX, xlsx_checksum);
        let cmd = format!("test -e {}/{}", base, backupset_file_name);
        let (status, _, _) = self.exec_cmd_with_status(&cmd);
        (status == 0, backupset_file_name)
    }

    // 回退到指定的checksum
    pub fn exec_rollback_backupset(&self, base: &str, xlsx_checksum: &str) -> bool {

        let index_file = get_index_file();
        let recyclebin_index_file = format!("{}/{}", BACKUPUP_RECYCLE_BIN_DIR, config::BACKUPUP_INDEX_FILENAME);

        let backupset_file = format!("{}/{}-{}.tar", BACKUPUP_DIR, BACKUPUP_FILE_PREFIX, xlsx_checksum);

        let mut cmd = format!("cd {} && ", base);
        cmd = format!("{} mkdir -p {} && ", cmd, BACKUPUP_RECYCLE_BIN_DIR);
        cmd = format!("{} export file_name=\"{}\" && ", cmd, backupset_file);
        cmd = format!("{} export LANG=en_US.utf8 && ", cmd);
        cmd = format!("{} echo \"$file_name\" >> {} && ", cmd, recyclebin_index_file);
        cmd = format!("{} tar -xf $file_name && sed -i '/{}/d' {} && ", cmd, backupset_file.replace("/", "\\/"), index_file);
        cmd = format!("{} mv $file_name {} && ", cmd, BACKUPUP_RECYCLE_BIN_DIR);
        cmd = format!("{} export file_count=$(cat bin/{} | wc -l) && ", cmd, BACKUPUP_SHA256SUM_FILENAME);
        cmd = format!("{} sha256sum -c bin/{} | grep ': OK' | wc -l | awk -v c=$file_count '{{print $0==c}}'", cmd, BACKUPUP_SHA256SUM_FILENAME);
        let stdout = self.exec_cmd(&cmd);
        // 返回0，则代表有文件的sha256sum不一致；返回1，全部sha256sum通过
        stdout.trim_end_matches("\n") == "1"
    }

    // 列出远端备份集
    // cat $DBPS_HOME/monica.backupset/monica.backupset.index 
    pub fn list_remote_backupset(&self, dbps_home: &str) -> Vec<String> {
        let index_file = get_index_file();

        let stdout = self.exec_cmd(&format!("export DBPS_HOME={} && cat $DBPS_HOME/{} 2>/dev/null", dbps_home, index_file));
        let mut lines: Vec<String> = Vec::new();
        for line in stdout.trim_end_matches("\n").lines(){
            lines.push(String::from(line));
        }
        lines
    }

    // 生成sha256sum文件
    pub fn exec_gen_sha256sum_file(&self, dbps_home: &str, file_list: &str) -> Result<String, String> {
        let file_name = format!("bin/{}", BACKUPUP_SHA256SUM_FILENAME);
        let (_, _, stderr) = self.exec_cmd_with_status(&format!("export DBPS_HOME={} && cd $DBPS_HOME && sha256sum {} > {}", 
                dbps_home, file_list, file_name));

        if !stderr.is_empty() {
            Err(format!("SHA-256sum file generate failed, cause: {}", stderr))
        } else {
            Ok(file_name)
        }
    }

    // 检查进程是否存在
    pub fn check_valid_ps(&self, dir_prefix: &str) -> bool {
        let stdout = self.exec_cmd(&format!("ps -ef --cols 10240 | grep \"{}\" | egrep -v 'grep' | wc -l", dir_prefix));
        stdout != "0\n"
    }

    // 获取正在运行的jddm参数
    // pub fn read_jddm_start_with(&self, dir_prefix: &str) -> String {
    //     let stdout = self.exec_cmd(&format!("ps -ef --cols 10240 | grep \"DPath={} \" | grep -v grep | awk '{{print $NF}}'", dir_prefix));
    //     stdout.trim_end_matches("\n").to_string()
    // }

    // 获取正在运行的jddm参数
    pub fn write_jddm_start_with(&self, dir_prefix: &str) -> bool {
        let (status,_,_) = self.exec_cmd_with_status(&format!("ps -ef --cols 10240 | grep \"DPath={} \" | grep -v grep | awk '{{print $NF}}' > {}/bin/monica.started", dir_prefix, dir_prefix));
        status == 0
    }

    // 检查进程是否存在
    pub fn kill_ps(&self, dir_prefix: &str) -> (bool, bool) {
        let starting = self.check_valid_ps(dir_prefix);
        if !starting {
            return (starting, false);
        }
        self.exec_cmd(&format!("ps -ef --cols 10240 | grep \"{}\" | egrep -v 'grep' | awk '{{print $2}}' | xargs kill -9", dir_prefix));
        let starting2 = self.check_valid_ps(dir_prefix);
        (starting, starting2)
    }

    pub fn dbps_home(&self, s: &Server, dir_prefix: &str) -> Option<String> {

        // /data/dataxone/sync/<service_name>
        let base = path_join(&s.service_base_path, &s.service_name);

        // ds_<service_name>
        let stdout = self.exec_cmd(&format!("ls {} | egrep \"^({}){}\"", base, dir_prefix, &s.service_name));
        let dir = stdout.trim_end_matches("\n");
        debug!("xlsx:Line: {:<2} Host: {}, Exec_ssh_cmd: {}", self.rid, self.host, dir);
        if dir == "" {
            None
        } else {
            Some(path_join(&base, dir))
        }
       
    }

    pub fn ds_dbps_home(&self, s: &Server) -> Option<String> {
        let s = self.dbps_home(s, &"ds_");
        s
    }

    pub fn dt_dbps_home(&self, s: &Server) -> Option<String> {
        if s.dst_type.is_none() {
            return None;
        }

        // /data/dataxone/sync/<service_name>
        let base = path_join(&s.service_base_path, &s.service_name);

        let cmd = if s.dst_type.clone().unwrap() == "KAFKA" {
            // oracle到kafka: dy_<service_name>
            // polardb到kafka: dt_<service_name>_y
            format!("ls {} | egrep \"^(dy)_{}|dt_{}_y\"", base, &s.service_name, &s.service_name)
        } else {
            // oracle到oracle: dt_<service_name>
            format!("ls {} | egrep \"^(dt)_{}\"", base, &s.service_name)
        };

        // dt_<service_name>
        let stdout = self.exec_cmd(&cmd);
        let dir = stdout.trim_end_matches("\n");
        debug!("xlsx:Line: {:<2} Host: {}, Exec_ssh_cmd: {}", self.rid, self.host, dir);
        if dir == "" {
            return None
        }

        Some(path_join(&base, dir))

    }

    pub fn jddm_home(&self, s: &Server) -> Option<String> {
        let s = self.dbps_home(s, &"dt_");
        s
    }

    pub fn is_file(&self, remote_file: &str) -> bool {
        let (status, _, _) = self.exec_cmd_with_status(&format!("test -e {}", remote_file));
        status == 0
    }

    // 文件重命名
    pub fn move_file(&self, src_file: &str, dst_file: &str) -> bool {
        let (_, _, stderr) = self.exec_cmd_with_status(&format!("mv {} {}", src_file, dst_file));
        if !stderr.is_empty() {
            error!("xlsx:Line: {:<2} Host: {}, Exec_ssh_cmd: {}", self.rid, self.host, stderr);
            false
        } else {
            true
        }
    }

    // oracle的版本获取: 
    // example: 19.3.0.0.0.Linux.x86_64
    pub fn get_ss_version(&self, sd_type: &str, dbps_home: &str) -> Option<String> {
        match sd_type {
            "ORACLE" => {
                let stdout = self.exec_cmd(&format!("{}/bin/xagentd -v | grep 'for oracle version' | awk '{{print $6\".\"$NF}}'", dbps_home));
                let version = stdout.trim_end_matches("\n");
                debug!("xlsx:Line: {:<2} Host: {}, Exec_ssh_cmd: {}", self.rid, self.host, version);
                if version.is_empty() {
                    return None;
                }
                Some(version.to_string())
            }
            _ => {
                None
            }
        }
    }

    // 将位点信息写入到备份文件中
    // 写入 $DBPS_HOME/bin/monica.yrba.dat
    pub fn write_log_pos(&self, dbps_home: &str, yrba: &str) -> Result<bool, String> {
        let (_, _, stderr) = self.exec_cmd_with_status(&format!("export DBPS_HOME={} && echo \"{}\" > $DBPS_HOME/bin/{}", dbps_home, yrba, get_yrba_file_name()));
        
        if !stderr.is_empty() {
            Err(format!("Log position file remove failed, cause: {}", stderr))
        } else {
            Ok(true)
        }
    }

    // 从备份文件中读取位点信息
    // 读取 $DBPS_HOME/bin/monica.yrba.dat
    pub fn get_log_pos(&self, dbps_home: &str) -> Option<String> {
        let (status, stdout, _) = self.exec_cmd_with_status(&format!("export DBPS_HOME={} && cat $DBPS_HOME/bin/{}", dbps_home, get_yrba_file_name()));
        let s = stdout.trim_end_matches("\n");
        if status != 0 {
            return None;
        } 
        
        if s.is_empty() {
            return None;
        }
        Some(s.to_string())
    }


    // 向远程服务器发送文件
    pub fn scp_send(&self, file: PathBuf, rfile: PathBuf, current: usize, counter: usize) -> bool {
        let remote_file = rfile.to_string_lossy().to_string();
        // let local_file = file.to_string_lossy().to_string();
        let local_file_name = file.file_name().unwrap().to_str().unwrap();
        // info!("xlsx:Line: {:<2} Upload \"{}\"", self.rid, local_file);
        info!("xlsx:Line: {:<2} Upload [{}/{}] {} to \"{}\"", self.rid, current, counter, local_file_name, remote_file);

        // 获取本地文件的基础信息
        let file_size = get_filesize(&file);
        // Write the file
        // 文件繁忙：
        // called `Result::unwrap()` on an `Err` value: Error { code: Session(-28), msg: "failed to send file" }

        // 避免程序正在使用/运行，先生成临时文件
        let remote_tmp_file_path: &String = &format!("{}.monica", remote_file);
        // 临时文件
        let remote_tmp_file = Path::new(remote_tmp_file_path);

        let mut ch;
        loop {
            match self.sess.scp_send(&remote_tmp_file, 0o755, file_size, None) {
                Ok(c) => {
                    ch = c;
                    break;
                },
                Err(e) => {
                    error!("xlsx:Line: {:<2} Upload [{}/{}] file {:10} error, cause: {}, retry again, sleep 3s.", self.rid, current, counter, local_file_name, e);
                    std::thread::sleep(Duration::from_secs(3));
                }
            };
        }

        let mut f;
        loop {
            match File::open(&file) {
                Ok(file) => {
                    f = file;
                    break;
                },
                Err(e) => {
                    error!("xlsx:Line: {:<2} Upload [{}/{}] file {:10} error, cause: {}, retry again, sleep 3s.", self.rid, current, counter, local_file_name, e);
                }
            }
            std::thread::sleep(Duration::from_secs(3));
        }

        // let mut f = File::open(&file).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        
        // 文件大小
        let file_kb = file_size  as f64 / 1024.0;
        let col_size=format!("{:.0}",file_kb).len();
        // 本次已发送字节数
        let mut bytes_send = 0;
        // 全部发送的字节数
        let mut total_bytes_send: usize = 0;
        // 块大小
        let chunk_size = get_chunk_size();

        // 是否上传完成
        let mut completed = false;

        // 当前的时钟
        let mut clock = Local::now().timestamp();
        // 16KB
        // 1.5M => 1M
        for chunk in buf.chunks(chunk_size) {
            // called `Result::unwrap()` on an `Err` value: Custom { kind: Other, error: "Unable to send channel data" }
            // 网络不稳定会导致报错：error: "Unable to send channel data"
            match ch.write_all(chunk) {
                Ok(()) => {},
                Err(e) => {
                    error!("xlsx:Line: {:<2} Upload [{}/{}] file {} interrupted, Network not available, {}, retry again after 3s.", self.rid, current, counter, local_file_name, e);
                    std::thread::sleep(Duration::from_secs(3));
                    // 再次重试还是失败的话，那只能重新上传整个文件
                    match ch.write_all(chunk) {
                        Ok(()) => {},
                        Err(e) => {
                            error!("xlsx:Line: {:<2} Upload [{}/{}] file {} interrupted, Network not available, {}", self.rid, current, counter, local_file_name, e);
                            break;
                        }
                    }
                }
            }
            bytes_send += chunk.len();
            total_bytes_send += chunk.len();

            let p = (total_bytes_send as f64 * 100.0 / file_size as f64).floor();
            let total_send_kb = (total_bytes_send  as f64 / 1024.0).ceil();

            // 1、如果传输已经到100%，则不用输出如下日志
            if total_bytes_send as u64 >= file_size {
                // 速度：平均一秒的速度
                let speed_kb: f64 = (bytes_send as f64 / 1024.0 / 1.0).ceil();
                info!("xlsx:Line: {:<2} TX [{}/{}] {} {:>3.0}% {:col_size$.0}KiB / {:.0}KiB {:col_size$.0}KiB/s Done", self.rid, current, counter, local_file_name, p, total_send_kb, file_kb, speed_kb);
                completed = true;
                break;
            }

            // 当前的时钟：相差多少秒
            let new_clock = Local::now().timestamp();
            let duration_sec = new_clock - clock;
            if duration_sec > 0 {
                // 速度计算
                let mut speed_kb = (bytes_send as f64 / 1024.0 / duration_sec as f64).ceil();
                if speed_kb == 0.0 {
                    speed_kb = 1.0;
                }
                // 计算用时
                // 2、传输文件较大，需要拆分成为多次传输，每次打印的信息按秒打印
                let eta_secs = (file_kb - total_send_kb) / speed_kb;
                let eta = eta_format(eta_secs as u64);
                info!("xlsx:Line: {:<2} TX [{}/{}] {} {:>3.0}% {:col_size$.0}KiB / {:.0}KiB {:col_size$.0}KiB/s {} ETA", self.rid, current, counter, local_file_name, p, total_send_kb, file_kb, speed_kb, eta);
                // 重置计时器
                clock = Local::now().timestamp();
                bytes_send = 0;
            }

            
        }
        
        ch.send_eof().unwrap();
        ch.wait_eof().unwrap();
        ch.close().unwrap();
        ch.wait_close().unwrap();

        // 计算sha256sum并写入sha256sum.txt文件
        completed & self.write_sha256sum_to_file(file, rfile) & self.move_file(&remote_tmp_file_path, &remote_file)

    }

    // 计算本地文件的sha256sum，并写入远程目录文件
    // 写入 $DBPS_HOME/bin/
    fn write_sha256sum_to_file(&self, file: PathBuf, rfile: PathBuf) -> bool {
        let local_file_checksum = file::sha256sum(file.to_path_buf());

        let remote_file_dir = rfile.parent().unwrap().to_str().unwrap();
        // $DBPS_HOME/bin/monica.sha256sum.txt.tmp
        let remote_checksum_file = format!("{}/{}.tmp", remote_file_dir, BACKUPUP_SHA256SUM_FILENAME);

        let (_, _, stderr) = self.exec_cmd_with_status(&format!("echo \"{} {}\" >> {}", local_file_checksum, rfile.to_string_lossy().to_string(), remote_checksum_file));

        if !stderr.is_empty() {
            error!("SHA-256sum file write failed, cause: {}", stderr);
            false
        } else {
            true
        }
    }

    // 删除文件
    pub fn remove_sha256sum_file(&self, dbps_home: &str) -> bool {

        // $DBPS_HOME/bin/monica.sha256sum.txt.tmp
        let remote_checksum_file = format!("{}/bin/{}.tmp", dbps_home, BACKUPUP_SHA256SUM_FILENAME);

        let (_, _, stderr) = self.exec_cmd_with_status(&format!("rm \"{}\" 2>/dev/null", remote_checksum_file));

        if !stderr.is_empty() {
            error!("SHA-256sum file remove failed, cause: {}", stderr);
            false
        } else {
            true
        }
    }

    // 查询 index 文件获取最近的一个文件
    pub fn verify_sha256sum_file(&self, base: &str) -> bool {

        // $DBPS_HOME/bin/monica.sha256sum.txt.tmp
        let remote_checksum_file = format!("{}/bin/{}.tmp", base, BACKUPUP_SHA256SUM_FILENAME);
        let mut cmd = format!("export LANG=en_US.utf8 && ");
        cmd = format!("{} export file_count=$(cat {} | wc -l) && ", cmd, remote_checksum_file);
        cmd = format!("{} sha256sum -c {} | grep ': OK' | wc -l | awk -v c=$file_count '{{print $0==c}}'", cmd, remote_checksum_file);
        let stdout = self.exec_cmd(&cmd);
        // 返回0，则代表有文件的sha256sum不一致；返回1，全部sha256sum通过
        stdout.trim_end_matches("\n") == "1"
    }

    // // 向远程服务器发送文件
    // pub fn scp_send3(&self, file: PathBuf, rfile: PathBuf) -> bool {
    //     let remote_file = rfile.to_string_lossy().to_string();
    //     // let local_file = file.to_string_lossy().to_string();
    //     let local_file_name = file.file_name().unwrap().to_str().unwrap();
    //     // info!("xlsx:Line: {:<2} Upload \"{}\"", self.rid, local_file);
    //     info!("xlsx:Line: {:<2} Upload {} to \"{}\"", self.rid, local_file_name, remote_file);

    //     // 获取本地文件的基础信息
    //     let file_size = get_filesize(&file);
    //     // Write the file
    //     // 文件繁忙：
    //     // called `Result::unwrap()` on an `Err` value: Error { code: Session(-28), msg: "failed to send file" }

    //     // 避免程序正在使用/运行，先生成临时文件
    //     let remote_tmp_file_path: &String = &format!("{}.monica", remote_file);
    //     // 临时文件
    //     let remote_tmp_file = Path::new(remote_tmp_file_path);

    //     let mut ch;
    //     loop {
    //         match self.sess.scp_send(&remote_tmp_file, 0o755, file_size, None) {
    //             Ok(c) => {
    //                 ch = c;
    //                 break;
    //             },
    //             Err(e) => {
    //                 error!("xlsx:Line: {:<2} Upload file {:10} error: {}, retry again, sleep 3s.", local_file_name, self.rid, e);
    //                 std::thread::sleep(Duration::from_secs(3));
    //             }
    //         };
    //     }

    //     let mut f = File::open(&file).unwrap();
    //     let mut buf: Vec<u8> = Vec::new();
    //     f.read_to_end(&mut buf).unwrap();
        
    //     // 文件大小
    //     let file_kb = file_size  as f64 / 1024.0;
    //     let col_size=format!("{:.0}",file_kb).len();
    //     // 本次已发送字节数
    //     let mut bytes_send = 0;
    //     // 全部发送的字节数
    //     let mut total_bytes_send: usize = 0;
    //     // 块大小
    //     let chunk_size = get_chunk_size();

    //     // 当前的时钟
    //     let mut clock = Local::now().timestamp();
    //     // 16KB
    //     // 1.5M => 1M
    //     for chunk in buf.chunks(chunk_size) {
    //         // called `Result::unwrap()` on an `Err` value: Custom { kind: Other, error: "Unable to send channel data" }
    //         // 网络不稳定会导致报错：error: "Unable to send channel data"
    //         match ch.write_all(chunk) {
    //             Ok(()) => {},
    //             Err(e) => {
    //                 error!("xlsx:Line: {:<2} Upload file {} interrupted, Network not available, {}, retry again after 3s.", self.rid, local_file_name, e);
    //                 std::thread::sleep(Duration::from_secs(3));
    //                 // 再次重试还是失败的话，那只能重新上传整个文件
    //                 match ch.write_all(chunk) {
    //                     Ok(()) => {},
    //                     Err(e) => {
    //                         error!("xlsx:Line: {:<2} Upload file {} interrupted, Network not available, {}", self.rid, local_file_name, e);
    //                         break;
    //                     }
    //                 }
    //             }
    //         }
    //         bytes_send += chunk.len();
    //         total_bytes_send += chunk.len();

    //         let p = (total_bytes_send as f64 * 100.0 / file_size as f64).floor();
    //         let total_send_kb = (total_bytes_send  as f64 / 1024.0).ceil();

    //         // 1、如果传输已经到100%，则不用输出如下日志
    //         if total_bytes_send as u64 >= file_size {
    //             // 速度：平均一秒的速度
    //             let speed_kb: f64 = (bytes_send as f64 / 1024.0 / 1.0).ceil();
    //             info!("xlsx:Line: {:<2} TX {} {:>3.0}% {:col_size$.0}KiB / {:.0}KiB {:col_size$.0}KiB/s Done", self.rid, local_file_name, p, total_send_kb, file_kb, speed_kb);
    //             break;
    //         }

    //         // 当前的时钟：相差多少秒
    //         let new_clock = Local::now().timestamp();
    //         let duration_sec = new_clock - clock;
    //         if duration_sec > 0 {
    //             // 速度计算
    //             let mut speed_kb = (bytes_send as f64 / 1024.0 / duration_sec as f64).ceil();
    //             if speed_kb == 0.0 {
    //                 speed_kb = 1.0;
    //             }
    //             // 计算用时
    //             // 2、传输文件较大，需要拆分成为多次传输，每次打印的信息按秒打印
    //             let eta_secs = (file_kb - total_send_kb) / speed_kb;
    //             let eta = eta_format(eta_secs as u64);
    //             info!("xlsx:Line: {:<2} TX {} {:>3.0}% {:col_size$.0}KiB / {:.0}KiB {:col_size$.0}KiB/s {} ETA", self.rid, local_file_name, p, total_send_kb, file_kb, speed_kb, eta);
    //             // 重置计时器
    //             clock = Local::now().timestamp();
    //             bytes_send = 0;
    //         }

            
    //     }
        
    //     ch.send_eof().unwrap();
    //     ch.wait_eof().unwrap();
    //     ch.close().unwrap();
    //     ch.wait_close().unwrap();


    //     // 重命名文件 后 再验证文件
    //     self.move_file(&remote_tmp_file_path, &remote_file).verify_file(file, rfile);

    //     true

    // }


    // // 验证文件
    // // 检查执行权限和版本信息是否有效
    // // 最有效的方式就是直接执行一次，看看是否会执行失败
    // // DBPS_HOME/bin/<file> -v
    // fn verify_file(&self, file: PathBuf, rfile: PathBuf) -> &Self {

    //     // 计算本地文件的sha256
    //     // https://docs.rs/sha256/latest/sha256/
    //     let remote_file = rfile.to_string_lossy().to_string();
    //     let local_file_name = file.file_name().unwrap().to_str().unwrap();
    //     let stdout = self.exec_cmd(&format!("sha256sum \"{}\" | awk '{{print $1}}'", remote_file));
    //     let remote_file_sha256 = stdout.trim_end_matches("\n");
    //     if remote_file_sha256 == fxx::sha256sum(file) {
    //         info!("xlsx:Line: {:<2} File {} sha256sum verify passed", self.rid, local_file_name);
    //     } else {
    //         // SHA-256sum 校验失败
    //         // 校验失败后重新上传
    //         error!("xlsx:Line: {:<2} File {} sha256sum verify failed, re-upload after 3s", self.rid, local_file_name);
    //         std::thread::sleep(Duration::from_secs(3));
    //         self.scp_send(file, rfile);
    //     }

    //     self
    // }

    
}


fn eta_format(secs: u64) -> String {
    let remaining_seconds = secs % 60;
    let minutes = (secs % 3600) / 60;
    let hours = secs / 3600;
    let s = format!("{:02.0}:{:02.0}:{:02.0}", hours, minutes, remaining_seconds);
    s
}

fn get_index_file() -> String {
    let index_file = format!("{}/{}", BACKUPUP_DIR, config::BACKUPUP_INDEX_FILENAME);
    index_file
}

fn abnormal_exit(flag: &str, e: ssh2::Error){
    println!("{} failure:", flag);
    println!("  CAUSE: {}", e);
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}