use std::{fs::{self, File, OpenOptions}, io::{Read, Write}, path::{Path, PathBuf}, process::exit};

use chrono::Local;
use flate2::read::GzDecoder;
use log::{error, info};
use tar::Archive;

use crate::config::{self, get_local_inventory_dir, Server, GLOBAL_CONFIG};


// 调用系统命令解压文件
pub fn extract_compressed_files(base: &str, tgz: &str, dst: &str, rid: usize) {
    
    let path = Path::new(base).join(tgz);
    info!("xlsx:Line: {:<2} Extracting file {}", rid, path.to_string_lossy().to_string());
    if !path.exists() {
        abnormal_exit_not_found();
    }
    let file = File::open(path.clone()).unwrap();
    let tar: GzDecoder<File> = GzDecoder::new(file);
    let mut archive = Archive::new(tar);
    archive.set_overwrite(true);
    // archive.unpack(format!("{}/{}/..", base, dst)).unwrap();
    match archive.unpack(format!("{}/{}/..", base, dst)) {
        Ok(_) => {},
        Err(e) => {
            let p = path.to_string_lossy().to_string();
            error!("xlsx:Line: {:<2} Extracting file {} failed, cause: {}", rid, p, e);
            // abnormal_exit_precheck(&e.to_string());
        }
    }
}

pub fn path_join(s: &str, path: &str) -> String {
    let s1 = format!("{}/{}", s, path);
    s1
}

// pub fn local_path_join(s: &str, path: &str) -> std::path::PathBuf {
//     let p = Path::new(path).join(path);
//     p
// }

// 计算sha256sum
pub fn sha256sum(file: PathBuf) -> String {
    let file_sha256 = sha256::try_digest(file).unwrap();
    file_sha256
}

pub fn get_filesize(file: &PathBuf) -> u64{
    std::fs::metadata(file).map(|metadata| metadata.len()).unwrap_or(0)
}

pub fn abnormal_exit_not_found(){
    println!("PreChecks failure:::");
    println!("  CAUSE: No such file or directory");
    println!("  ACTION: Contact DSG Support Services or refer to the software manual.");
    println!("Bye.");
    exit(-1);
}

// 将备份断点信息写入缓存
// pub fn write_backup_ckp(xlsx_checksum: &str, rid: usize){
//     // 写入断点信息到本地缓存目录
//     let mut f = OpenOptions::new().append(true).write(true).create(true).open(get_backup_cache_file()).unwrap();
//     f.write_all(format!("{}:{}:{}:{}\n", &get_input_file(), xlsx_checksum, rid, Local::now().format("%Y-%m-%d %H:%M:%S")).as_bytes()).unwrap();
// }

// pub fn get_backup_ckp_contents() -> String {

//     let mut f = OpenOptions::new().append(true).write(true).create(true).open(get_backup_cache_file()).unwrap();
//     let mut contents: String = String::new();

//     match f.read_to_string(&mut contents) {
//         Ok(_) => {},
//         Err(_) => {}
//     }

//     contents
// }

// 将补丁断点信息写入缓存
// pub fn write_patch_ckp(xlsx_checksum: &str, rid: usize){
//     // 写入断点信息到本地缓存目录
//     let mut f = OpenOptions::new().append(true).write(true).create(true).open(get_patch_cache_file()).unwrap();
//     f.write_all(format!("{}:{}:{}:{}\n", &get_input_file(), xlsx_checksum, rid, Local::now().format("%Y-%m-%d %H:%M:%S")).as_bytes()).unwrap();
// }

// pub fn get_patch_ckp_contents() -> String {

//     let mut f = OpenOptions::new().append(true).write(true).create(true).open(get_patch_cache_file()).unwrap();
//     let mut contents: String = String::new();

//     match f.read_to_string(&mut contents) {
//         Ok(_) => {},
//         Err(_) => {}
//     }

//     contents
// }


// 写入本地清单目录
pub fn write_local_inventory(xlsx_checksum: &str){

    let dir = get_local_inventory_dir();

    // .monica/inventory/142cc5bcbe240c17bebba1d9cfa6721a062554c3a1eeaac580b4204d1e37c325
    let local_inventory_dir = format!("{}/{}", dir, xlsx_checksum);
    fs::create_dir_all(local_inventory_dir).unwrap();

    // .monica/inventory/142cc5bcbe240c17bebba1d9cfa6721a062554c3a1eeaac580b4204d1e37c325/input.json  #记录有那些链路需要升级
    let local_inventory_file = format!("{}/{}/input.json", dir, xlsx_checksum);

    // 写入文件
    let contents = serde_json::to_string(&config::GLOBAL_CONFIG.servers).unwrap();
    fs::write(local_inventory_file, contents).unwrap();

    // 写入backupset.index文件
    let index_file = format!("{}/backupset.index", dir);
    // 格式：142cc5bcbe240c17bebba1d9cfa6721a062554c3a1eeaac580b4204d1e37c325:日期
    let s = format!("{}:{}:{}:{}\n", &xlsx_checksum[0..12], xlsx_checksum, GLOBAL_CONFIG.servers.len(), Local::now().format("%Y-%m-%d %H:%M:%S"));

    match OpenOptions::new().append(true).write(true).create(true).open(&index_file) {
        Ok(mut f) => {
            f.write_all(s.as_bytes()).unwrap();
        },
        Err(e) => {
            error!("File {} write failed, cause: {}", &index_file, e);
        }
    }

}


// 写入检查点文件
pub fn write_checkpoint(dbps_home: &str, s: &Server, role: usize, xlsx_checksum: &str){

    // .monica/inventory
    let dir = get_local_inventory_dir();

    // .monica/inventory/<checksum>/
    let local_inventory_dir = format!("{}/{}", dir, xlsx_checksum);
    fs::create_dir_all(local_inventory_dir).unwrap();

    // .monica/inventory/<checksum>/2-0.ckp
    // .monica/inventory/<checksum>/2-10.ckp
    // .monica/inventory/<checksum>/2-11.ckp
    let local_ckp_file = format!("{}/{}/{}-{}.ckp", dir, xlsx_checksum, s.rid, role);

    let s = format!("{}:{}:{}:{}\n", dbps_home, s.service_name, s.rid, Local::now().format("%Y-%m-%d %H:%M:%S"));

    write_ckp(&local_ckp_file, s);

}

fn write_ckp(local_ckp_file: &String, s: String) {
    match OpenOptions::new().append(true).write(true).create(true).open(&local_ckp_file) {
        Ok(mut f) => {
            f.write_all(s.as_bytes()).unwrap();
        },
        Err(e) => {
            error!("File {} write failed, cause: {}", &local_ckp_file, e);
        }
    }
}

pub fn read_checkpoint(s: &Server, role: usize, xlsx_checksum: &str) -> Option<String> {

    // .monica/inventory
    let dir = get_local_inventory_dir();

    // .monica/inventory/<checksum>/2-0.ckp
    // .monica/inventory/<checksum>/2-10.ckp
    // .monica/inventory/<checksum>/2-11.ckp
    let local_ckp_file = format!("{}/{}/{}-{}.ckp", dir, xlsx_checksum, s.rid, role);

    read_ckp(local_ckp_file)

}

fn read_ckp(local_ckp_file: String) -> Option<String> {
    match File::open(local_ckp_file) {
        Ok(mut f) => {
            let mut contents: String = String::new();
            f.read_to_string(&mut contents).unwrap();
            Some(contents)
        },
        Err(_) => None
    }
}

// 写入检查点文件
pub fn write_backup_checkpoint(dbps_home: &str, s: &Server, role: usize, xlsx_checksum: &str){

    // .monica/inventory
    let dir = get_local_inventory_dir();

    // .monica/inventory/<checksum>/
    let local_inventory_dir = format!("{}/{}", dir, xlsx_checksum);
    fs::create_dir_all(local_inventory_dir).unwrap();

    // .monica/inventory/<checksum>/2-0.ckp
    // .monica/inventory/<checksum>/2-10.ckp
    // .monica/inventory/<checksum>/2-11.ckp
    let local_ckp_file = format!("{}/{}/{}-{}.bak", dir, xlsx_checksum, s.rid, role);

    let s = format!("{}:{}:{}:{}\n", dbps_home, s.service_name, s.rid, Local::now().format("%Y-%m-%d %H:%M:%S"));

    write_ckp(&local_ckp_file, s);
}

pub fn read_backup_checkpoint(s: &Server, role: usize, xlsx_checksum: &str) -> Option<String> {

    // .monica/inventory
    let dir = get_local_inventory_dir();

    // .monica/inventory/<checksum>/2-0.ckp
    // .monica/inventory/<checksum>/2-10.ckp
    // .monica/inventory/<checksum>/2-11.ckp
    let local_ckp_file = format!("{}/{}/{}-{}.bak", dir, xlsx_checksum, s.rid, role);

    read_ckp(local_ckp_file)

}


// 写入检查点文件：上传文件断点续传
pub fn write_file_checkpoint(s: &Server, role: usize, xlsx_checksum: &str, local_file: &str){

    // .monica/inventory
    let dir = get_local_inventory_dir();

    // .monica/inventory/<checksum>/
    let local_inventory_dir = format!("{}/{}", dir, xlsx_checksum);
    fs::create_dir_all(local_inventory_dir).unwrap();

    // .monica/inventory/<checksum>/2-0.file.ckp
    // .monica/inventory/<checksum>/2-10.file.ckp
    // .monica/inventory/<checksum>/2-11.file.ckp
    let local_ckp_file = format!("{}/{}/{}-{}.file.ckp", dir, xlsx_checksum, s.rid, role);

    match OpenOptions::new().append(true).write(true).create(true).open(&local_ckp_file) {
        Ok(mut f) => {
            f.write_all(format!("{}\n", local_file).as_bytes()).unwrap();
        },
        Err(e) => {
            error!("File {} write failed, cause: {}", &local_ckp_file, e);
        }
    }

}

// 文件是否已经上传过
pub fn file_checkpoint(s: &Server, role: usize, xlsx_checksum: &str, local_file: &str) -> Option<String> {

    // .monica/inventory
    let dir = get_local_inventory_dir();

    // .monica/inventory/<checksum>/2-0.file.ckp
    // .monica/inventory/<checksum>/2-10.file.ckp
    // .monica/inventory/<checksum>/2-11.file.ckp
    let local_ckp_file = format!("{}/{}/{}-{}.file.ckp", dir, xlsx_checksum, s.rid, role);

    match File::open(local_ckp_file) {
        Ok(mut f) => {
            let mut contents: String = String::new();
            f.read_to_string(&mut contents).unwrap();

            for line in contents.lines() {
                if line == local_file {
                    return Some(String::from(local_file));
                }
            }

            None
        },
        Err(_) => None
    }

}

// 获取本地的清单索引文件
pub fn read_local_inventory_index() -> String {

    // .monica/inventory
    let dir = get_local_inventory_dir();
    
    // 写入backupset.index文件
    let index_file = format!("{}/backupset.index", dir);
    
    let mut contents: String = String::new();
    match File::open(index_file) {
        Ok(mut f) => {
            f.read_to_string(&mut contents).unwrap();
            contents
        },
        Err(_) => contents
    }
}

pub fn clean_local_inventory(checksum: &str, s: &str){

    // .monica/inventory
    let dir = get_local_inventory_dir();
    match fs::remove_dir_all(&format!("{}/{}", dir, checksum)) {
        Ok(_) => {
            info!("Clean local inventory dir {}/{} for rollback", dir, checksum);

            // 从文件中删除对应的行
            let index_file = format!("{}/backupset.index", dir);

            let mut new_lines = Vec::new();
            for line in s.lines() {
                if line.contains(&format!(":{}:", checksum)){
                    continue;
                }
                new_lines.push(line);
            }

            match File::create(&index_file) {
                Ok(mut f) => {
                    f.write_all(new_lines.join("\n").as_bytes()).unwrap();
                    f.flush().unwrap();
                    info!("Flush local inventory index {} for rollback", index_file);
                },
                Err(e) => {
                    error!("File {} open failed, cause: {}", &index_file, e);
                }
            }
        },
        Err(e) => {error!("Remove local inventory di failed, cause: {}", e)}
    }

}