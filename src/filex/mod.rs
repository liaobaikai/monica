use std::{fs::File, path::Path};

use flate2::read::GzDecoder;
use log::debug;
use tar::Archive;


// 调用系统命令解压文件
pub fn extract_compressed_files(base: &str, tgz: &str, dst: &str) {
    
    let path = Path::new(base).join(tgz);
    debug!("[PreChecks]:: Extracting file {}", path.display());
    let file = File::open(path).unwrap();
    let tar = GzDecoder::new(file);
    let mut archive = Archive::new(tar);
    archive.set_overwrite(true);
    archive.unpack(format!("{}/{}/..", base, dst)).unwrap();

    // let mut flag = false;
    // if Path::new(&m.dir).is_dir() {
    //     flag = true;
    // }

    // if flag {
    //     let mut exists: bool = true;
    //     for f in m.file.iter() {
    //         if !Path::new(&m.dir).join(f).is_file() {
    //             // 文件
    //             exists = false;
    //         }
    //     }
    //     flag = flag | exists;
    // }

    // let dir = file.parent().unwrap();
    // let mut command = Command::new("tar");
    // command.current_dir(dir);
    // // command.args(["-z", "-x", "-f", &local_file_path.to_str().unwrap(), "-C", parent_dir.to_str().unwrap()]);
    // command.args(["-z", "-x", "-f", &file.to_str().unwrap()]);
    // command.spawn().expect("tar command failed to start");
    // flag
}



