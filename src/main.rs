
use std::{env, fs, io, path::Path, process::exit};
use chrono::Local;
use config::{get_debug, Command};
use log::LevelFilter;
use log4rs::{append::{console::ConsoleAppender, file::FileAppender}, config::{runtime::RootBuilder, Appender, Logger, Root}, encode::pattern::PatternEncoder, Config};
use cmd::{backup::handle_command_backup, lsinventory::handle_command_lsinventory, precheck::handle_command_precheck, rollback::handle_command_rollback};
use structopt::StructOpt;
use crate::config::{get_basedir, get_datadir, get_input_file, get_manifest_file};

mod config;
mod db;
mod ssh;
mod file;
mod cmd;

fn print_info(log_file: &str){
    // :: /data/dataxone/
    let basedir = get_basedir();
    // :: .monica
    let datadir = get_datadir();

    // :: /data/dataxone/
    let log_file_output = env::current_dir().unwrap().display().to_string();

    // 获取编译的时间
    let key: Option<&'static str> = option_env!("BUILD_TIME");

    // 创建日志目录
    fs::create_dir_all(format!("{}/{}/logs", log_file_output, &datadir)).unwrap();

    println!("");
    println!("{} {}, build {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), key.unwrap());
    println!("{} version {}", env!("CARGO_PKG_DESCRIPTION"), env!("CARGO_PKG_VERSION"));
    println!("copyright (c) 2024, dsgdata.com.  All rights reserved.");
    println!("Authors           : {}", env!("CARGO_PKG_AUTHORS"));
    println!("Dataxone base     : {}", &basedir);
    println!("Monica datadir    : {}/{}", log_file_output, &datadir);
    println!("Manifest file     : {}", get_manifest_file());
    println!("Input file        : {}", get_input_file());
    println!("Log file location : {}", log_file);
    println!("");
    println!("--------------------------------------------------------------------------------");
    println!("");
    println!("Local Machine Information::");
    println!("Platform description: {} {}", env::consts::OS, env::consts::ARCH);
    println!("");
    println!("--------------------------------------------------------------------------------");
    println!("");

}


#[tokio::main]
async fn main() -> Result<(), sqlx::Error>{
    let opt: config::Opt = config::Opt::from_args();

    let log_file_output = env::current_dir().unwrap().display().to_string();
    let datadir = get_datadir();
    let log_dir = format!("{}/{}/logs", log_file_output, &datadir);
    let log_file_name = format!("monica-{}.log", Local::now().format("%Y-%m-%d_%H%M%S"));
    let log_file = format!("{}/{}", log_dir, log_file_name);
    
    print_info(&log_file);

    let mut level = LevelFilter::Info;
    if get_debug() {
        level = LevelFilter::Debug;
    }

    let path = Path::new(&log_file);
    let pe = Box::new(PatternEncoder::new("{d(%m %d %H:%M:%S)} {({M}):11.22} {l:>5.5} {m}{n}"));
    let stdout = ConsoleAppender::builder().encoder(pe.clone()).build();
    let file = FileAppender::builder().encoder(pe).build(path).unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file", Box::new(file)))
        // .logger(Logger::builder().build("app::backend::db", LevelFilter::Info))
        // .logger(Logger::builder()
        //     .appender("requests")
        //     .additive(false)
        //     .build("app::requests", LevelFilter::Info))
        .build(Root::builder().appender("stdout").appender("file").build(level))
        .unwrap();
    let _ = log4rs::init_config(config).unwrap();

    match opt.command {
        Command::Precheck(a)=> {
            // 预检查功能
            println!("User request: precheck\n");
            
            // 提前检查xlsx是否有效
            let _ = config::GLOBAL_CONFIG.servers;

            handle_command_precheck(a.worker_threads).await;
        },
        Command::Patch(a) => {

            println!("User request: patch\n");
            if !a.skip_check {
                handle_command_precheck(a.worker_threads).await;
            }

            // 备份
            handle_command_backup(a.worker_threads).await;
    
            // 跳过提示，直接升级
            if !a.quiet {
                for i in 0..3 {
                    let mut input = String::new();

                    println!("Do you want to continue apply patch? [y|n] ");
                    
                    io::stdin().read_line(&mut input).unwrap();
                    if input.starts_with("y") {
                        break;
                    }
                    if i == 2 || input.starts_with("n") {
                        println!("Bye.");
                        exit(-1);
                    } 
                }
            }

            // 提前检查xlsx是否有效
            let _ = config::GLOBAL_CONFIG.servers;
    
            // 升级操作
            cmd::apply::handle_command_xpatch(a.worker_threads).await;

        },
        Command::Rollback(a) => {
            println!("User request: rollback\n");
            // 回退
            handle_command_rollback(a.worker_threads).await;
        },
        Command::Lsinventory(a) => {
            // 列出远端目录
            println!("User request: lsinventory\n");
            handle_command_lsinventory(a.worker_threads);
        },
        Command::Backup(a) => {
            // 列出远端目录
            println!("User request: backup\n");

            // 提前检查xlsx是否有效
            let _ = config::GLOBAL_CONFIG.servers;
            handle_command_backup(a.worker_threads).await;
        }
    }

    println!("{} completed", env!("CARGO_PKG_NAME"));
    println!("");

    Ok(())
}
