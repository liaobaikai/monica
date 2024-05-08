set BUILD_TIME=%date:~0,4%-%date:~5,2%-%date:~8,2%
cargo build --release
rem https://github.com/upx/upx/releases
.\upx.exe target/release/monica.exe