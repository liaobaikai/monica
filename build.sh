export BUILD_TIME=`date '+%Y-%m-%d'`
tar -xf upx-4.2.3-amd64_linux.tar.xz
cargo build --release
strip target/release/monica
chmod +x ./upx
./upx target/release/monica



