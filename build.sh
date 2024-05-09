export BUILD_TIME=`date '+%Y-%m-%d'`
cargo build --release
strip target/release/monica
chmod +x ./upx
./upx target/release/monica



