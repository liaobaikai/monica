use crate::cmd::print_local_inventory_tab;

// 查看备份集事件处理
pub fn handle_command_lsinventory(_: usize){
    println!("{}", print_local_inventory_tab());
    println!("");
}