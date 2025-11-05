use std::sync::atomic::{AtomicBool, Ordering};

static DEBUG_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn enable_debug() {
    DEBUG_ENABLED.store(true, Ordering::Relaxed);
}

pub fn disable_debug() {
    DEBUG_ENABLED.store(false, Ordering::Relaxed);
}

pub fn is_debug_enabled() -> bool {
    DEBUG_ENABLED.load(Ordering::Relaxed)
}

#[macro_export]
macro_rules! byteorm_debug {
    ($($arg:tt)*) => {
        if $crate::rustgen::debug::is_debug_enabled() {
            eprintln!("[ByteORM Debug] {}", format!($($arg)*));
        }
    };
}

pub fn log_query(sql: &str, params_count: usize) {
    if is_debug_enabled() {
        eprintln!("[ByteORM Debug] Executing SQL: {}", sql);
        eprintln!("[ByteORM Debug] Parameters count: {}", params_count);
    }
}

pub fn log_query_with_params(sql: &str, params: &[&str]) {
    if is_debug_enabled() {
        eprintln!("[ByteORM Debug] Executing SQL: {}", sql);
        eprintln!("[ByteORM Debug] Parameters: {:?}", params);
    }
}

pub fn log_result(operation: &str, rows_affected: u64) {
    if is_debug_enabled() {
        eprintln!("[ByteORM Debug] {} - Rows affected: {}", operation, rows_affected);
    }
}

pub fn log_error(operation: &str, error: &str) {
    if is_debug_enabled() {
        eprintln!("[ByteORM Debug] Error in {}: {}", operation, error);
    }
}
