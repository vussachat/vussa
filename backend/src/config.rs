pub(crate) const MAIN_CHANNEL: &str = "main";
pub(crate) const HISTORY_PAGE_SIZE: usize = 50;
pub(crate) const HOT_HISTORY_LIMIT: usize = 300;
pub(crate) const SESSION_TTL_SECONDS: u64 = 60 * 60 * 24 * 14;
pub(crate) const CSRF_HEADER: &str = "x-csrf-token";
pub(crate) const PRESENCE_TTL_SECONDS: u64 = 45;
pub(crate) const MAX_FILE_BYTES: usize = 25 * 1024 * 1024;

pub(crate) fn upload_dir() -> String {
    std::env::var("UPLOAD_DIR").unwrap_or_else(|_| ".data/uploads".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_limits_are_bounded() {
        assert_eq!(HISTORY_PAGE_SIZE, 50);
        assert_eq!(MAX_FILE_BYTES, 25 * 1024 * 1024);
        assert_eq!(SESSION_TTL_SECONDS, 60 * 60 * 24 * 14);
    }
}
