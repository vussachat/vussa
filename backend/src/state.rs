use super::*;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) valkey: redis::Client,
    pub(crate) cache_health: Arc<dyn CacheHealth>,
    pub(crate) database_health: Arc<dyn DatabaseHealth>,
    pub(crate) database: PgPool,
    pub(crate) repository: Arc<dyn ChatRepository>,
    pub(crate) blob_store: Arc<dyn BlobStore>,
    pub(crate) scanner: Arc<dyn FileScanner>,
    pub(crate) recovery_notifier: Arc<dyn RecoveryNotifier>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) rooms: Arc<RoomManager>,
    pub(crate) password_verifiers: Arc<Semaphore>,
    pub(crate) password_verification_flights:
        Arc<TokioMutex<HashMap<VerificationKey, VerificationSender>>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct VerificationKey(pub(crate) [u8; 32]);

#[derive(Clone, Copy, Debug)]
pub(crate) enum VerificationOutcome {
    Verified(bool),
    Overloaded,
    Unavailable,
}

pub(crate) type VerificationSender = watch::Sender<Option<VerificationOutcome>>;
