use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AuthUser {
    pub(crate) id: Uuid,
    pub(crate) email: String,
    pub(crate) username: String,
    pub(crate) roles: Vec<String>,
    pub(crate) permissions: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct Session {
    pub(crate) id: Uuid,
    pub(crate) csrf: String,
    pub(crate) user: AuthUser,
}

#[derive(Debug, Serialize)]
pub(crate) struct Channel {
    pub(crate) name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ConversationSummary {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) owner_user_id: Option<Uuid>,
    pub(crate) display_name: String,
    pub(crate) peer_user_id: Option<Uuid>,
    pub(crate) peer_username: Option<String>,
    pub(crate) last_message_at: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ChannelMember {
    pub(crate) user_id: Uuid,
    pub(crate) username: String,
    pub(crate) membership_role: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ReactionSummary {
    pub(crate) message_id: Uuid,
    pub(crate) emoji: String,
    pub(crate) user_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ClientEvent {
    ListChannels,
    CreateChannel {
        name: String,
    },
    CreatePrivateChannel {
        name: String,
    },
    OpenDirect {
        user_id: Uuid,
    },
    InviteMember {
        channel: String,
        user_id: Uuid,
    },
    RemoveMember {
        channel: String,
        user_id: Uuid,
    },
    JoinChannel {
        name: String,
    },
    DeleteChannel {
        name: String,
    },
    DeleteMessage {
        id: Uuid,
    },
    AddReaction {
        message_id: Uuid,
        emoji: String,
    },
    RemoveReaction {
        message_id: Uuid,
        emoji: String,
    },
    Typing {
        typing: bool,
    },
    MarkRead {
        message_id: Uuid,
        created_at: u64,
    },
    SendMessage {
        text: String,
        #[serde(default)]
        root_message_id: Option<Uuid>,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        file_ids: Vec<Uuid>,
    },
    EditMessage {
        id: Uuid,
        text: String,
    },
    LoadHistory {
        channel: String,
        before_created_at: u64,
        before_id: Uuid,
    },
    LoadThread {
        message_id: Uuid,
        before_created_at: Option<u64>,
        before_id: Option<Uuid>,
    },
    Heartbeat,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct ChatMessage {
    pub(crate) id: Uuid,
    pub(crate) channel: String,
    pub(crate) username: String,
    pub(crate) text: String,
    pub(crate) created_at: u64,
    pub(crate) edited: bool,
    pub(crate) deleted: bool,
    pub(crate) root_message_id: Option<Uuid>,
    pub(crate) reply_count: u32,
    pub(crate) metadata: serde_json::Value,
    pub(crate) mentions: Vec<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) file_ids: Vec<Uuid>,
}

impl ChatMessage {
    pub(crate) fn try_from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;

        let created_at = row.try_get::<i64, _>("created_at")?;
        let reply_count = row.try_get::<i64, _>("reply_count")?;
        Ok(Self {
            id: row.try_get("id")?,
            channel: row.try_get("channel")?,
            username: row.try_get("username")?,
            text: row.try_get("text")?,
            created_at: checked_unsigned(created_at, "created_at")?,
            edited: row.try_get("edited")?,
            deleted: row.try_get("deleted")?,
            root_message_id: row.try_get("root_message_id")?,
            reply_count: checked_unsigned(reply_count, "reply_count")?,
            metadata: row.try_get("metadata")?,
            mentions: row.try_get("mentions")?,
            client_id: row.try_get("client_id")?,
            file_ids: row.try_get("file_ids")?,
        })
    }
}

fn checked_unsigned<T>(value: i64, field: &'static str) -> Result<T, sqlx::Error>
where
    T: TryFrom<i64>,
{
    value.try_into().map_err(|_| {
        sqlx::Error::Decode(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{field} cannot be negative or out of range"),
            )
            .into(),
        )
    })
}

#[derive(Debug, bitcode::Encode, bitcode::Decode, Clone)]
pub(crate) struct StoredMessage {
    pub(crate) id: Uuid,
    pub(crate) channel: String,
    pub(crate) username: String,
    pub(crate) text: String,
    pub(crate) created_at: u64,
    pub(crate) edited: bool,
    pub(crate) deleted: bool,
    pub(crate) root_message_id: Option<Uuid>,
    pub(crate) reply_count: u32,
    pub(crate) metadata_json: String,
    pub(crate) mentions: Vec<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) file_ids: Vec<Uuid>,
    pub(crate) owner_session: Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Participant {
    pub(crate) user_id: Uuid,
    pub(crate) username: String,
    #[serde(default)]
    pub(crate) display_name: String,
    #[serde(default)]
    pub(crate) custom_status: String,
    pub(crate) roles: Vec<String>,
    pub(crate) online: bool,
}

impl StoredMessage {
    pub(crate) fn from_message(message: ChatMessage, owner_session: Uuid) -> Self {
        Self {
            id: message.id,
            channel: message.channel,
            username: message.username,
            text: message.text,
            created_at: message.created_at,
            edited: message.edited,
            deleted: message.deleted,
            root_message_id: message.root_message_id,
            reply_count: message.reply_count,
            metadata_json: serde_json::to_string(&message.metadata).unwrap_or_else(|_| "{}".into()),
            mentions: message.mentions,
            client_id: message.client_id,
            file_ids: message.file_ids,
            owner_session,
        }
    }

    pub(crate) fn into_message(self) -> ChatMessage {
        ChatMessage {
            id: self.id,
            channel: self.channel,
            username: self.username,
            text: self.text,
            created_at: self.created_at,
            edited: self.edited,
            deleted: self.deleted,
            root_message_id: self.root_message_id,
            reply_count: self.reply_count,
            metadata: serde_json::from_str(&self.metadata_json)
                .unwrap_or_else(|_| serde_json::json!({})),
            mentions: self.mentions,
            client_id: self.client_id,
            file_ids: self.file_ids,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerEvent {
    Welcome {
        username: String,
    },
    Channels {
        channels: Vec<String>,
    },
    PrivateConversations {
        conversations: Vec<ConversationSummary>,
    },
    Members {
        channel: String,
        members: Vec<ChannelMember>,
    },
    ChannelCreated {
        name: String,
    },
    ChannelDeleted {
        name: String,
    },
    Joined {
        name: String,
    },
    History {
        channel: String,
        messages: Vec<ChatMessage>,
        source: HistorySource,
        has_more: bool,
    },
    HistoryPage {
        channel: String,
        messages: Vec<ChatMessage>,
        source: HistorySource,
        has_more: bool,
    },
    Message {
        message: ChatMessage,
    },
    MessageUpdated {
        message: ChatMessage,
    },
    ThreadHistory {
        root_message_id: Uuid,
        messages: Vec<ChatMessage>,
        has_more: bool,
    },
    ReactionUpdated {
        channel: String,
        reaction: ReactionSummary,
    },
    Typing {
        channel: String,
        user_id: Uuid,
        username: String,
        typing: bool,
    },
    ReadStateUpdated {
        channel: String,
        user_id: Uuid,
        message_id: Uuid,
        created_at: u64,
    },
    Error {
        message: String,
    },
    Participants {
        channel: String,
        participants: Vec<Participant>,
    },
    ParticipantJoined {
        channel: String,
        participant: Participant,
    },
    ParticipantLeft {
        channel: String,
        user_id: Uuid,
    },
    PresenceSync {
        channel: String,
        participants: Vec<Participant>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistorySource {
    Cache,
    Database,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_message_protocol_preserves_optional_collaboration_fields() {
        let root = Uuid::now_v7();
        let file = Uuid::now_v7();
        let event: ClientEvent = serde_json::from_value(serde_json::json!({
            "type": "send_message",
            "text": "hello",
            "root_message_id": root,
            "client_id": "retry-1",
            "file_ids": [file]
        }))
        .expect("valid send event");
        match event {
            ClientEvent::SendMessage {
                text,
                root_message_id,
                client_id,
                file_ids,
            } => {
                assert_eq!(text, "hello");
                assert_eq!(root_message_id, Some(root));
                assert_eq!(client_id.as_deref(), Some("retry-1"));
                assert_eq!(file_ids, vec![file]);
            }
            _ => panic!("unexpected event variant"),
        }
    }

    #[test]
    fn stored_messages_round_trip_without_losing_attachment_state() {
        let message = ChatMessage {
            id: Uuid::now_v7(),
            channel: "main".into(),
            username: "alice".into(),
            text: "attached".into(),
            created_at: 42,
            edited: true,
            deleted: false,
            root_message_id: Some(Uuid::now_v7()),
            reply_count: 2,
            metadata: serde_json::json!({"kind": "test"}),
            mentions: vec!["bob".into()],
            client_id: Some("client-42".into()),
            file_ids: vec![Uuid::now_v7(), Uuid::now_v7()],
        };
        let restored = StoredMessage::from_message(message.clone(), Uuid::now_v7()).into_message();
        assert_eq!(restored.id, message.id);
        assert_eq!(restored.root_message_id, message.root_message_id);
        assert_eq!(restored.reply_count, message.reply_count);
        assert_eq!(restored.metadata, message.metadata);
        assert_eq!(restored.mentions, message.mentions);
        assert_eq!(restored.client_id, message.client_id);
        assert_eq!(restored.file_ids, message.file_ids);
        assert_eq!(restored.text, message.text);
    }

    #[test]
    fn server_events_use_stable_snake_case_wire_names() {
        let event = ServerEvent::ReadStateUpdated {
            channel: "main".into(),
            user_id: Uuid::now_v7(),
            message_id: Uuid::now_v7(),
            created_at: 99,
        };
        let value = serde_json::to_value(event).expect("serializable server event");
        assert_eq!(value["type"], "read_state_updated");
        assert_eq!(value["channel"], "main");
        assert_eq!(value["created_at"], 99);
    }

    #[test]
    fn legacy_presence_payloads_default_new_profile_fields() {
        let participant: Participant = serde_json::from_value(serde_json::json!({
            "user_id": Uuid::nil(),
            "username": "alice",
            "roles": ["user"],
            "online": true
        }))
        .expect("legacy presence payload remains readable");
        assert_eq!(participant.display_name, "");
        assert_eq!(participant.custom_status, "");
    }
}
