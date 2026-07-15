DELETE FROM notification_deliveries d
USING notifications duplicate, notifications retained
WHERE d.notification_id = duplicate.id
  AND duplicate.user_id = retained.user_id
  AND duplicate.message_id = retained.message_id
  AND duplicate.kind = retained.kind
  AND duplicate.message_id IS NOT NULL
  AND duplicate.id > retained.id;

DELETE FROM notifications duplicate
USING notifications retained
WHERE duplicate.user_id = retained.user_id
  AND duplicate.message_id = retained.message_id
  AND duplicate.kind = retained.kind
  AND duplicate.message_id IS NOT NULL
  AND duplicate.id > retained.id;

CREATE UNIQUE INDEX IF NOT EXISTS notifications_recipient_message_kind_idx
    ON notifications (user_id, message_id, kind);
