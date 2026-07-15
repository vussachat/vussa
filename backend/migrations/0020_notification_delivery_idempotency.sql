DELETE FROM notification_deliveries duplicate
USING notification_deliveries retained
WHERE duplicate.notification_id = retained.notification_id
  AND duplicate.channel = retained.channel
  AND duplicate.id > retained.id;

CREATE UNIQUE INDEX IF NOT EXISTS notification_deliveries_notification_channel_idx
    ON notification_deliveries (notification_id, channel);
