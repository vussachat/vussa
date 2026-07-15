#!/usr/bin/env bash
set -euo pipefail

# Requires DATABASE_URL and VALKEY_URL. CI supplies managed service
# containers; local users can point these variables at disposable instances.
base_url="${BASE_URL:-http://127.0.0.1:3300}"
tmp="$(mktemp -d)"
pid=""
cleanup() {
  [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

if [[ "${START_SERVER:-true}" == "true" ]]; then
  BIND_ADDRESS=127.0.0.1:3300 cargo run --quiet --package vussa >"$tmp/server.log" 2>&1 &
  pid=$!
fi

for _ in {1..60}; do
  curl --fail --silent "$base_url/api/v1/health/live" >/dev/null && break
  sleep 1
done
curl --fail --silent "$base_url/api/v1/health/ready" >/dev/null

email="smoke-${RANDOM}@example.com"
username="smoke${RANDOM}"
password='smoke-password-2026'
register_response="$(curl --fail --silent --show-error -D "$tmp/register.headers" -c "$tmp/cookies" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/auth/register" \
  -d "{\"email\":\"$email\",\"username\":\"$username\",\"password\":\"$password\"}")"
user_id="$(printf '%s' "$register_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
test -n "$user_id"
csrf="$(awk 'tolower($1)=="x-csrf-token:" {print $2}' "$tmp/register.headers" | tr -d '\r' | tail -1)"
test -n "$csrf"
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/auth/me" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/channels" >/dev/null
public_channel_name="smoke-public-${RANDOM}"
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/channels" \
  -d "{\"name\":\"$public_channel_name\"}" | grep -q "$public_channel_name"
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/channels" | grep -q "$public_channel_name"
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/unread" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/metrics" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/notifications/config" | grep -q 'vapid_public_key'
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/favorites" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/messages/saved" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X POST "$base_url/api/v1/channels/main/favorite" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/favorites" | grep -q 'main'
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X DELETE "$base_url/api/v1/channels/main/favorite" >/dev/null
reports_status="$(curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/cookies" "$base_url/api/v1/moderation/reports")"
if [[ "$reports_status" != "403" ]]; then
  echo "ordinary users must not access the moderation queue (got HTTP $reports_status)" >&2
  exit 1
fi
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X PATCH "$base_url/api/v1/notifications/preferences" \
  -d '{"mentions":true,"direct_messages":true,"channel_messages":false,"email_enabled":false,"browser_push_enabled":false}' >/dev/null
printf 'attachment smoke payload\n' >"$tmp/attachment.txt"
upload_response="$(curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -F "file=@$tmp/attachment.txt;type=text/plain" "$base_url/api/v1/files")"
file_id="$(printf '%s' "$upload_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
test -n "$file_id"
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/files/$file_id" >"$tmp/downloaded.txt"
cmp "$tmp/attachment.txt" "$tmp/downloaded.txt"
other_email="other-${RANDOM}@example.com"
other_username="other${RANDOM}"
other_register_response="$(curl --fail --silent --show-error -D "$tmp/other-register.headers" -c "$tmp/other.cookies" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/auth/register" \
  -d "{\"email\":\"$other_email\",\"username\":\"$other_username\",\"password\":\"$password\"}")"
other_user_id="$(printf '%s' "$other_register_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
test -n "$other_user_id"
other_csrf="$(awk 'tolower($1)=="x-csrf-token:" {print $2}' "$tmp/other-register.headers" | tr -d '\r' | tail -1)"
test -n "$other_csrf"
curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/users/search?q=$other_username" | grep -q "$other_username"
cookie_value="$(awk '$6 == "vussa_session" { print $7 }' "$tmp/cookies")"
test -n "$cookie_value"
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -H 'content-type: application/json' -X PATCH "$base_url/api/v1/notifications/preferences" \
  -d '{"mentions":true,"direct_messages":true,"channel_messages":true,"email_enabled":false,"browser_push_enabled":false}' >/dev/null
special_mention_message_id="$(SMOKE_KEEP_MESSAGE=true SMOKE_MESSAGE_TEXT='@channel special mention smoke' \
  SMOKE_EDIT_TEXT='@channel special mention smoke edited' \
  python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$cookie_value")"
test -n "$special_mention_message_id"
curl --fail --silent --show-error -b "$tmp/other.cookies" \
  "$base_url/api/v1/notifications?limit=50" | grep -q 'mention'
private_name="smoke-private-${RANDOM}"
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/channels/private" \
  -d "{\"name\":\"$private_name\"}" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/messages/search?q=created%20this%20private%20channel" | grep -q '"username":"system"'
invite_response="$(curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/channels/$private_name/invite-links" \
  -d '{"expires_at":4102444800000,"max_uses":1}')"
invite_token="$(printf '%s' "$invite_response" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
test -n "$invite_token"
for _ in 1 2; do
  curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/channels/$private_name/members" \
    -d "{\"user_id\":\"$other_user_id\"}" >/dev/null
done
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -X POST "$base_url/api/v1/invite-links/$invite_token/accept" | grep -q "$private_name"
curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/channels/$private_name/members" | grep -q "$other_username"
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X POST "$base_url/api/v1/channels/$private_name/members/$other_user_id/moderator" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X DELETE "$base_url/api/v1/channels/$private_name/members/$other_user_id/moderator" >/dev/null
owner_leave_status="$(curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/cookies" -H "x-csrf-token: $csrf" -X DELETE "$base_url/api/v1/channels/$private_name/membership")"
if [[ "$owner_leave_status" != "400" ]]; then
  echo "private channel owner unexpectedly left with HTTP $owner_leave_status" >&2
  exit 1
fi
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -H 'content-type: application/json' -X PATCH "$base_url/api/v1/notifications/preferences" \
  -d '{"mentions":true,"direct_messages":true,"channel_messages":false,"email_enabled":false,"browser_push_enabled":false}' >/dev/null
private_message_id="$(SMOKE_CHANNEL="$private_name" SMOKE_KEEP_MESSAGE=true SMOKE_MESSAGE_TEXT="private membership smoke message" \
  python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$cookie_value")"
test -n "$private_message_id"
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -X POST "$base_url/api/v1/messages/$private_message_id/save" >/dev/null
curl --fail --silent --show-error -b "$tmp/other.cookies" "$base_url/api/v1/messages/saved" | grep -q "$private_message_id"
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -X POST "$base_url/api/v1/channels/$private_name/favorite" >/dev/null
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -X DELETE "$base_url/api/v1/channels/$private_name/membership" >/dev/null
if curl --fail --silent --show-error -b "$tmp/other.cookies" "$base_url/api/v1/conversations" | grep -q "$private_name"; then
  echo "a member who left still received the private conversation" >&2
  exit 1
fi
if curl --fail --silent --show-error -b "$tmp/other.cookies" "$base_url/api/v1/messages/saved" | grep -q "$private_message_id"; then
  echo "a member who left still received a saved private message" >&2
  exit 1
fi
if curl --fail --silent --show-error -b "$tmp/other.cookies" "$base_url/api/v1/favorites" | grep -q "$private_name"; then
  echo "a member who left still received a private favorite" >&2
  exit 1
fi
if curl --fail --silent --show-error -b "$tmp/other.cookies" \
  "$base_url/api/v1/notifications?limit=50" | grep -q "$private_name"; then
  echo "a departed private-channel member received a notification" >&2
  exit 1
fi
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/channels/$private_name/members" \
  -d "{\"user_id\":\"$other_user_id\"}" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X DELETE "$base_url/api/v1/channels/$private_name/members/$other_user_id" >/dev/null
direct_response="$(curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/conversations/direct" \
  -d "{\"user_id\":\"$other_user_id\"}")"
direct_name="$(printf '%s' "$direct_response" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')"
direct_id="$(printf '%s' "$direct_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
test -n "$direct_id" -a -n "$direct_name"
if curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/other.cookies" \
  "$base_url/api/v1/files/$file_id" | grep -q '^2'; then
  echo "unlinked attachment was readable by another account" >&2
  exit 1
fi
subscription_response="$(curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/notifications/subscriptions" \
  -d '{"endpoint":"https://push.example.test/subscription","p256dh":"0123456789abcdef","auth":"01234567"}')"
subscription_id="$(printf '%s' "$subscription_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
test -n "$subscription_id"
curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/notifications/subscriptions" | grep -q 'push.example.test'
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X DELETE "$base_url/api/v1/notifications/subscriptions/$subscription_id" >/dev/null
ws_management_channel="$private_name"
ws_management_channel_name="smoke-ws-channel-${RANDOM}"
ws_management_private_name="smoke-ws-private-${RANDOM}"
message_id="$(SMOKE_FILE_ID="$file_id" SMOKE_KEEP_MESSAGE=true SMOKE_MESSAGE_TEXT="@$other_username websocket smoke message" \
  SMOKE_MANAGEMENT_CHANNEL="$ws_management_channel" SMOKE_MANAGEMENT_USER_ID="$other_user_id" \
  SMOKE_CREATE_CHANNEL_NAME="$ws_management_channel_name" \
  SMOKE_CREATE_PRIVATE_CHANNEL_NAME="$ws_management_private_name" \
  SMOKE_OPEN_DIRECT_USER_ID="$other_user_id" \
  python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$cookie_value")"
test -n "$message_id"
SMOKE_KEEP_MESSAGE=true SMOKE_MESSAGE_TEXT='search-pagination-token first' \
  SMOKE_EDIT_TEXT='search-pagination-token first edited' \
  python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$cookie_value" >/dev/null
SMOKE_KEEP_MESSAGE=true SMOKE_MESSAGE_TEXT='search-pagination-token second' \
  SMOKE_EDIT_TEXT='search-pagination-token second edited' \
  python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$cookie_value" >/dev/null
search_page="$(curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/messages/search?q=search-pagination-token&channel=main&limit=1")"
printf '%s' "$search_page" | grep -q 'search-pagination-token'
printf '%s' "$search_page" | grep -q '<mark>'
search_before_created_at="$(printf '%s' "$search_page" | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["next"]["before_created_at"])')"
search_before_id="$(printf '%s' "$search_page" | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["next"]["before_id"])')"
curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/messages/search?q=search-pagination-token&channel=main&limit=1&before_created_at=$search_before_created_at&before_id=$search_before_id" \
  | grep -q 'search-pagination-token'
message_payload="$(curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/messages/$message_id")"
printf '%s' "$message_payload" | grep -q '"metadata"'
printf '%s' "$message_payload" | grep -q "\"$other_username\""
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X POST "$base_url/api/v1/messages/$message_id/save" >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/messages/saved" | grep -q "$message_id"
curl --fail --silent --show-error -b "$tmp/cookies" \
  "$base_url/api/v1/messages/saved" | grep -q "$file_id"
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X DELETE "$base_url/api/v1/messages/$message_id/save" >/dev/null
curl --fail --silent --show-error -b "$tmp/other.cookies" \
  "$base_url/api/v1/notifications?limit=50" | grep -q 'mention'
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -H 'content-type: application/json' -X PATCH "$base_url/api/v1/notifications/preferences" \
  -d '{"mentions":true,"direct_messages":true,"channel_messages":true,"email_enabled":false,"browser_push_enabled":false}' >/dev/null
channel_message_id="$(SMOKE_MESSAGE_TEXT="channel preference smoke message" python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$cookie_value")"
test -n "$channel_message_id"
curl --fail --silent --show-error -b "$tmp/other.cookies" \
  "$base_url/api/v1/notifications?limit=50" | grep -q 'channel_message'
direct_message_id="$(SMOKE_CHANNEL="$direct_name" SMOKE_MESSAGE_TEXT="direct preference smoke message" \
  python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$cookie_value")"
test -n "$direct_message_id"
curl --fail --silent --show-error -b "$tmp/other.cookies" \
  "$base_url/api/v1/notifications?limit=50" | grep -q 'direct_message'
notification_id="$(curl --fail --silent --show-error -b "$tmp/other.cookies" \
  "$base_url/api/v1/notifications?limit=50" | python3 -c \
  'import json,sys; items=json.load(sys.stdin); print(items[0]["id"])')"
test -n "$notification_id"
curl --fail --silent --show-error -b "$tmp/other.cookies" -H "x-csrf-token: $other_csrf" \
  -X POST "$base_url/api/v1/notifications/$notification_id/read" >/dev/null
curl --fail --silent --show-error -b "$tmp/other.cookies" \
  "$base_url/api/v1/notifications?limit=50" | grep -q "$notification_id"
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/messages/$message_id/permalink" | grep -q '"url"'
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/reports" \
  -d "{\"message_id\":\"$message_id\",\"reason\":\"integration smoke report\"}" >/dev/null
if curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/cookies" \
  "$base_url/api/v1/link-preview?url=http%3A%2F%2F127.0.0.1%3A8080" | grep -q '^2'; then
  echo "private link preview target was accepted" >&2
  exit 1
fi
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/account/sessions" >/dev/null
curl --fail --silent --show-error -H 'content-type: application/json' -X POST \
  "$base_url/api/v1/auth/recovery/request" \
  -d '{"email":"does-not-exist@example.com"}' | grep -q 'If the account exists'
if curl --silent --output /dev/null --write-out '%{http_code}' -H 'content-type: application/json' -X POST \
  "$base_url/api/v1/auth/recovery/reset" -d '{"token":"invalid","password":"new-password-2026"}' | grep -q '^2'; then
  echo "invalid recovery token was accepted" >&2
  exit 1
fi
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X PATCH "$base_url/api/v1/profile" \
  -d '{"display_name":"Smoke Test","custom_status":"online"}' >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/profile" | grep -q 'Smoke Test'
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/profile" | grep -q 'online'
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/account/export" | grep -q '"user"'
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X PUT "$base_url/api/v1/drafts/main" \
  -d '{"body":"persisted smoke draft"}' >/dev/null
curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/drafts/main" | grep -q 'persisted smoke draft'
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X DELETE "$base_url/api/v1/drafts/main" >/dev/null
curl --fail --silent --show-error -D "$tmp/second-login.headers" -c "$tmp/second.cookies" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/auth/login" \
  -d "{\"email\":\"$email\",\"password\":\"$password\"}" >/dev/null
second_session_id="$(curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/account/sessions" | \
  python3 -c 'import json,sys; print(next(item["id"] for item in json.load(sys.stdin) if not item["current"]))')"
test -n "$second_session_id"
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -X DELETE "$base_url/api/v1/account/sessions/$second_session_id" >/dev/null
second_status="$(curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/second.cookies" \
  "$base_url/api/v1/auth/me")"
if [[ "$second_status" != "401" ]]; then
  echo "revoked account session remained authenticated (HTTP $second_status)" >&2
  exit 1
fi
new_password='smoke-password-changed-2026'
curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
  -H 'content-type: application/json' -X PATCH "$base_url/api/v1/account/password" \
  -d "{\"current_password\":\"$password\",\"new_password\":\"$new_password\"}" >/dev/null
old_password_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/auth/login" \
  -d "{\"email\":\"$email\",\"password\":\"$password\"}")"
if [[ "$old_password_status" != "401" ]]; then
  echo "old account password remained valid after password rotation (HTTP $old_password_status)" >&2
  exit 1
fi
curl --fail --silent --show-error -H 'content-type: application/json' -X POST "$base_url/api/v1/auth/login" \
  -d "{\"email\":\"$email\",\"password\":\"$new_password\"}" >/dev/null
delete_email="delete-${RANDOM}@example.com"
delete_username="delete${RANDOM}"
curl --fail --silent --show-error -D "$tmp/delete-register.headers" -c "$tmp/delete.cookies" \
  -H 'content-type: application/json' -X POST "$base_url/api/v1/auth/register" \
  -d "{\"email\":\"$delete_email\",\"username\":\"$delete_username\",\"password\":\"$password\"}" >/dev/null
delete_csrf="$(awk 'tolower($1)=="x-csrf-token:" {print $2}' "$tmp/delete-register.headers" | tr -d '\r' | tail -1)"
test -n "$delete_csrf"
curl --fail --silent --show-error -b "$tmp/delete.cookies" -H "x-csrf-token: $delete_csrf" \
  -X DELETE "$base_url/api/v1/account" >/dev/null
delete_status="$(curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/delete.cookies" \
  "$base_url/api/v1/auth/me")"
if [[ "$delete_status" != "401" ]]; then
  echo "deleted account session remained authenticated (HTTP $delete_status)" >&2
  exit 1
fi
if [[ -n "${ADMIN_EMAIL:-}" && -n "${ADMIN_PASSWORD:-}" ]]; then
  curl --fail --silent --show-error -D "$tmp/admin.headers" -c "$tmp/admin.cookies" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/auth/login" \
    -d "{\"email\":\"$ADMIN_EMAIL\",\"password\":\"$ADMIN_PASSWORD\"}" >/dev/null
  admin_csrf="$(awk 'tolower($1)=="x-csrf-token:" {print $2}' "$tmp/admin.headers" | tr -d '\r' | tail -1)"
  test -n "$admin_csrf"
  admin_cookie_value="$(awk '$6 == "vussa_session" { print $7 }' "$tmp/admin.cookies")"
  test -n "$admin_cookie_value"
  ws_admin_channel_name="smoke-ws-admin-${RANDOM}"
  SMOKE_KEEP_MESSAGE=true SMOKE_CREATE_CHANNEL_NAME="$ws_admin_channel_name" \
    SMOKE_DELETE_CHANNEL_NAME="$ws_admin_channel_name" \
    python3 scripts/websocket-smoke.py "$base_url/api/v1/ws" "$admin_cookie_value" >/dev/null
  for admin_endpoint in users roles permissions audit operations channels messages bans participants/main; do
    curl --fail --silent --show-error -b "$tmp/admin.cookies" \
      "$base_url/api/v1/admin/$admin_endpoint" >/dev/null
  done
  public_channel_id="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/channels" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); print(next(item["id"] for item in data["items"] if item["name"] == sys.argv[1]))' \
    "$public_channel_name")"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=channel.created&target=$public_channel_id" | grep -q "$public_channel_id"
  private_channel_id="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/channels" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); print(next(item["id"] for item in data["items"] if item["name"] == sys.argv[1]))' \
    "$private_name")"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=channel.member_added&target=$private_channel_id" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); assert sum(1 for item in data["items"] if item["metadata"].get("user_id") == sys.argv[1]) == 1' \
    "$other_user_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=channel.member_left&target=$private_channel_id" | grep -q "$other_user_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=channel.member_removed&target=$private_channel_id" | grep -q "$other_user_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=channel.direct_created&target=$direct_id" | grep -q "$direct_id"
  admin_message_id="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/messages?channel=main" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); print(data["items"][0]["id"])')"
  test -n "$admin_message_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/messages/$admin_message_id/history" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/admin/messages/$admin_message_id/delete" \
    -d '{"reason":"single-message moderation smoke"}' >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/admin/messages/$admin_message_id/restore" \
    -d '{}' >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/admin/messages/bulk-moderate" \
    -d "{\"ids\":[\"$admin_message_id\"],\"action\":\"delete\",\"reason\":\"bulk moderation smoke\"}" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=message.delete&target=$admin_message_id" | grep -q "$admin_message_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/admin/messages/bulk-moderate" \
    -d "{\"ids\":[\"$admin_message_id\"],\"action\":\"restore\"}" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=message.restore&target=$admin_message_id" | grep -q "$admin_message_id"
  admin_channel_name="admin-smoke-${RANDOM}"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/admin/channels" \
    -d "{\"name\":\"$admin_channel_name\",\"description\":\"admin lifecycle smoke\",\"retention_days\":30}" >/dev/null
  admin_channel_id="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/channels" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); print(next(item["id"] for item in data["items"] if item["name"] == sys.argv[1]))' \
    "$admin_channel_name")"
  test -n "$admin_channel_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=channel.created&target=$admin_channel_id" | grep -q "$admin_channel_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/channels/$admin_channel_id/archive" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/channels/$admin_channel_id/restore" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X PATCH "$base_url/api/v1/admin/channels/$admin_channel_id" \
    -d '{"description":"updated admin lifecycle smoke","retention_days":14,"posting_restricted":true}' >/dev/null
  admin_channels_updated="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/channels")"
  printf '%s' "$admin_channels_updated" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); item=next(item for item in data["items"] if item["id"] == sys.argv[1]); assert item["description"] == "updated admin lifecycle smoke" and item["retention_days"] == 14 and item["posting_restricted"] is True' \
    "$admin_channel_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X PATCH "$base_url/api/v1/admin/channels/$admin_channel_id" \
    -d '{"posting_restricted":false}' >/dev/null
  admin_channels_reopened="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/channels")"
  printf '%s' "$admin_channels_reopened" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); assert next(item["posting_restricted"] for item in data["items"] if item["id"] == sys.argv[1]) is False' \
    "$admin_channel_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/channels/$admin_channel_id/delete" >/dev/null
  admin_channels_deleted="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/channels")"
  printf '%s' "$admin_channels_deleted" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); assert next(item["deleted_at"] for item in data["items"] if item["id"] == sys.argv[1]) is not None' \
    "$admin_channel_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/channels/$admin_channel_id/undelete" >/dev/null
  admin_channels_restored="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/channels")"
  printf '%s' "$admin_channels_restored" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); assert next(item["deleted_at"] for item in data["items"] if item["id"] == sys.argv[1]) is None' \
    "$admin_channel_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/users/$other_user_id/roles/moderator" >/dev/null
  admin_users_with_role="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/users")"
  printf '%s' "$admin_users_with_role" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); users=data.get("items",data); assert any(user["id"] == sys.argv[1] and "moderator" in user["roles"] for user in users)' \
    "$other_user_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X DELETE "$base_url/api/v1/admin/users/$other_user_id/roles/moderator" >/dev/null
  admin_users_without_role="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/users")"
  printf '%s' "$admin_users_without_role" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); users=data.get("items",data); assert any(user["id"] == sys.argv[1] and "moderator" not in user["roles"] for user in users)' \
    "$other_user_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/users/$other_user_id/disable" >/dev/null
  admin_users_disabled="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/users")"
  printf '%s' "$admin_users_disabled" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); users=data.get("items",data); assert next(user["disabled_at"] for user in users if user["id"] == sys.argv[1]) is not None' \
    "$other_user_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/users/$other_user_id/enable" >/dev/null
  admin_users_enabled="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/users")"
  printf '%s' "$admin_users_enabled" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); users=data.get("items",data); assert next(user["disabled_at"] for user in users if user["id"] == sys.argv[1]) is None' \
    "$other_user_id"
  open_reports="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/moderation/reports?status=open")"
  report_id="$(printf '%s' "$open_reports" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
  test -n "$report_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \\
    "$base_url/api/v1/admin/audit?action=report.created&target=$report_id" | grep -q "$report_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/moderation/reports/$report_id/resolve" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/moderation/reports/$report_id/reopen" >/dev/null
  ban_response="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/admin/bans" \
    -d "{\"user_id\":\"$user_id\",\"reason\":\"integration smoke ban\"}")"
  ban_id="$(printf '%s' "$ban_response" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
  test -n "$ban_id"
  if curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/channels" | grep -q 'main'; then
    echo "an actively banned account still received channel discovery" >&2
    exit 1
  fi
  if curl --fail --silent --show-error -b "$tmp/cookies" "$base_url/api/v1/unread" | grep -q 'main'; then
    echo "an actively banned account still received unread state" >&2
    exit 1
  fi
  if curl --fail --silent --show-error -b "$tmp/cookies" \
    "$base_url/api/v1/messages/search?q=websocket%20smoke%20message" | grep -q "$message_id"; then
    echo "an actively banned account still discovered protected messages through search" >&2
    exit 1
  fi
  for banned_creation in \
    "POST $base_url/api/v1/channels {\"name\":\"banned-public-${RANDOM}\"}" \
    "POST $base_url/api/v1/channels/private {\"name\":\"banned-private-${RANDOM}\"}" \
    "POST $base_url/api/v1/conversations/direct {\"user_id\":\"$other_user_id\"}" \
    "POST $base_url/api/v1/channels/$private_name/invite-links {\"max_uses\":1}"; do
    set -- $banned_creation
    banned_status="$(curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/cookies" \
      -H "x-csrf-token: $csrf" -H 'content-type: application/json' -X "$1" "$2" -d "$3")"
    if [[ "$banned_status" != "403" ]]; then
      echo "globally banned account could create a conversation or invite (HTTP $banned_status)" >&2
      exit 1
    fi
  done
  for protected_url in \
    "$base_url/api/v1/messages/$message_id" \
    "$base_url/api/v1/messages/$message_id/permalink" \
    "$base_url/api/v1/files/$file_id"; do
    protected_status="$(curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/cookies" "$protected_url")"
    if [[ "$protected_status" == 2* ]]; then
      echo "an actively banned account accessed protected resource $protected_url" >&2
      exit 1
    fi
  done
  if curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
    -F "file=@$tmp/attachment.txt;type=text/plain" "$base_url/api/v1/files" | grep -q '^2'; then
    echo "an actively globally banned account uploaded a file" >&2
    exit 1
  fi
  admin_reset_password='admin-reset-password-2026'
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -H 'content-type: application/json' -X POST "$base_url/api/v1/admin/users/$other_user_id/password-reset" \
    -d "{\"password\":\"$admin_reset_password\"}" >/dev/null
  curl --fail --silent --show-error -H 'content-type: application/json' -X POST \
    "$base_url/api/v1/auth/login" -d "{\"email\":\"$other_email\",\"password\":\"$admin_reset_password\"}" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X DELETE "$base_url/api/v1/admin/bans/$ban_id" >/dev/null
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=user.ban_revoked&target=$user_id" | grep -q "$user_id"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X DELETE "$base_url/api/v1/admin/users/$other_user_id" >/dev/null
  admin_users_after_delete="$(curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/users")"
  printf '%s' "$admin_users_after_delete" | python3 -c \
    'import json,sys; data=json.load(sys.stdin); users=data.get("items",data); assert not any(user["id"] == sys.argv[1] for user in users)' \
    "$other_user_id"
  renamed_username="smoke-renamed-${RANDOM}"
  curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
    -H 'content-type: application/json' -X PATCH "$base_url/api/v1/account" \
    -d "{\"username\":\"$renamed_username\"}" | grep -q "$renamed_username"
  curl --fail --silent --show-error -b "$tmp/admin.cookies" \
    "$base_url/api/v1/admin/audit?action=user.username_changed&target=$user_id" | grep -q "$renamed_username"
fi
if [[ -z "${ADMIN_EMAIL:-}" || -z "${ADMIN_PASSWORD:-}" ]]; then
  curl --fail --silent --show-error -b "$tmp/cookies" -H "x-csrf-token: $csrf" \
    -X POST "$base_url/api/v1/auth/logout" >/dev/null
fi
if [[ -n "${ADMIN_EMAIL:-}" && -n "${ADMIN_PASSWORD:-}" ]]; then
  curl --fail --silent --show-error -b "$tmp/admin.cookies" -H "x-csrf-token: $admin_csrf" \
    -X POST "$base_url/api/v1/admin/users/$user_id/invalidate-sessions" >/dev/null
  invalidated_status=200
  for _ in {1..20}; do
    invalidated_status="$(curl --silent --output /dev/null --write-out '%{http_code}' -b "$tmp/cookies" "$base_url/api/v1/auth/me")"
    [[ "$invalidated_status" == "401" ]] && break
    sleep 0.25
  done
  if [[ "$invalidated_status" != "401" ]]; then
    echo "session invalidation did not revoke the user session (HTTP $invalidated_status)" >&2
    exit 1
  fi
fi
echo "integration smoke test passed"
