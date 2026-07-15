// @ts-nocheck

const mentionPattern = /(?:^|\s)@([a-zA-Z0-9_-]{0,32})$/;

export function mentionQuery(value) {
  const match = value.match(mentionPattern);
  return match ? match[1] : null;
}

export function applyMention(value, username) {
  return value.replace(mentionPattern, (match, query) => `${match.slice(0, match.length - query.length)}${username} `);
}
