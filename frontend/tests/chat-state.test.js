// @ts-nocheck

import test from 'node:test';
import assert from 'node:assert/strict';
import { initialChatState, reduceChatEvent } from '../src/lib/chat-state.js';

test('message retries are idempotent in the client state', () => {
  const message = { id: 'm1', created_at: 2, text: 'hello' };
  let state = reduceChatEvent(initialChatState(), { type: 'message', message });
  state = reduceChatEvent(state, { type: 'message', message: { ...message, text: 'hello' } });
  assert.equal(state.messages.length, 1);
});

test('message updates replace the visible message', () => {
  const message = { id: 'm1', created_at: 1, text: 'before' };
  let state = reduceChatEvent(initialChatState(), { type: 'message', message });
  state = reduceChatEvent(state, { type: 'message_updated', message: { ...message, text: 'after', edited: true } });
  assert.equal(state.messages[0].text, 'after');
  assert.equal(state.messages[0].edited, true);
});

test('typing and reactions are removed by negative events', () => {
  let state = initialChatState();
  state = reduceChatEvent(state, { type: 'typing', user_id: 'u1', username: 'A', typing: true });
  state = reduceChatEvent(state, { type: 'reaction_updated', reaction: { message_id: 'm1', emoji: '👍', user_ids: ['u1'] } });
  state = reduceChatEvent(state, { type: 'typing', user_id: 'u1', username: 'A', typing: false });
  state = reduceChatEvent(state, { type: 'reaction_updated', reaction: { message_id: 'm1', emoji: '👍', user_ids: [] } });
  assert.deepEqual(state.typingUsers, []);
  assert.deepEqual(state.reactions, []);
});

test('thread history accepts live replies without disturbing the main timeline', () => {
  const root = { id: 'root', created_at: 1, root_message_id: null };
  const reply = { id: 'reply', created_at: 2, root_message_id: 'root' };
  let state = reduceChatEvent(initialChatState(), { type: 'message', message: root });
  state = reduceChatEvent(state, { type: 'thread_history', root_message_id: 'root', messages: [] });
  state = reduceChatEvent(state, { type: 'message', message: reply });
  assert.deepEqual(state.messages.map((item) => item.id), ['root', 'reply']);
  assert.deepEqual(state.threadMessages.map((item) => item.id), ['reply']);
  assert.equal(state.threadRoot, 'root');
  assert.equal(state.messages[0].reply_count, 1);
});
