// @ts-nocheck

import test from 'node:test';
import assert from 'node:assert/strict';
import { applyMention, mentionQuery } from '../src/lib/mentions.js';

test('mention query only activates on the unfinished final token', () => {
  assert.equal(mentionQuery('hello @ali'), 'ali');
  assert.equal(mentionQuery('@'), '');
  assert.equal(mentionQuery('hello @ali there'), null);
  assert.equal(mentionQuery('email@alias'), null);
});

test('applying a mention preserves the message prefix and adds spacing', () => {
  assert.equal(applyMention('hello @ali', 'alice'), 'hello @alice ');
  assert.equal(applyMention('@ali', 'alice'), '@alice ');
});

test('special channel and online mention names are valid mention targets', () => {
  assert.equal(applyMention('notify @ch', 'channel'), 'notify @channel ');
  assert.equal(applyMention('notify @he', 'here'), 'notify @here ');
});
