// @ts-nocheck

import test from 'node:test';
import assert from 'node:assert/strict';
import { participantDisplayName, participantSecondaryLabel } from '../src/lib/participant-label.js';

test('participant labels prefer profile display name and active status', () => {
  const participant = { username: 'alice', display_name: 'Alice Example', custom_status: 'Reviewing', roles: ['user'] };
  assert.equal(participantDisplayName(participant), 'Alice Example');
  assert.equal(participantSecondaryLabel(participant), 'Reviewing');
});

test('participant labels fall back for legacy presence payloads', () => {
  const participant = { username: 'alice', roles: ['moderator'] };
  assert.equal(participantDisplayName(participant), 'alice');
  assert.equal(participantSecondaryLabel(participant), 'moderator');
});
