// @ts-nocheck

import test from 'node:test';
import assert from 'node:assert/strict';
import { typingSignal } from '../src/lib/typing.js';

test('typing signal starts for non-empty draft changes and stops when cleared', () => {
  assert.equal(typingSignal('', 'hello'), true);
  assert.equal(typingSignal('hello', '   '), false);
  assert.equal(typingSignal('same', 'same'), null);
});
