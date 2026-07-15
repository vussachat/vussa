// @ts-nocheck

import test from 'node:test';
import assert from 'node:assert/strict';
import { safeSearchHighlight } from '../src/lib/search-highlight.js';

test('preserves controlled search marks and escapes message text', () => {
  assert.equal(
    safeSearchHighlight('hello <mark>world</mark> <img src=x onerror=alert(1)>'),
    'hello <mark>world</mark> &lt;img src=x onerror=alert(1)&gt;'
  );
});

test('does not treat arbitrary tags as highlight markup', () => {
  assert.equal(safeSearchHighlight('<mark data-x="bad">term</mark>'), '&lt;mark data-x=&quot;bad&quot;&gt;term&lt;/mark&gt;');
});
