// @ts-nocheck
import test from 'node:test';
import assert from 'node:assert/strict';
import { renderSafeMarkdown } from '../src/lib/safe-markdown.js';

test('renders basic markdown and only safe web links', () => {
  const html = renderSafeMarkdown('**hello** `code` https://example.com/a');
  assert.match(html, /<strong>hello<\/strong>/);
  assert.match(html, /<code>code<\/code>/);
  assert.match(html, /rel="noopener noreferrer nofollow"/);
});

test('escapes markup and does not create unsafe links', () => {
  const html = renderSafeMarkdown('<script>alert(1)</script> javascript:alert(1)');
  assert.match(html, /&lt;script&gt;alert\(1\)&lt;\/script&gt;/);
  assert.doesNotMatch(html, /<script|href="javascript:/i);
});
