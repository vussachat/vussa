// @ts-nocheck

import test from 'node:test';
import assert from 'node:assert/strict';
import { extractPreviewUrls } from '../src/lib/link-preview.js';

test('preview URL extraction is bounded, deduplicated, and strips punctuation', () => {
  const urls = extractPreviewUrls('Read https://example.com/docs, https://example.com/docs and http://example.org/path! https://three.example/x https://four.example/y');
  assert.deepEqual(urls, ['https://example.com/docs', 'http://example.org/path', 'https://three.example/x']);
});

test('preview URL extraction ignores non-web text', () => {
  assert.deepEqual(extractPreviewUrls('file:///tmp/a and @user'), []);
});
