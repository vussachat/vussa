const HTML_ESCAPE = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;'
};

/**
 * Escape server-provided search text while allowing only the controlled mark
 * tags emitted by PostgreSQL full-text highlighting.
 */
/** @param {unknown} value */
export function safeSearchHighlight(value) {
  const escaped = String(value ?? '').replace(/[&<>"']/g, (character) => HTML_ESCAPE[/** @type {keyof typeof HTML_ESCAPE} */ (character)]);
  return escaped.replace(/&lt;mark&gt;([\s\S]*?)&lt;\/mark&gt;/g, '<mark>$1</mark>');
}
