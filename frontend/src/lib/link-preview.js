const URL_PATTERN = /https?:\/\/[^\s<>"']+/gi;

/**
 * @param {string} text
 * @returns {string[]}
 */
export function extractPreviewUrls(text) {
  return [...new Set((text.match(URL_PATTERN) ?? []).map((value) => value.replace(/[),.;!?]+$/, '')))].slice(0, 3);
}
