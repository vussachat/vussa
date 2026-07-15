/**
 * @param {string} previousDraft
 * @param {string} nextDraft
 * @returns {boolean|null}
 */
export function typingSignal(previousDraft, nextDraft) {
  if (previousDraft === nextDraft) return null;
  return Boolean(nextDraft.trim());
}
