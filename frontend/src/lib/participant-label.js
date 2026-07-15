/**
 * @param {{ username: string, display_name?: string }} participant
 * @returns {string}
 */
export function participantDisplayName(participant) {
  return participant.display_name?.trim() || participant.username;
}

/**
 * @param {{ custom_status?: string, roles?: string[] }} participant
 * @returns {string}
 */
export function participantSecondaryLabel(participant) {
  return participant.custom_status?.trim() || participant.roles?.join(' · ') || 'user';
}
