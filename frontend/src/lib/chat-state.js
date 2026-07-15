// @ts-nocheck

export function initialChatState() {
  return { messages: [], reactions: [], typingUsers: [], threadRoot: null, threadMessages: [], status: 'disconnected' };
}

function upsert(messages, message) {
  const index = messages.findIndex((item) => item.id === message.id);
  if (index < 0) return [...messages, message].sort((a, b) => a.created_at - b.created_at);
  return messages.map((item, offset) => offset === index ? { ...item, ...message } : item);
}

export function reduceChatEvent(state, event) {
  switch (event.type) {
    case 'message': {
      const existingReply = event.message.root_message_id
        ? state.messages.some((item) => item.id === event.message.id)
        : false;
      const messages = upsert(state.messages, event.message);
      const updatedMessages = event.message.root_message_id && !existingReply
        ? messages.map((item) => item.id === event.message.root_message_id
          ? { ...item, reply_count: (item.reply_count ?? 0) + 1 }
          : item)
        : messages;
      return {
        ...state,
        messages: updatedMessages,
        threadMessages: state.threadRoot && event.message.root_message_id === state.threadRoot
          ? upsert(state.threadMessages, event.message)
          : state.threadMessages,
      };
    }
    case 'message_updated':
      return { ...state, messages: upsert(state.messages, event.message) };
    case 'reaction_updated': {
      const without = state.reactions.filter((item) => !(item.message_id === event.reaction.message_id && item.emoji === event.reaction.emoji));
      return { ...state, reactions: event.reaction.user_ids.length ? [...without, event.reaction] : without };
    }
    case 'typing': {
      const without = state.typingUsers.filter((item) => item.user_id !== event.user_id);
      return { ...state, typingUsers: event.typing ? [...without, { user_id: event.user_id, username: event.username }] : without };
    }
    case 'thread_history':
      return { ...state, threadRoot: event.root_message_id, threadMessages: event.messages ?? [] };
    case 'joined':
      return { ...state, status: 'connected' };
    default:
      return state;
  }
}
