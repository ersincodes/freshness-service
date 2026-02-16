/**
 * Chat state management using React Context.
 * Manages conversations, messages, and streaming state.
 * 
 * Architecture:
 * - Uses useReducer for predictable state updates
 * - Delegates streaming to useChatStream hook
 * - Uses refs to avoid stale closure issues in async callbacks
 * - Persists conversations to localStorage
 */

import {
  createContext,
  useContext,
  useReducer,
  useCallback,
  useRef,
  useEffect,
  type ReactNode,
} from "react";
import type {
  ChatTurn,
  Conversation,
  Source,
  RetrievalMode,
  PreferredChatMode,
} from "../lib/types";
import { generateId, storage } from "../lib/utils";
import { sendChatMessage } from "../lib/api";
import { useChatStream } from "../lib/use-chat-stream";

// ============================================================================
// State Types
// ============================================================================

interface ChatState {
  conversations: Conversation[];
  activeConversationId: string | null;
  isLoading: boolean;
  error: string | null;
  preferredMode: PreferredChatMode;
  // Document integration options
  includeWeb: boolean;
  includeDocuments: boolean;
  selectedDocumentIds: string[];
}

type ChatAction =
  | { type: "SET_CONVERSATIONS"; payload: Conversation[] }
  | { type: "SET_ACTIVE_CONVERSATION"; payload: string | null }
  | { type: "CREATE_CONVERSATION"; payload: Conversation }
  | { type: "DELETE_CONVERSATION"; payload: string }
  | { type: "ADD_TURN"; payload: { conversationId: string; turn: ChatTurn } }
  | { type: "UPDATE_TURN"; payload: { conversationId: string; turnId: string; updates: Partial<ChatTurn> } }
  | { type: "APPEND_TO_TURN"; payload: { conversationId: string; turnId: string; content: string } }
  | { type: "SET_LOADING"; payload: boolean }
  | { type: "SET_ERROR"; payload: string | null }
  | { type: "SET_PREFERRED_MODE"; payload: PreferredChatMode }
  | { type: "CLEAR_CONVERSATION"; payload: string }
  | { type: "SET_INCLUDE_WEB"; payload: boolean }
  | { type: "SET_INCLUDE_DOCUMENTS"; payload: boolean }
  | { type: "SET_SELECTED_DOCUMENT_IDS"; payload: string[] };

// ============================================================================
// Storage Keys
// ============================================================================

const STORAGE_KEY = "freshness-chat-conversations";

// ============================================================================
// Reducer
// ============================================================================

function chatReducer(state: ChatState, action: ChatAction): ChatState {
  switch (action.type) {
    case "SET_CONVERSATIONS":
      return { ...state, conversations: action.payload };

    case "SET_ACTIVE_CONVERSATION":
      return { ...state, activeConversationId: action.payload };

    case "CREATE_CONVERSATION":
      return {
        ...state,
        conversations: [action.payload, ...state.conversations],
        activeConversationId: action.payload.id,
      };

    case "DELETE_CONVERSATION": {
      const filtered = state.conversations.filter((c) => c.id !== action.payload);
      return {
        ...state,
        conversations: filtered,
        activeConversationId:
          state.activeConversationId === action.payload
            ? filtered[0]?.id || null
            : state.activeConversationId,
      };
    }

    case "ADD_TURN": {
      return {
        ...state,
        conversations: state.conversations.map((c) =>
          c.id === action.payload.conversationId
            ? {
                ...c,
                turns: [...c.turns, action.payload.turn],
                updated_at: new Date().toISOString(),
              }
            : c
        ),
      };
    }

    case "UPDATE_TURN": {
      return {
        ...state,
        conversations: state.conversations.map((c) =>
          c.id === action.payload.conversationId
            ? {
                ...c,
                turns: c.turns.map((t) =>
                  t.id === action.payload.turnId
                    ? { ...t, ...action.payload.updates }
                    : t
                ),
                updated_at: new Date().toISOString(),
              }
            : c
        ),
      };
    }

    case "APPEND_TO_TURN": {
      return {
        ...state,
        conversations: state.conversations.map((c) =>
          c.id === action.payload.conversationId
            ? {
                ...c,
                turns: c.turns.map((t) =>
                  t.id === action.payload.turnId
                    ? { ...t, content: t.content + action.payload.content }
                    : t
                ),
              }
            : c
        ),
      };
    }

    case "SET_LOADING":
      return { ...state, isLoading: action.payload };

    case "SET_ERROR":
      return { ...state, error: action.payload };

    case "SET_PREFERRED_MODE":
      return { ...state, preferredMode: action.payload };

    case "CLEAR_CONVERSATION": {
      return {
        ...state,
        conversations: state.conversations.map((c) =>
          c.id === action.payload
            ? { ...c, turns: [], updated_at: new Date().toISOString() }
            : c
        ),
      };
    }

    case "SET_INCLUDE_WEB":
      return { ...state, includeWeb: action.payload };

    case "SET_INCLUDE_DOCUMENTS":
      return { ...state, includeDocuments: action.payload };

    case "SET_SELECTED_DOCUMENT_IDS":
      return { ...state, selectedDocumentIds: action.payload };

    default:
      return state;
  }
}

// ============================================================================
// Context
// ============================================================================

interface ChatContextValue {
  state: ChatState;
  activeConversation: Conversation | null;
  createConversation: () => void;
  deleteConversation: (id: string) => void;
  setActiveConversation: (id: string | null) => void;
  sendMessage: (content: string, useStreaming?: boolean) => Promise<void>;
  stopStreaming: () => void;
  clearConversation: (id: string) => void;
  setPreferredMode: (mode: PreferredChatMode) => void;
  // Document integration
  setIncludeWeb: (include: boolean) => void;
  setIncludeDocuments: (include: boolean) => void;
  setSelectedDocumentIds: (ids: string[]) => void;
}

const ChatContext = createContext<ChatContextValue | null>(null);

// ============================================================================
// Provider
// ============================================================================

interface ChatProviderProps {
  children: ReactNode;
}

export function ChatProvider({ children }: ChatProviderProps) {
  const [state, dispatch] = useReducer(chatReducer, {
    conversations: storage.get<Conversation[]>(STORAGE_KEY, []),
    activeConversationId: null,
    isLoading: false,
    error: null,
    preferredMode: "ONLINE",
    includeWeb: true,
    includeDocuments: false,
    selectedDocumentIds: [],
  });

  // Use ref to always have access to latest state in async callbacks
  // This fixes the stale closure bug where callbacks captured old state
  const stateRef = useRef(state);
  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  // Streaming hook
  const { startStream, abortStream } = useChatStream();

  // Persist conversations to localStorage whenever they change
  useEffect(() => {
    storage.set(STORAGE_KEY, state.conversations);
  }, [state.conversations]);

  // Get active conversation
  const activeConversation =
    state.conversations.find((c) => c.id === state.activeConversationId) || null;

  // Create new conversation
  const createConversation = useCallback(() => {
    const conversation: Conversation = {
      id: generateId(),
      title: "New Chat",
      turns: [],
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    dispatch({ type: "CREATE_CONVERSATION", payload: conversation });
  }, []);

  // Delete conversation
  const deleteConversation = useCallback((id: string) => {
    dispatch({ type: "DELETE_CONVERSATION", payload: id });
  }, []);

  // Set active conversation
  const setActiveConversation = useCallback((id: string | null) => {
    dispatch({ type: "SET_ACTIVE_CONVERSATION", payload: id });
  }, []);

  // Stop streaming
  const stopStreaming = useCallback(() => {
    abortStream();
    dispatch({ type: "SET_LOADING", payload: false });
  }, [abortStream]);

  const setPreferredMode = useCallback((mode: PreferredChatMode) => {
    dispatch({ type: "SET_PREFERRED_MODE", payload: mode });
  }, []);

  const setIncludeWeb = useCallback((include: boolean) => {
    dispatch({ type: "SET_INCLUDE_WEB", payload: include });
  }, []);

  const setIncludeDocuments = useCallback((include: boolean) => {
    dispatch({ type: "SET_INCLUDE_DOCUMENTS", payload: include });
  }, []);

  const setSelectedDocumentIds = useCallback((ids: string[]) => {
    dispatch({ type: "SET_SELECTED_DOCUMENT_IDS", payload: ids });
  }, []);

  // Send message
  const sendMessage = useCallback(
    async (content: string, useStreaming: boolean = true) => {
      if (!content.trim()) return;

      // Create conversation if none exists
      let conversationId = stateRef.current.activeConversationId;
      if (!conversationId) {
        const conversation: Conversation = {
          id: generateId(),
          title: content.slice(0, 50) + (content.length > 50 ? "..." : ""),
          turns: [],
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        };
        dispatch({ type: "CREATE_CONVERSATION", payload: conversation });
        conversationId = conversation.id;
      }

      // Add user message
      const userTurn: ChatTurn = {
        id: generateId(),
        role: "user",
        content,
        created_at: new Date().toISOString(),
      };
      dispatch({ type: "ADD_TURN", payload: { conversationId, turn: userTurn } });

      // Create assistant turn placeholder
      const assistantTurnId = generateId();
      const assistantTurn: ChatTurn = {
        id: assistantTurnId,
        role: "assistant",
        content: "",
        created_at: new Date().toISOString(),
        isStreaming: useStreaming,
      };
      dispatch({ type: "ADD_TURN", payload: { conversationId, turn: assistantTurn } });

      dispatch({ type: "SET_LOADING", payload: true });
      dispatch({ type: "SET_ERROR", payload: null });

      // Capture conversationId and assistantTurnId for callbacks
      // These are stable values that won't change during streaming
      const targetConversationId = conversationId;
      const targetTurnId = assistantTurnId;

      try {
        if (useStreaming) {
          // Streaming mode using the dedicated hook
          startStream(
            {
              query: content,
              conversation_id: targetConversationId,
              prefer_mode: stateRef.current.preferredMode,
              include_web: stateRef.current.includeWeb,
              include_documents: stateRef.current.includeDocuments,
              document_ids: stateRef.current.selectedDocumentIds.length > 0
                ? stateRef.current.selectedDocumentIds
                : undefined,
            },
            {
              onMeta: (data) => {
                dispatch({
                  type: "UPDATE_TURN",
                  payload: {
                    conversationId: targetConversationId,
                    turnId: targetTurnId,
                    updates: {
                      mode: data.mode as RetrievalMode,
                      sources: data.sources as Source[],
                    },
                  },
                });
              },
              onToken: (data) => {
                dispatch({
                  type: "APPEND_TO_TURN",
                  payload: {
                    conversationId: targetConversationId,
                    turnId: targetTurnId,
                    content: data.text,
                  },
                });
              },
              onDone: () => {
                dispatch({
                  type: "UPDATE_TURN",
                  payload: {
                    conversationId: targetConversationId,
                    turnId: targetTurnId,
                    updates: { isStreaming: false },
                  },
                });
                dispatch({ type: "SET_LOADING", payload: false });
              },
              onError: (data) => {
                dispatch({
                  type: "UPDATE_TURN",
                  payload: {
                    conversationId: targetConversationId,
                    turnId: targetTurnId,
                    updates: {
                      isStreaming: false,
                      error: { code: data.code, message: data.message },
                      content: `Error: ${data.message}`,
                    },
                  },
                });
                dispatch({ type: "SET_LOADING", payload: false });
                dispatch({ type: "SET_ERROR", payload: data.message });
              },
            }
          );
        } else {
          // Non-streaming mode
          const response = await sendChatMessage({
            query: content,
            conversation_id: targetConversationId,
            prefer_mode: stateRef.current.preferredMode,
            include_web: stateRef.current.includeWeb,
            include_documents: stateRef.current.includeDocuments,
            document_ids: stateRef.current.selectedDocumentIds.length > 0
              ? stateRef.current.selectedDocumentIds
              : undefined,
          });

          dispatch({
            type: "UPDATE_TURN",
            payload: {
              conversationId: targetConversationId,
              turnId: targetTurnId,
              updates: {
                content: response.answer,
                mode: response.mode,
                sources: response.sources,
                isStreaming: false,
              },
            },
          });

          dispatch({ type: "SET_LOADING", payload: false });
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown error";
        dispatch({
          type: "UPDATE_TURN",
          payload: {
            conversationId: targetConversationId,
            turnId: targetTurnId,
            updates: {
              isStreaming: false,
              error: { code: "ERROR", message },
              content: `Error: ${message}`,
            },
          },
        });
        dispatch({ type: "SET_LOADING", payload: false });
        dispatch({ type: "SET_ERROR", payload: message });
      }
    },
    [startStream]
  );

  // Clear conversation
  const clearConversation = useCallback((id: string) => {
    dispatch({ type: "CLEAR_CONVERSATION", payload: id });
  }, []);

  const value: ChatContextValue = {
    state,
    activeConversation,
    createConversation,
    deleteConversation,
    setActiveConversation,
    sendMessage,
    stopStreaming,
    clearConversation,
    setPreferredMode,
    setIncludeWeb,
    setIncludeDocuments,
    setSelectedDocumentIds,
  };

  return <ChatContext.Provider value={value}>{children}</ChatContext.Provider>;
}

// ============================================================================
// Hook
// ============================================================================

export function useChat() {
  const context = useContext(ChatContext);
  if (!context) {
    throw new Error("useChat must be used within a ChatProvider");
  }
  return context;
}
