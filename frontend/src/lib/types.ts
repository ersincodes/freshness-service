/**
 * TypeScript types matching backend Pydantic models.
 * These types define the API contract between frontend and backend.
 */

// ============================================================================
// Enums and Literals
// ============================================================================

export type RetrievalMode = "ONLINE" | "OFFLINE_ARCHIVE" | "LOCAL_WEIGHTS";
export type PreferredChatMode = "ONLINE" | "OFFLINE";

export type RetrievalType = "online" | "offline_keyword" | "offline_semantic";

export type HealthStatusType = "ok" | "error" | "unavailable";

export type OfflineRetrievalMode = "keyword" | "semantic";

// ============================================================================
// Source Types
// ============================================================================

export interface Source {
  url: string;
  snippet: string;
  retrieval_type: RetrievalType;
  timestamp?: string;
  url_hash?: string;
}

// ============================================================================
// Chat Types
// ============================================================================

export interface ChatRequest {
  query: string;
  conversation_id?: string;
  prefer_mode?: PreferredChatMode;
}

export interface TimingInfo {
  search_ms: number;
  scrape_ms: number;
  llm_ms: number;
  total_ms: number;
}

export interface ChatResponse {
  conversation_id: string;
  answer: string;
  mode: RetrievalMode;
  sources: Source[];
  timing: TimingInfo;
}

// SSE Event Types
export interface ChatStreamMetaEvent {
  mode: RetrievalMode;
  sources: Source[];
  conversation_id: string;
}

export interface ChatStreamTokenEvent {
  text: string;
}

export interface ChatStreamDoneEvent {
  final_text: string;
}

export interface ChatStreamErrorEvent {
  code: string;
  message: string;
}

// ============================================================================
// Chat Turn (Frontend State)
// ============================================================================

export interface ChatTurn {
  id: string;
  role: "user" | "assistant";
  content: string;
  created_at: string;
  mode?: RetrievalMode;
  sources?: Source[];
  error?: { code: string; message: string };
  isStreaming?: boolean;
}

export interface Conversation {
  id: string;
  title: string;
  turns: ChatTurn[];
  created_at: string;
  updated_at: string;
}

// ============================================================================
// Archive Types
// ============================================================================

export interface ArchiveEntry {
  url_hash: string;
  url: string;
  timestamp: string;
  excerpt: string;
}

export interface ArchiveSearchResponse {
  entries: ArchiveEntry[];
  total: number;
  cursor?: string;
}

export interface ArchivePageResponse {
  url_hash: string;
  url: string;
  content: string;
  timestamp: string;
}

// ============================================================================
// Settings Types
// ============================================================================

export interface SettingsResponse {
  brave_api_key_set: boolean;
  lm_studio_base_url: string;
  model_name: string;
  offline_retrieval_mode: OfflineRetrievalMode;
  max_search_results: number;
  request_timeout_s: number;
  max_chars_per_source: number;
  semantic_top_k: number;
}

// ============================================================================
// Health Types
// ============================================================================

export interface HealthStatus {
  status: HealthStatusType;
  message?: string;
  latency_ms?: number;
}

export interface HealthResponse {
  backend: HealthStatus;
  lm_studio: HealthStatus;
  brave_search: HealthStatus;
}

// ============================================================================
// Error Types
// ============================================================================

export interface ApiError {
  code: string;
  message: string;
}
