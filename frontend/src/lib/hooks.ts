/**
 * React Query hooks for API calls.
 */

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import * as api from "./api";
import type { ChatRequest, ChatResponse, ArchiveSearchResponse, ArchivePageResponse, SettingsResponse, HealthResponse } from "./types";

// Re-export the chat stream hook for convenience
export { useChatStream } from "./use-chat-stream";
export type { StreamCallbacks, StreamResult, UseChatStreamReturn } from "./use-chat-stream";

// ============================================================================
// Query Keys
// ============================================================================

export const queryKeys = {
  health: ["health"] as const,
  settings: ["settings"] as const,
  archiveSearch: (query: string) => ["archive", "search", query] as const,
  archivePage: (urlHash: string) => ["archive", "page", urlHash] as const,
};

// ============================================================================
// Health Hooks
// ============================================================================

export function useHealth() {
  return useQuery<HealthResponse>({
    queryKey: queryKeys.health,
    queryFn: api.getHealth,
    refetchInterval: 30000, // Refresh every 30 seconds
    staleTime: 10000,
  });
}

// ============================================================================
// Settings Hooks
// ============================================================================

export function useSettings() {
  return useQuery<SettingsResponse>({
    queryKey: queryKeys.settings,
    queryFn: api.getSettings,
    staleTime: 60000, // Settings don't change often
  });
}

// ============================================================================
// Chat Hooks
// ============================================================================

export function useChatMutation() {
  return useMutation<ChatResponse, Error, ChatRequest>({
    mutationFn: api.sendChatMessage,
  });
}

// ============================================================================
// Archive Hooks
// ============================================================================

export function useArchiveSearch(query: string = "", enabled: boolean = true) {
  return useQuery<ArchiveSearchResponse>({
    queryKey: queryKeys.archiveSearch(query),
    queryFn: () => api.searchArchive(query),
    enabled,
    staleTime: 30000,
  });
}

export function useArchivePage(urlHash: string, enabled: boolean = true) {
  return useQuery<ArchivePageResponse>({
    queryKey: queryKeys.archivePage(urlHash),
    queryFn: () => api.getArchivePage(urlHash),
    enabled: enabled && !!urlHash,
    staleTime: 60000,
  });
}

// ============================================================================
// Invalidation Helpers
// ============================================================================

export function useInvalidateArchive() {
  const queryClient = useQueryClient();
  
  return () => {
    queryClient.invalidateQueries({ queryKey: ["archive"] });
  };
}
