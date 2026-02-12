/**
 * Custom hook for handling SSE chat streaming.
 * Encapsulates all streaming logic including connection management,
 * event parsing, and abort handling.
 */

import { useRef, useCallback } from "react";
import type {
  ChatRequest,
  ChatStreamMetaEvent,
  ChatStreamTokenEvent,
  ChatStreamDoneEvent,
  ChatStreamErrorEvent,
  RetrievalMode,
  Source,
} from "./types";

// ============================================================================
// Types
// ============================================================================

export interface StreamCallbacks {
  onMeta?: (data: ChatStreamMetaEvent) => void;
  onToken?: (data: ChatStreamTokenEvent) => void;
  onDone?: (data: ChatStreamDoneEvent) => void;
  onError?: (data: ChatStreamErrorEvent) => void;
}

export interface StreamResult {
  mode?: RetrievalMode;
  sources?: Source[];
  content: string;
  error?: { code: string; message: string };
}

export interface UseChatStreamReturn {
  /** Start streaming a chat message */
  startStream: (
    request: ChatRequest,
    callbacks: StreamCallbacks
  ) => void;
  /** Abort the current stream */
  abortStream: () => void;
  /** Whether a stream is currently active */
  isStreaming: boolean;
}

// ============================================================================
// Configuration
// ============================================================================

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

// ============================================================================
// Hook Implementation
// ============================================================================

export function useChatStream(): UseChatStreamReturn {
  const controllerRef = useRef<AbortController | null>(null);
  const isStreamingRef = useRef(false);

  const abortStream = useCallback(() => {
    if (controllerRef.current) {
      controllerRef.current.abort();
      controllerRef.current = null;
    }
    isStreamingRef.current = false;
  }, []);

  const startStream = useCallback(
    (request: ChatRequest, callbacks: StreamCallbacks) => {
      // Abort any existing stream
      abortStream();

      const controller = new AbortController();
      controllerRef.current = controller;
      isStreamingRef.current = true;

      (async () => {
        try {
          const response = await fetch(`${API_BASE_URL}/api/chat/stream`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(request),
            signal: controller.signal,
          });

          if (!response.ok) {
            const errorText = await response.text();
            callbacks.onError?.({
              code: "HTTP_ERROR",
              message: `Request failed: ${response.status} ${errorText}`,
            });
            isStreamingRef.current = false;
            controllerRef.current = null;
            return;
          }

          const reader = response.body?.getReader();
          if (!reader) {
            callbacks.onError?.({
              code: "NO_BODY",
              message: "Response body is empty",
            });
            isStreamingRef.current = false;
            controllerRef.current = null;
            return;
          }

          const decoder = new TextDecoder();
          let buffer = "";

          while (true) {
            const { done, value } = await reader.read();

            if (done) break;

            buffer += decoder.decode(value, { stream: true });

            // Process complete events from buffer
            const lines = buffer.split("\n");
            buffer = lines.pop() || ""; // Keep incomplete line in buffer

            let currentEvent = "";

            for (const line of lines) {
              if (line.startsWith("event: ")) {
                currentEvent = line.slice(7).trim();
              } else if (line.startsWith("data: ")) {
                const dataStr = line.slice(6);
                try {
                  const data = JSON.parse(dataStr);

                  switch (currentEvent) {
                    case "meta":
                      callbacks.onMeta?.(data as ChatStreamMetaEvent);
                      break;
                    case "token":
                      callbacks.onToken?.(data as ChatStreamTokenEvent);
                      break;
                    case "done":
                      callbacks.onDone?.(data as ChatStreamDoneEvent);
                      break;
                    case "error":
                      callbacks.onError?.(data as ChatStreamErrorEvent);
                      break;
                  }
                } catch {
                  // Ignore JSON parse errors for incomplete data
                }
              }
            }
          }

          // Stream completed naturally
          isStreamingRef.current = false;
          controllerRef.current = null;
        } catch (error) {
          if (error instanceof Error && error.name === "AbortError") {
            // Stream was intentionally aborted
            isStreamingRef.current = false;
            controllerRef.current = null;
            return;
          }

          callbacks.onError?.({
            code: "STREAM_ERROR",
            message: error instanceof Error ? error.message : "Unknown error",
          });
          isStreamingRef.current = false;
          controllerRef.current = null;
        }
      })();
    },
    [abortStream]
  );

  return {
    startStream,
    abortStream,
    get isStreaming() {
      return isStreamingRef.current;
    },
  };
}
