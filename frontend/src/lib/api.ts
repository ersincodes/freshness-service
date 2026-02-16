/**
 * API client for the Freshness Service backend.
 * Provides typed functions for all API endpoints.
 */

import type {
  ChatRequest,
  ChatResponse,
  ChatStreamMetaEvent,
  ChatStreamTokenEvent,
  ChatStreamDoneEvent,
  ChatStreamErrorEvent,
  ArchiveSearchResponse,
  ArchivePageResponse,
  SettingsResponse,
  HealthResponse,
  Document,
  DocumentListResponse,
  DocumentUploadResponse,
} from "./types";

// ============================================================================
// Configuration
// ============================================================================

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

// ============================================================================
// Error Handling
// ============================================================================

export class ApiError extends Error {
  code: string;
  
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
    this.name = "ApiError";
  }
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let errorData: { code?: string; message?: string; detail?: { code?: string; message?: string } | string } = {};
    try {
      errorData = await response.json();
    } catch {
      // Ignore JSON parse errors
    }
    
    const code = errorData.detail && typeof errorData.detail === "object" 
      ? errorData.detail.code 
      : errorData.code || "API_ERROR";
    const message = errorData.detail && typeof errorData.detail === "object"
      ? errorData.detail.message
      : typeof errorData.detail === "string"
        ? errorData.detail
        : errorData.message || `Request failed with status ${response.status}`;
    
    throw new ApiError(code || "API_ERROR", message || "Unknown error");
  }
  
  return response.json();
}

// ============================================================================
// Chat API
// ============================================================================

/**
 * Send a chat message and receive a complete response.
 */
export async function sendChatMessage(request: ChatRequest): Promise<ChatResponse> {
  const response = await fetch(`${API_BASE_URL}/api/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });
  
  return handleResponse<ChatResponse>(response);
}

/**
 * Stream chat response via Server-Sent Events.
 * 
 * @param request - Chat request
 * @param callbacks - Event callbacks for different event types
 * @returns AbortController to cancel the stream
 */
export function streamChatMessage(
  request: ChatRequest,
  callbacks: {
    onMeta?: (data: ChatStreamMetaEvent) => void;
    onToken?: (data: ChatStreamTokenEvent) => void;
    onDone?: (data: ChatStreamDoneEvent) => void;
    onError?: (data: ChatStreamErrorEvent) => void;
  }
): AbortController {
  const controller = new AbortController();
  
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
        return;
      }
      
      const reader = response.body?.getReader();
      if (!reader) {
        callbacks.onError?.({
          code: "NO_BODY",
          message: "Response body is empty",
        });
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
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        // Stream was intentionally aborted
        return;
      }
      
      callbacks.onError?.({
        code: "STREAM_ERROR",
        message: error instanceof Error ? error.message : "Unknown error",
      });
    }
  })();
  
  return controller;
}

// ============================================================================
// Archive API
// ============================================================================

/**
 * Search the archive for pages matching the query.
 */
export async function searchArchive(
  query: string = "",
  limit: number = 20,
  cursor?: string
): Promise<ArchiveSearchResponse> {
  const params = new URLSearchParams();
  if (query) params.set("q", query);
  params.set("limit", limit.toString());
  if (cursor) params.set("cursor", cursor);
  
  const response = await fetch(`${API_BASE_URL}/api/archive/search?${params}`);
  return handleResponse<ArchiveSearchResponse>(response);
}

/**
 * Get detailed view of an archived page.
 */
export async function getArchivePage(urlHash: string): Promise<ArchivePageResponse> {
  const response = await fetch(`${API_BASE_URL}/api/archive/page/${urlHash}`);
  return handleResponse<ArchivePageResponse>(response);
}

// ============================================================================
// Settings API
// ============================================================================

/**
 * Get current settings (non-secret values only).
 */
export async function getSettings(): Promise<SettingsResponse> {
  const response = await fetch(`${API_BASE_URL}/api/settings`);
  return handleResponse<SettingsResponse>(response);
}

// ============================================================================
// Health API
// ============================================================================

/**
 * Check health of all services.
 */
export async function getHealth(): Promise<HealthResponse> {
  const response = await fetch(`${API_BASE_URL}/api/health`);
  return handleResponse<HealthResponse>(response);
}

// ============================================================================
// Documents API
// ============================================================================

/**
 * Upload a document (PDF, XLSX, XLS) for processing.
 */
export async function uploadDocument(file: File): Promise<DocumentUploadResponse> {
  const formData = new FormData();
  formData.append("file", file);
  
  const response = await fetch(`${API_BASE_URL}/api/documents/upload`, {
    method: "POST",
    body: formData,
  });
  
  return handleResponse<DocumentUploadResponse>(response);
}

/**
 * List all uploaded documents.
 */
export async function listDocuments(): Promise<DocumentListResponse> {
  const response = await fetch(`${API_BASE_URL}/api/documents`);
  return handleResponse<DocumentListResponse>(response);
}

/**
 * Get status and details of a specific document.
 */
export async function getDocument(documentId: string): Promise<Document> {
  const response = await fetch(`${API_BASE_URL}/api/documents/${documentId}`);
  return handleResponse<Document>(response);
}

/**
 * Delete a document and all its chunks.
 */
export async function deleteDocument(documentId: string): Promise<{ status: string; message: string }> {
  const response = await fetch(`${API_BASE_URL}/api/documents/${documentId}`, {
    method: "DELETE",
  });
  return handleResponse<{ status: string; message: string }>(response);
}
