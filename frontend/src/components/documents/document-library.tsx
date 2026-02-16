/**
 * DocumentLibrary component for uploading and managing documents.
 * Supports PDF, XLSX, and XLS files.
 */

import { useState, useRef, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Button } from "../ui/button";
import { Card } from "../ui/card";
import {
  listDocuments,
  uploadDocument,
  deleteDocument,
} from "../../lib/api";
import type { Document, DocumentStatus } from "../../lib/types";

// ============================================================================
// Helper Functions
// ============================================================================

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(isoString: string): string {
  const date = new Date(isoString);
  return date.toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function getStatusColor(status: DocumentStatus): string {
  switch (status) {
    case "ready":
      return "bg-green-100 text-green-800";
    case "processing":
      return "bg-yellow-100 text-yellow-800";
    case "pending":
      return "bg-blue-100 text-blue-800";
    case "error":
      return "bg-red-100 text-red-800";
    default:
      return "bg-gray-100 text-gray-800";
  }
}

function getDocTypeIcon(docType: string): string {
  switch (docType) {
    case "pdf":
      return "📄";
    case "xlsx":
    case "xls":
      return "📊";
    default:
      return "📁";
  }
}

// ============================================================================
// Document Item Component
// ============================================================================

interface DocumentItemProps {
  document: Document;
  onDelete: (id: string) => void;
  isDeleting: boolean;
}

function DocumentItem({ document, onDelete, isDeleting }: DocumentItemProps) {
  return (
    <div className="flex items-center justify-between p-4 border-b border-gray-100 last:border-b-0 hover:bg-gray-50">
      <div className="flex items-center gap-3 flex-1 min-w-0">
        <span className="text-2xl">{getDocTypeIcon(document.doc_type)}</span>
        <div className="flex-1 min-w-0">
          <p className="font-medium text-gray-900 truncate" title={document.filename}>
            {document.filename}
          </p>
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <span>{formatFileSize(document.size_bytes)}</span>
            <span>•</span>
            <span>{formatDate(document.uploaded_at)}</span>
            {document.chunk_count > 0 && (
              <>
                <span>•</span>
                <span>{document.chunk_count} chunks</span>
              </>
            )}
          </div>
          {document.error_message && (
            <p className="text-sm text-red-600 mt-1">{document.error_message}</p>
          )}
        </div>
      </div>
      
      <div className="flex items-center gap-3">
        <span
          className={`px-2 py-1 text-xs font-medium rounded-full ${getStatusColor(document.status)}`}
        >
          {document.status}
        </span>
        <Button
          variant="outline"
          size="sm"
          onClick={() => onDelete(document.document_id)}
          disabled={isDeleting}
          className="text-red-600 hover:text-red-700 hover:bg-red-50"
        >
          {isDeleting ? "..." : "Delete"}
        </Button>
      </div>
    </div>
  );
}

// ============================================================================
// Upload Zone Component
// ============================================================================

interface UploadZoneProps {
  onUpload: (files: FileList) => void;
  isUploading: boolean;
}

function UploadZone({ onUpload, isUploading }: UploadZoneProps) {
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  
  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);
  
  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);
  
  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);
      
      if (e.dataTransfer.files.length > 0) {
        onUpload(e.dataTransfer.files);
      }
    },
    [onUpload]
  );
  
  const handleFileSelect = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files && e.target.files.length > 0) {
        onUpload(e.target.files);
        // Reset input so same file can be selected again
        e.target.value = "";
      }
    },
    [onUpload]
  );
  
  return (
    <div
      className={`
        border-2 border-dashed rounded-lg p-8 text-center transition-colors
        ${isDragging ? "border-blue-500 bg-blue-50" : "border-gray-300 hover:border-gray-400"}
        ${isUploading ? "opacity-50 pointer-events-none" : "cursor-pointer"}
      `}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
      onClick={() => fileInputRef.current?.click()}
    >
      <input
        ref={fileInputRef}
        type="file"
        accept=".pdf,.xlsx,.xls"
        multiple
        className="hidden"
        onChange={handleFileSelect}
        disabled={isUploading}
      />
      
      <div className="text-4xl mb-3">📤</div>
      
      {isUploading ? (
        <p className="text-gray-600">Uploading...</p>
      ) : (
        <>
          <p className="text-gray-700 font-medium mb-1">
            Drop files here or click to upload
          </p>
          <p className="text-sm text-gray-500">
            Supports PDF, XLSX, and XLS files (max 25MB)
          </p>
        </>
      )}
    </div>
  );
}

// ============================================================================
// Main DocumentLibrary Component
// ============================================================================

export function DocumentLibrary() {
  const queryClient = useQueryClient();
  const [deletingId, setDeletingId] = useState<string | null>(null);
  
  // Fetch documents list
  const {
    data: documentsData,
    isLoading,
    error,
  } = useQuery({
    queryKey: ["documents"],
    queryFn: listDocuments,
    // Only poll when there are documents being processed
    refetchInterval: (query) => {
      const docs = query.state.data?.documents ?? [];
      const hasProcessing = docs.some(
        (d) => d.status === "pending" || d.status === "processing"
      );
      return hasProcessing ? 2000 : false;
    },
  });
  
  // Upload mutation
  const uploadMutation = useMutation({
    mutationFn: uploadDocument,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["documents"] });
    },
  });
  
  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: deleteDocument,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["documents"] });
      setDeletingId(null);
    },
    onError: () => {
      setDeletingId(null);
    },
  });
  
  const handleUpload = useCallback(
    async (files: FileList) => {
      // Upload files sequentially
      for (const file of Array.from(files)) {
        try {
          await uploadMutation.mutateAsync(file);
        } catch (err) {
          console.error(`Failed to upload ${file.name}:`, err);
        }
      }
    },
    [uploadMutation]
  );
  
  const handleDelete = useCallback(
    (documentId: string) => {
      setDeletingId(documentId);
      deleteMutation.mutate(documentId);
    },
    [deleteMutation]
  );
  
  const documents = documentsData?.documents ?? [];
  const readyCount = documents.filter((d) => d.status === "ready").length;
  
  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 mb-2">Document Library</h1>
        <p className="text-gray-600">
          Upload PDF and Excel files to chat with your data. Documents are processed
          and indexed for retrieval.
        </p>
      </div>
      
      {/* Upload Zone */}
      <div className="mb-6">
        <UploadZone
          onUpload={handleUpload}
          isUploading={uploadMutation.isPending}
        />
        
        {uploadMutation.isError && (
          <p className="mt-2 text-sm text-red-600">
            Upload failed: {(uploadMutation.error as Error)?.message || "Unknown error"}
          </p>
        )}
      </div>
      
      {/* Documents List */}
      <Card className="overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-200 bg-gray-50">
          <div className="flex items-center justify-between">
            <h2 className="font-medium text-gray-900">
              Uploaded Documents
              {documents.length > 0 && (
                <span className="ml-2 text-sm text-gray-500">
                  ({readyCount} ready, {documents.length} total)
                </span>
              )}
            </h2>
          </div>
        </div>
        
        {isLoading ? (
          <div className="p-8 text-center text-gray-500">Loading documents...</div>
        ) : error ? (
          <div className="p-8 text-center text-red-600">
            Failed to load documents: {(error as Error)?.message || "Unknown error"}
          </div>
        ) : documents.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            No documents uploaded yet. Upload a PDF or Excel file to get started.
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {documents.map((doc) => (
              <DocumentItem
                key={doc.document_id}
                document={doc}
                onDelete={handleDelete}
                isDeleting={deletingId === doc.document_id}
              />
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}
