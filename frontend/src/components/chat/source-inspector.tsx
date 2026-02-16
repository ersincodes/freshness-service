import { X, ExternalLink, Clock, Database, Globe, Search, FileText } from "lucide-react";
import { Button } from "../ui/button";
import { Badge } from "../ui/badge";
import type { Source } from "../../lib/types";
import { formatDate, extractDomain } from "../../lib/utils";

interface SourceInspectorProps {
  source: Source | null;
  onClose: () => void;
}

export function SourceInspector({ source, onClose }: SourceInspectorProps) {
  if (!source) {
    return (
      <div className="w-80 border-l border-gray-200 bg-gray-50 p-4 flex items-center justify-center">
        <p className="text-sm text-gray-500 text-center">
          Click on a source to inspect it
        </p>
      </div>
    );
  }
  
  const isDocument = source.source_type === "document" || source.url.startsWith("doc://");
  
  const retrievalTypeConfig: Record<string, { label: string; icon: typeof Globe; variant: "success" | "warning" | "info" | "default" }> = {
    online: { label: "Online", icon: Globe, variant: "success" },
    offline_keyword: { label: "Keyword Search", icon: Search, variant: "warning" },
    offline_semantic: { label: "Semantic Search", icon: Database, variant: "info" },
    document_keyword: { label: "Document (Keyword)", icon: FileText, variant: "default" },
    document_semantic: { label: "Document (Semantic)", icon: FileText, variant: "info" },
  };
  
  const config = retrievalTypeConfig[source.retrieval_type] || retrievalTypeConfig.offline_keyword;
  const Icon = config.icon;
  
  return (
    <div className="w-80 border-l border-gray-200 bg-white flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-gray-200">
        <h3 className="font-semibold text-gray-900">Source Details</h3>
        <Button variant="ghost" size="icon" onClick={onClose}>
          <X className="h-4 w-4" />
        </Button>
      </div>
      
      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Source Info */}
        {isDocument ? (
          <div>
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
              Document
            </label>
            <div className="mt-1 flex items-center gap-2 text-sm text-green-600">
              <FileText className="h-4 w-4 flex-shrink-0" />
              {source.filename || "Unknown Document"}
            </div>
            {source.location && (
              <div className="mt-2 text-xs text-gray-500">
                {source.location.page && <span className="mr-2">Page {source.location.page}</span>}
                {source.location.sheet && <span className="mr-2">Sheet: {source.location.sheet}</span>}
                {source.location.row_start && source.location.row_end && (
                  <span>Rows {source.location.row_start}-{source.location.row_end}</span>
                )}
              </div>
            )}
          </div>
        ) : (
          <div>
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
              URL
            </label>
            <a
              href={source.url}
              target="_blank"
              rel="noopener noreferrer"
              className="mt-1 flex items-center gap-2 text-sm text-primary-600 hover:text-primary-700 break-all"
            >
              <ExternalLink className="h-4 w-4 flex-shrink-0" />
              {extractDomain(source.url)}
            </a>
            <p className="mt-1 text-xs text-gray-400 break-all">{source.url}</p>
          </div>
        )}
        
        {/* Retrieval Type */}
        <div>
          <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            Retrieval Type
          </label>
          <div className="mt-1">
            <Badge variant={config.variant} className="gap-1">
              <Icon className="h-3 w-3" />
              {config.label}
            </Badge>
          </div>
        </div>
        
        {/* Timestamp */}
        {source.timestamp && (
          <div>
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
              Retrieved At
            </label>
            <div className="mt-1 flex items-center gap-2 text-sm text-gray-700">
              <Clock className="h-4 w-4 text-gray-400" />
              {formatDate(source.timestamp)}
            </div>
          </div>
        )}
        
        {/* Snippet */}
        <div>
          <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            Content Snippet
          </label>
          <div className="mt-1 p-3 bg-gray-50 rounded-lg text-sm text-gray-700 max-h-64 overflow-y-auto">
            {source.snippet || "No snippet available"}
          </div>
        </div>
        
        {/* URL Hash */}
        {source.url_hash && (
          <div>
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
              Archive ID
            </label>
            <p className="mt-1 text-xs text-gray-400 font-mono">{source.url_hash}</p>
          </div>
        )}
      </div>
      
      {/* Footer */}
      {!isDocument && (
        <div className="p-4 border-t border-gray-200">
          <a
            href={source.url}
            target="_blank"
            rel="noopener noreferrer"
            className="block"
          >
            <Button variant="outline" className="w-full gap-2">
              <ExternalLink className="h-4 w-4" />
              Open in Browser
            </Button>
          </a>
        </div>
      )}
    </div>
  );
}
