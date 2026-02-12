import { ExternalLink, Clock, Copy, Check } from "lucide-react";
import { useState } from "react";
import { Button } from "../ui/button";
import { useArchivePage } from "../../lib/hooks";
import { formatDate, extractDomain } from "../../lib/utils";

interface ArchiveDetailProps {
  urlHash: string | null;
}

export function ArchiveDetail({ urlHash }: ArchiveDetailProps) {
  const [copied, setCopied] = useState(false);
  const { data, isLoading, error } = useArchivePage(urlHash || "", !!urlHash);
  
  if (!urlHash) {
    return (
      <div className="flex-1 flex items-center justify-center p-8 bg-gray-50">
        <p className="text-gray-500 text-center">
          Select an entry from the list to view its content
        </p>
      </div>
    );
  }
  
  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center p-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>
    );
  }
  
  if (error) {
    return (
      <div className="flex-1 p-8">
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          Failed to load page: {error.message}
        </div>
      </div>
    );
  }
  
  if (!data) {
    return null;
  }
  
  const handleCopy = async () => {
    await navigator.clipboard.writeText(data.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  
  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <div className="p-4 border-b border-gray-200 bg-white">
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h2 className="text-lg font-semibold text-gray-900 truncate">
              {extractDomain(data.url)}
            </h2>
            <a
              href={data.url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-sm text-primary-600 hover:text-primary-700 flex items-center gap-1 mt-1"
            >
              <ExternalLink className="h-3 w-3" />
              <span className="truncate">{data.url}</span>
            </a>
            <div className="flex items-center gap-1 mt-2 text-xs text-gray-400">
              <Clock className="h-3 w-3" />
              Archived: {formatDate(data.timestamp)}
            </div>
          </div>
          <div className="flex gap-2 flex-shrink-0">
            <Button variant="outline" size="sm" onClick={handleCopy}>
              {copied ? (
                <>
                  <Check className="h-4 w-4 mr-1" />
                  Copied
                </>
              ) : (
                <>
                  <Copy className="h-4 w-4 mr-1" />
                  Copy
                </>
              )}
            </Button>
            <a href={data.url} target="_blank" rel="noopener noreferrer">
              <Button variant="outline" size="sm">
                <ExternalLink className="h-4 w-4 mr-1" />
                Open
              </Button>
            </a>
          </div>
        </div>
      </div>
      
      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 bg-gray-50">
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <pre className="whitespace-pre-wrap text-sm text-gray-700 font-sans">
            {data.content}
          </pre>
        </div>
      </div>
      
      {/* Footer */}
      <div className="p-4 border-t border-gray-200 bg-white">
        <p className="text-xs text-gray-400">
          Archive ID: <code className="font-mono">{data.url_hash}</code>
        </p>
      </div>
    </div>
  );
}
