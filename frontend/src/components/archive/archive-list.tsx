import { useState, useCallback } from "react";
import { Search, ExternalLink, Clock, FileText } from "lucide-react";
import { Input } from "../ui/input";
import { Card, CardContent } from "../ui/card";
import { useArchiveSearch } from "../../lib/hooks";
import { formatDate, extractDomain } from "../../lib/utils";
import type { ArchiveEntry } from "../../lib/types";

interface ArchiveListProps {
  onSelectEntry: (entry: ArchiveEntry) => void;
  selectedUrlHash?: string;
}

export function ArchiveList({ onSelectEntry, selectedUrlHash }: ArchiveListProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  
  const { data, isLoading, error } = useArchiveSearch(debouncedQuery);
  
  // Debounce search with useCallback
  const handleSearchChange = useCallback((value: string) => {
    const timeoutId = setTimeout(() => {
      setDebouncedQuery(value);
    }, 300);
    return () => clearTimeout(timeoutId);
  }, []);
  
  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setSearchQuery(value);
    handleSearchChange(value);
  };
  
  return (
    <div className="flex flex-col h-full">
      {/* Search Header */}
      <div className="p-4 border-b border-gray-200">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
          <Input
            type="text"
            placeholder="Search archive..."
            value={searchQuery}
            onChange={handleInputChange}
            className="pl-10"
          />
        </div>
        {data && (
          <p className="mt-2 text-xs text-gray-500">
            {data.total} {data.total === 1 ? "entry" : "entries"} found
          </p>
        )}
      </div>
      
      {/* Results */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3">
        {isLoading && (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
          </div>
        )}
        
        {error && (
          <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
            Failed to load archive: {error.message}
          </div>
        )}
        
        {data && data.entries.length === 0 && (
          <div className="text-center py-8">
            <FileText className="h-12 w-12 text-gray-300 mx-auto mb-3" />
            <p className="text-gray-500">
              {searchQuery ? "No matching entries found" : "Archive is empty"}
            </p>
          </div>
        )}
        
        {data?.entries.map((entry) => (
          <Card
            key={entry.url_hash}
            className={`cursor-pointer transition-colors hover:border-primary-300 ${
              selectedUrlHash === entry.url_hash ? "border-primary-500 bg-primary-50" : ""
            }`}
            onClick={() => onSelectEntry(entry)}
          >
            <CardContent className="p-3">
              <div className="flex items-start gap-3">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <ExternalLink className="h-4 w-4 text-gray-400 flex-shrink-0" />
                    <span className="text-sm font-medium text-gray-900 truncate">
                      {extractDomain(entry.url)}
                    </span>
                  </div>
                  <p className="text-xs text-gray-400 truncate mb-2">{entry.url}</p>
                  <p className="text-sm text-gray-600 line-clamp-2">{entry.excerpt}</p>
                  <div className="flex items-center gap-1 mt-2 text-xs text-gray-400">
                    <Clock className="h-3 w-3" />
                    {formatDate(entry.timestamp)}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
