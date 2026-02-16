import { ExternalLink, FileText } from "lucide-react";
import type { Source } from "../../lib/types";
import { extractDomain } from "../../lib/utils";
import { cn } from "../../lib/utils";

interface SourceChipProps {
  source: Source;
  onClick?: () => void;
  isSelected?: boolean;
}

function getSourceLabel(source: Source): string {
  // Check if it's a document source
  if (source.source_type === "document" || source.url.startsWith("doc://")) {
    if (source.filename) {
      // Add location info if available
      const locationParts: string[] = [];
      if (source.location?.page) {
        locationParts.push(`p${source.location.page}`);
      }
      if (source.location?.sheet) {
        locationParts.push(source.location.sheet);
      }
      if (source.location?.row_start && source.location?.row_end) {
        locationParts.push(`r${source.location.row_start}-${source.location.row_end}`);
      }
      
      const locationStr = locationParts.length > 0 ? ` (${locationParts.join(", ")})` : "";
      return source.filename + locationStr;
    }
    return "Document";
  }
  
  // Web source - show domain
  return extractDomain(source.url);
}

export function SourceChip({ source, onClick, isSelected }: SourceChipProps) {
  const isDocument = source.source_type === "document" || source.url.startsWith("doc://");
  const label = getSourceLabel(source);
  
  return (
    <button
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium transition-colors",
        "border border-gray-200 hover:border-primary-300 hover:bg-primary-50",
        isSelected && "border-primary-500 bg-primary-50 text-primary-700",
        isDocument && "border-green-200 hover:border-green-300 hover:bg-green-50",
        isDocument && isSelected && "border-green-500 bg-green-50 text-green-700"
      )}
    >
      {isDocument ? (
        <FileText className="h-3 w-3 text-green-500" />
      ) : (
        <ExternalLink className="h-3 w-3 text-gray-400" />
      )}
      <span className="max-w-[180px] truncate">{label}</span>
    </button>
  );
}
