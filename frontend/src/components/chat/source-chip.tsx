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
  if (source.source_kind === "analytics") {
    const base = source.display_name || source.filename || "Analytics";
    return source.sheet_name ? `${base} · ${source.sheet_name}` : base;
  }
  // Check if it's a document source
  if (
    source.source_kind === "document" ||
    source.source_type === "document" ||
    source.url.startsWith("doc://")
  ) {
    const name = source.display_name || source.filename;
    if (name) {
      const locationParts: string[] = [];
      if (source.location?.page) {
        locationParts.push(`p${source.location.page}`);
      }
      if (source.location?.sheet) {
        locationParts.push(source.location.sheet);
      }
      if (source.sheet_name && !source.location?.sheet) {
        locationParts.push(source.sheet_name);
      }
      if (source.location?.row_start && source.location?.row_end) {
        locationParts.push(`r${source.location.row_start}-${source.location.row_end}`);
      }

      const locationStr = locationParts.length > 0 ? ` (${locationParts.join(", ")})` : "";
      return name + locationStr;
    }
    return "Document";
  }

  // Web source - show domain
  return extractDomain(source.url);
}

export function SourceChip({ source, onClick, isSelected }: SourceChipProps) {
  const isDocument =
    source.source_kind === "document" ||
    source.source_kind === "analytics" ||
    source.source_type === "document" ||
    source.url.startsWith("doc://");
  const isAnalytics = source.source_kind === "analytics";
  const label = getSourceLabel(source);
  const tip =
    source.document_id != null && source.document_id !== ""
      ? `ID: ${source.document_id}`
      : undefined;

  return (
    <button
      type="button"
      title={tip}
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium transition-colors",
        "border border-gray-200 hover:border-primary-300 hover:bg-primary-50",
        isSelected && "border-primary-500 bg-primary-50 text-primary-700",
        isDocument &&
          !isAnalytics &&
          "border-green-200 hover:border-green-300 hover:bg-green-50",
        isDocument &&
          isSelected &&
          !isAnalytics &&
          "border-green-500 bg-green-50 text-green-700",
        isAnalytics && "border-violet-200 hover:border-violet-300 hover:bg-violet-50",
        isAnalytics && isSelected && "border-violet-500 bg-violet-50 text-violet-800"
      )}
    >
      {isDocument ? (
        <FileText
          className={cn(
            "h-3 w-3",
            isAnalytics ? "text-violet-500" : "text-green-500"
          )}
        />
      ) : (
        <ExternalLink className="h-3 w-3 text-gray-400" />
      )}
      <span className="max-w-[180px] truncate">{label}</span>
    </button>
  );
}
