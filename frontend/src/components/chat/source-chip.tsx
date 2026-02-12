import { ExternalLink } from "lucide-react";
import type { Source } from "../../lib/types";
import { extractDomain } from "../../lib/utils";
import { cn } from "../../lib/utils";

interface SourceChipProps {
  source: Source;
  onClick?: () => void;
  isSelected?: boolean;
}

export function SourceChip({ source, onClick, isSelected }: SourceChipProps) {
  const domain = extractDomain(source.url);
  
  return (
    <button
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium transition-colors",
        "border border-gray-200 hover:border-primary-300 hover:bg-primary-50",
        isSelected && "border-primary-500 bg-primary-50 text-primary-700"
      )}
    >
      <ExternalLink className="h-3 w-3 text-gray-400" />
      <span className="max-w-[150px] truncate">{domain}</span>
    </button>
  );
}
