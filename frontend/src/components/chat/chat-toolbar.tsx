import { Button } from "../ui/button";
import type { PreferredChatMode } from "../../lib/types";

interface ChatToolbarProps {
  preferredMode: PreferredChatMode;
  onPreferredModeChange: (mode: PreferredChatMode) => void;
  includeWeb: boolean;
  onIncludeWebChange: (value: boolean) => void;
  includeDocuments: boolean;
  onIncludeDocumentsChange: (value: boolean) => void;
  readyDocumentsCount: number;
}

export function ChatToolbar({
  preferredMode,
  onPreferredModeChange,
  includeWeb,
  onIncludeWebChange,
  includeDocuments,
  onIncludeDocumentsChange,
  readyDocumentsCount,
}: ChatToolbarProps) {
  const documentsDisabled = readyDocumentsCount === 0;

  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex flex-wrap items-center gap-3 sm:gap-4">
        <p className="text-sm font-medium text-gray-700">Mode</p>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            size="sm"
            variant={preferredMode === "ONLINE" ? "default" : "outline"}
            onClick={() => onPreferredModeChange("ONLINE")}
            title="Use live web search when available"
          >
            Online
          </Button>
          <Button
            type="button"
            size="sm"
            variant={preferredMode === "OFFLINE" ? "default" : "outline"}
            onClick={() => onPreferredModeChange("OFFLINE")}
            title="Prefer local archive and offline retrieval"
          >
            Offline
          </Button>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-4">
        <label className="flex cursor-pointer items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={includeWeb}
            onChange={(e) => onIncludeWebChange(e.target.checked)}
            className="rounded border-gray-300 text-primary-600 focus:ring-2 focus:ring-primary-500"
            title="Include web search results in answers"
          />
          <span className="text-gray-700">Web</span>
        </label>
        <label
          className={`flex items-center gap-2 text-sm ${documentsDisabled ? "cursor-not-allowed" : "cursor-pointer"}`}
        >
          <input
            type="checkbox"
            checked={includeDocuments}
            onChange={(e) => onIncludeDocumentsChange(e.target.checked)}
            className="rounded border-gray-300 text-primary-600 focus:ring-2 focus:ring-primary-500 disabled:opacity-50"
            disabled={documentsDisabled}
            title={
              documentsDisabled
                ? "Upload and process documents first"
                : "Include uploaded documents in retrieval"
            }
          />
          <span className={documentsDisabled ? "text-gray-400" : "text-gray-700"}>
            Documents
            {readyDocumentsCount > 0 && ` (${readyDocumentsCount})`}
          </span>
        </label>
      </div>
    </div>
  );
}
