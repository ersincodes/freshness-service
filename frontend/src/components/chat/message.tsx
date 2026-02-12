import ReactMarkdown from "react-markdown";
import { User, Bot, Loader2 } from "lucide-react";
import type { ChatTurn, Source } from "../../lib/types";
import { ModeBadge } from "./mode-badge";
import { SourceChip } from "./source-chip";
import { cn, formatDate } from "../../lib/utils";

interface MessageProps {
  turn: ChatTurn;
  onSourceClick?: (source: Source) => void;
  selectedSourceUrl?: string;
}

export function Message({ turn, onSourceClick, selectedSourceUrl }: MessageProps) {
  const isUser = turn.role === "user";
  const isAssistant = turn.role === "assistant";
  
  return (
    <div
      className={cn(
        "flex gap-4 p-4 rounded-lg",
        isUser && "bg-gray-50",
        isAssistant && "bg-white"
      )}
    >
      {/* Avatar */}
      <div
        className={cn(
          "flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center",
          isUser && "bg-primary-100 text-primary-600",
          isAssistant && "bg-gray-100 text-gray-600"
        )}
      >
        {isUser ? <User className="h-4 w-4" /> : <Bot className="h-4 w-4" />}
      </div>
      
      {/* Content */}
      <div className="flex-1 min-w-0">
        {/* Header */}
        <div className="flex items-center gap-2 mb-2">
          <span className="font-medium text-sm text-gray-900">
            {isUser ? "You" : "Assistant"}
          </span>
          <span className="text-xs text-gray-400">
            {formatDate(turn.created_at)}
          </span>
          {isAssistant && turn.mode && <ModeBadge mode={turn.mode} />}
          {turn.isStreaming && (
            <Loader2 className="h-4 w-4 animate-spin text-primary-500" />
          )}
        </div>
        
        {/* Message content */}
        <div className="prose prose-sm max-w-none text-gray-700">
          {turn.content ? (
            <ReactMarkdown
              components={{
                // Style code blocks
                code: ({ className, children, ...props }) => {
                  const isInline = !className;
                  return isInline ? (
                    <code
                      className="px-1.5 py-0.5 bg-gray-100 rounded text-sm font-mono"
                      {...props}
                    >
                      {children}
                    </code>
                  ) : (
                    <code
                      className={cn(
                        "block p-3 bg-gray-900 text-gray-100 rounded-lg overflow-x-auto text-sm font-mono",
                        className
                      )}
                      {...props}
                    >
                      {children}
                    </code>
                  );
                },
                // Style links
                a: ({ children, ...props }) => (
                  <a
                    className="text-primary-600 hover:text-primary-700 underline"
                    target="_blank"
                    rel="noopener noreferrer"
                    {...props}
                  >
                    {children}
                  </a>
                ),
              }}
            >
              {turn.content}
            </ReactMarkdown>
          ) : turn.isStreaming ? (
            <span className="text-gray-400">Thinking...</span>
          ) : null}
        </div>
        
        {/* Error display */}
        {turn.error && (
          <div className="mt-2 p-2 bg-red-50 border border-red-200 rounded text-sm text-red-700">
            <strong>Error:</strong> {turn.error.message}
          </div>
        )}
        
        {/* Sources */}
        {isAssistant && turn.sources && turn.sources.length > 0 && (
          <div className="mt-3 pt-3 border-t border-gray-100">
            <span className="text-xs text-gray-500 font-medium mb-2 block">
              Sources ({turn.sources.length})
            </span>
            <div className="flex flex-wrap gap-2">
              {turn.sources.map((source, index) => (
                <SourceChip
                  key={`${source.url}-${index}`}
                  source={source}
                  onClick={() => onSourceClick?.(source)}
                  isSelected={selectedSourceUrl === source.url}
                />
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
