import { useEffect, useRef } from "react";
import { Message } from "./message";
import type { ChatTurn, Source } from "../../lib/types";

const SUGGESTED_PROMPTS = [
  "What are the latest developments in renewable energy?",
  "Summarize best practices for API security.",
  "Compare SQL and NoSQL for a new side project.",
  "What should I know before deploying to production?",
] as const;

interface MessageListProps {
  turns: ChatTurn[];
  onSourceClick?: (source: Source) => void;
  selectedSourceUrl?: string;
  onSuggestionSelect?: (text: string) => void;
}

export function MessageList({
  turns,
  onSourceClick,
  selectedSourceUrl,
  onSuggestionSelect,
}: MessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const lastTurnContent = turns[turns.length - 1]?.content;

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [turns.length, lastTurnContent]);
  
  if (turns.length === 0) {
    return (
      <div className="flex flex-1 items-center justify-center p-6 sm:p-8">
        <div className="max-w-lg text-center">
          <h2 className="mb-2 text-xl font-semibold text-gray-900">
            Welcome to Freshness Service
          </h2>
          <p className="mb-6 text-gray-500">
            Ask me anything. I search the web for fresh information with citations, or use
            your local archive when offline.
          </p>
          {onSuggestionSelect && (
            <div className="space-y-2 text-left">
              <p className="text-center text-xs font-medium uppercase tracking-wide text-gray-400">
                Try asking
              </p>
              <div className="flex flex-col gap-2">
                {SUGGESTED_PROMPTS.map((text) => (
                  <button
                    key={text}
                    type="button"
                    onClick={() => onSuggestionSelect(text)}
                    className="rounded-lg border border-gray-200 bg-white px-3 py-2 text-left text-sm text-gray-700 transition-colors hover:border-primary-300 hover:bg-primary-50/60"
                  >
                    {text}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }
  
  return (
    <div className="flex-1 overflow-y-auto">
      <div className="max-w-3xl mx-auto py-4 space-y-2">
        {turns.map((turn) => (
          <Message
            key={turn.id}
            turn={turn}
            onSourceClick={onSourceClick}
            selectedSourceUrl={selectedSourceUrl}
          />
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
