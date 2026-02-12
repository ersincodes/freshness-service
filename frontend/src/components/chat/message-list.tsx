import { useEffect, useRef } from "react";
import { Message } from "./message";
import type { ChatTurn, Source } from "../../lib/types";

interface MessageListProps {
  turns: ChatTurn[];
  onSourceClick?: (source: Source) => void;
  selectedSourceUrl?: string;
}

export function MessageList({ turns, onSourceClick, selectedSourceUrl }: MessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);
  
  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [turns.length, turns[turns.length - 1]?.content]);
  
  if (turns.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center p-8">
        <div className="text-center">
          <h2 className="text-xl font-semibold text-gray-900 mb-2">
            Welcome to Freshness Service
          </h2>
          <p className="text-gray-500 max-w-md">
            Ask me anything! I'll search the web for fresh information and provide
            answers with citations. When offline, I'll use my local archive.
          </p>
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
