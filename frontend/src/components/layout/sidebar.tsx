import { MessageSquare, Database, Settings, Plus, Trash2, FileText } from "lucide-react";
import { Button } from "../ui/button";
import { cn } from "../../lib/utils";
import type { Conversation } from "../../lib/types";

type View = "chat" | "archive" | "documents" | "settings";

interface SidebarProps {
  currentView: View;
  onViewChange: (view: View) => void;
  conversations: Conversation[];
  activeConversationId: string | null;
  onSelectConversation: (id: string) => void;
  onNewConversation: () => void;
  onDeleteConversation: (id: string) => void;
}

export function Sidebar({
  currentView,
  onViewChange,
  conversations,
  activeConversationId,
  onSelectConversation,
  onNewConversation,
  onDeleteConversation,
}: SidebarProps) {
  const navItems: { id: View; label: string; icon: typeof MessageSquare }[] = [
    { id: "chat", label: "Chat", icon: MessageSquare },
    { id: "documents", label: "Documents", icon: FileText },
    { id: "archive", label: "Archive", icon: Database },
    { id: "settings", label: "Settings", icon: Settings },
  ];
  
  return (
    <div className="w-64 bg-gray-900 text-white flex flex-col h-full">
      {/* Logo */}
      <div className="p-4 border-b border-gray-800">
        <h1 className="text-lg font-bold">Freshness Service</h1>
        <p className="text-xs text-gray-400 mt-1">AI-powered search assistant</p>
      </div>
      
      {/* Navigation */}
      <nav className="p-2 border-b border-gray-800">
        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              onClick={() => onViewChange(item.id)}
              className={cn(
                "w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors",
                currentView === item.id
                  ? "bg-gray-800 text-white"
                  : "text-gray-400 hover:text-white hover:bg-gray-800"
              )}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </button>
          );
        })}
      </nav>
      
      {/* Conversations (only show in chat view) */}
      {currentView === "chat" && (
        <div className="flex-1 flex flex-col overflow-hidden">
          <div className="p-2">
            <Button
              variant="outline"
              className="w-full justify-start gap-2 bg-transparent border-gray-700 text-gray-300 hover:bg-gray-800 hover:text-white"
              onClick={onNewConversation}
            >
              <Plus className="h-4 w-4" />
              New Chat
            </Button>
          </div>
          
          <div className="flex-1 overflow-y-auto px-2 pb-2">
            {conversations.length === 0 ? (
              <p className="text-xs text-gray-500 text-center py-4">
                No conversations yet
              </p>
            ) : (
              <div className="space-y-1">
                {conversations.map((conv) => (
                  <div
                    key={conv.id}
                    className={cn(
                      "group flex items-center gap-2 px-3 py-2 rounded-lg text-sm cursor-pointer transition-colors",
                      activeConversationId === conv.id
                        ? "bg-gray-800 text-white"
                        : "text-gray-400 hover:text-white hover:bg-gray-800"
                    )}
                    onClick={() => onSelectConversation(conv.id)}
                  >
                    <MessageSquare className="h-4 w-4 flex-shrink-0" />
                    <span className="flex-1 truncate">{conv.title}</span>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        onDeleteConversation(conv.id);
                      }}
                      className="opacity-0 group-hover:opacity-100 p-1 hover:bg-gray-700 rounded transition-opacity"
                    >
                      <Trash2 className="h-3 w-3 text-gray-400 hover:text-red-400" />
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
      
      {/* Footer */}
      <div className="p-4 border-t border-gray-800">
        <p className="text-xs text-gray-500">
          Local-first AI assistant
        </p>
      </div>
    </div>
  );
}
