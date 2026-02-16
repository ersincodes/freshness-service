import { useState } from "react";
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import { ChatProvider, useChat } from "./store/chat-store.tsx";
import { Sidebar } from "./components/layout/sidebar";
import { MessageList } from "./components/chat/message-list";
import { Composer } from "./components/chat/composer";
import { SourceInspector } from "./components/chat/source-inspector";
import { ArchiveList } from "./components/archive/archive-list";
import { ArchiveDetail } from "./components/archive/archive-detail";
import { SettingsView } from "./components/settings/settings-view";
import { DocumentLibrary } from "./components/documents/document-library";
import { Button } from "./components/ui/button";
import { listDocuments } from "./lib/api";
import type { Source, ArchiveEntry } from "./lib/types";

// Create a client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

type View = "chat" | "archive" | "documents" | "settings";

function AppContent() {
  const [currentView, setCurrentView] = useState<View>("chat");
  const [selectedSource, setSelectedSource] = useState<Source | null>(null);
  const [selectedArchiveHash, setSelectedArchiveHash] = useState<string | null>(null);
  
  const {
    state,
    activeConversation,
    createConversation,
    deleteConversation,
    setActiveConversation,
    sendMessage,
    stopStreaming,
    setPreferredMode,
    setIncludeWeb,
    setIncludeDocuments,
  } = useChat();
  
  // Fetch documents for the toggle UI (no polling needed here, relies on cache invalidation)
  const { data: documentsData } = useQuery({
    queryKey: ["documents"],
    queryFn: listDocuments,
    staleTime: 30000, // Consider data fresh for 30 seconds
  });
  
  const readyDocuments = documentsData?.documents.filter(d => d.status === "ready") ?? [];
  
  const handleSourceClick = (source: Source) => {
    setSelectedSource(source);
  };
  
  const handleCloseSourceInspector = () => {
    setSelectedSource(null);
  };
  
  const handleSelectArchiveEntry = (entry: ArchiveEntry) => {
    setSelectedArchiveHash(entry.url_hash);
  };
  
  return (
    <div className="flex h-screen bg-gray-100">
      {/* Sidebar */}
      <Sidebar
        currentView={currentView}
        onViewChange={setCurrentView}
        conversations={state.conversations}
        activeConversationId={state.activeConversationId}
        onSelectConversation={setActiveConversation}
        onNewConversation={createConversation}
        onDeleteConversation={deleteConversation}
      />
      
      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {currentView === "chat" && (
          <>
            {/* Chat Area */}
            <div className="flex-1 flex flex-col bg-white">
              <div className="border-b border-gray-200 px-4 py-3">
                <div className="mx-auto flex max-w-3xl items-center justify-between">
                  <div className="flex items-center gap-4">
                    <p className="text-sm font-medium text-gray-700">Mode</p>
                    <div className="flex items-center gap-2">
                      <Button
                        type="button"
                        size="sm"
                        variant={state.preferredMode === "ONLINE" ? "default" : "outline"}
                        onClick={() => setPreferredMode("ONLINE")}
                      >
                        Online
                      </Button>
                      <Button
                        type="button"
                        size="sm"
                        variant={state.preferredMode === "OFFLINE" ? "default" : "outline"}
                        onClick={() => setPreferredMode("OFFLINE")}
                      >
                        Offline
                      </Button>
                    </div>
                  </div>
                  
                  {/* Source Toggles */}
                  <div className="flex items-center gap-4">
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={state.includeWeb}
                        onChange={(e) => setIncludeWeb(e.target.checked)}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                      <span className="text-gray-700">Web</span>
                    </label>
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={state.includeDocuments}
                        onChange={(e) => setIncludeDocuments(e.target.checked)}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                        disabled={readyDocuments.length === 0}
                      />
                      <span className={readyDocuments.length === 0 ? "text-gray-400" : "text-gray-700"}>
                        Documents {readyDocuments.length > 0 && `(${readyDocuments.length})`}
                      </span>
                    </label>
                  </div>
                </div>
              </div>
              <MessageList
                turns={activeConversation?.turns || []}
                onSourceClick={handleSourceClick}
                selectedSourceUrl={selectedSource?.url}
              />
              <Composer
                onSend={sendMessage}
                onStop={stopStreaming}
                isLoading={state.isLoading}
              />
            </div>
            
            {/* Source Inspector */}
            <SourceInspector
              source={selectedSource}
              onClose={handleCloseSourceInspector}
            />
          </>
        )}
        
        {currentView === "archive" && (
          <>
            {/* Archive List */}
            <div className="w-96 border-r border-gray-200 bg-white">
              <ArchiveList
                onSelectEntry={handleSelectArchiveEntry}
                selectedUrlHash={selectedArchiveHash || undefined}
              />
            </div>
            
            {/* Archive Detail */}
            <ArchiveDetail urlHash={selectedArchiveHash} />
          </>
        )}
        
        {currentView === "documents" && (
          <div className="flex-1 bg-white overflow-y-auto">
            <DocumentLibrary />
          </div>
        )}
        
        {currentView === "settings" && (
          <div className="flex-1 bg-white overflow-y-auto">
            <SettingsView />
          </div>
        )}
      </div>
    </div>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ChatProvider>
        <AppContent />
      </ChatProvider>
    </QueryClientProvider>
  );
}

export default App;
