import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ChatProvider, useChat } from "./store/chat-store.tsx";
import { Sidebar } from "./components/layout/sidebar";
import { MessageList } from "./components/chat/message-list";
import { Composer } from "./components/chat/composer";
import { SourceInspector } from "./components/chat/source-inspector";
import { ArchiveList } from "./components/archive/archive-list";
import { ArchiveDetail } from "./components/archive/archive-detail";
import { SettingsView } from "./components/settings/settings-view";
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

type View = "chat" | "archive" | "settings";

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
  } = useChat();
  
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
