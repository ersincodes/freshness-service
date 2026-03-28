import { useState } from "react";
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import { Menu } from "lucide-react";
import { ChatProvider, useChat } from "./store/chat-store.tsx";
import { Sidebar } from "./components/layout/sidebar";
import { MessageList } from "./components/chat/message-list";
import { Composer } from "./components/chat/composer";
import { ChatToolbar } from "./components/chat/chat-toolbar";
import { ChatErrorBanner } from "./components/chat/chat-error-banner";
import { SourceInspector } from "./components/chat/source-inspector";
import { ArchiveList } from "./components/archive/archive-list";
import { ArchiveDetail } from "./components/archive/archive-detail";
import { SettingsView } from "./components/settings/settings-view";
import { DocumentLibrary } from "./components/documents/document-library";
import { Button } from "./components/ui/button";
import { listDocuments } from "./lib/api";
import { useMediaQuery } from "./lib/use-media-query";
import type { Source, ArchiveEntry } from "./lib/types";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

type View = "chat" | "archive" | "documents" | "settings";

function MobileMenuButton({ onClick }: { onClick: () => void }) {
  return (
    <Button
      type="button"
      variant="outline"
      size="icon"
      className="shrink-0 lg:hidden"
      onClick={onClick}
      aria-label="Open menu"
    >
      <Menu className="h-5 w-5" />
    </Button>
  );
}

function AppContent() {
  const [currentView, setCurrentView] = useState<View>("chat");
  const [selectedSource, setSelectedSource] = useState<Source | null>(null);
  const [selectedArchiveHash, setSelectedArchiveHash] = useState<string | null>(null);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const isLg = useMediaQuery("(min-width: 1024px)");

  const {
    state,
    activeConversation,
    createConversation,
    deleteConversation,
    setActiveConversation,
    sendMessage,
    stopStreaming,
    clearError,
    setPreferredMode,
    setIncludeWeb,
    setIncludeDocuments,
  } = useChat();

  const { data: documentsData } = useQuery({
    queryKey: ["documents"],
    queryFn: listDocuments,
    staleTime: 30000,
  });

  const readyDocuments = documentsData?.documents.filter((d) => d.status === "ready") ?? [];

  const closeMobileMenu = () => setMobileMenuOpen(false);

  const handleSourceClick = (source: Source) => {
    setSelectedSource(source);
  };

  const handleCloseSourceInspector = () => {
    setSelectedSource(null);
  };

  const handleSelectArchiveEntry = (entry: ArchiveEntry) => {
    setSelectedArchiveHash(entry.url_hash);
  };

  const sidebarProps = {
    currentView,
    onViewChange: setCurrentView,
    conversations: state.conversations,
    activeConversationId: state.activeConversationId,
    onSelectConversation: setActiveConversation,
    onNewConversation: createConversation,
    onDeleteConversation: deleteConversation,
  };

  return (
    <div className="flex h-screen bg-gray-100">
      <div className="hidden h-full w-64 shrink-0 lg:block">
        <Sidebar {...sidebarProps} />
      </div>

      {mobileMenuOpen && (
        <>
          <button
            type="button"
            className="fixed inset-0 z-30 bg-black/50 lg:hidden"
            aria-label="Close menu"
            onClick={closeMobileMenu}
          />
          <div className="fixed inset-y-0 left-0 z-40 w-64 max-w-[85vw] lg:hidden">
            <Sidebar {...sidebarProps} onNavigate={closeMobileMenu} />
          </div>
        </>
      )}

      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
        {currentView === "chat" && (
          <div className="flex min-h-0 min-w-0 flex-1 overflow-hidden">
            <div className="flex min-h-0 min-w-0 flex-1 flex-col bg-white">
              <div className="shrink-0 border-b border-gray-200 px-4 py-3">
                <div className="mx-auto flex max-w-3xl items-start gap-3">
                  <MobileMenuButton onClick={() => setMobileMenuOpen(true)} />
                  <div className="min-w-0 flex-1">
                    <ChatToolbar
                      preferredMode={state.preferredMode}
                      onPreferredModeChange={setPreferredMode}
                      includeWeb={state.includeWeb}
                      onIncludeWebChange={setIncludeWeb}
                      includeDocuments={state.includeDocuments}
                      onIncludeDocumentsChange={setIncludeDocuments}
                      readyDocumentsCount={readyDocuments.length}
                    />
                  </div>
                </div>
              </div>
              <ChatErrorBanner message={state.error} onDismiss={clearError} />
              <MessageList
                turns={activeConversation?.turns || []}
                onSourceClick={handleSourceClick}
                selectedSourceUrl={selectedSource?.url}
                onSuggestionSelect={(text) => void sendMessage(text)}
              />
              <Composer
                onSend={sendMessage}
                onStop={stopStreaming}
                isLoading={state.isLoading}
              />
            </div>

            {isLg ? (
              <div className="flex w-80 shrink-0 border-l border-gray-200">
                <SourceInspector
                  source={selectedSource}
                  onClose={handleCloseSourceInspector}
                />
              </div>
            ) : selectedSource ? (
              <>
                <button
                  type="button"
                  className="fixed inset-0 z-40 bg-black/50"
                  aria-label="Close source panel"
                  onClick={handleCloseSourceInspector}
                />
                <div
                  className="fixed inset-y-0 right-0 z-50 flex w-full max-w-md flex-col border-l border-gray-200 bg-white shadow-xl"
                  role="dialog"
                  aria-modal="true"
                  aria-label="Source details"
                >
                  <SourceInspector
                    source={selectedSource}
                    onClose={handleCloseSourceInspector}
                  />
                </div>
              </>
            ) : null}
          </div>
        )}

        {currentView === "archive" && (
          <>
            {!isLg && selectedArchiveHash ? (
              <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden bg-white">
                <div className="flex shrink-0 items-center gap-2 border-b border-gray-200 px-4 py-2 lg:hidden">
                  <MobileMenuButton onClick={() => setMobileMenuOpen(true)} />
                  <span className="text-sm font-medium text-gray-900">Archive</span>
                </div>
                <ArchiveDetail
                  urlHash={selectedArchiveHash}
                  onBack={() => setSelectedArchiveHash(null)}
                />
              </div>
            ) : (
              <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden lg:flex-row">
                <div className="flex min-h-0 w-full shrink-0 flex-col border-b border-gray-200 bg-white lg:h-full lg:w-96 lg:border-b-0 lg:border-r">
                  <div className="flex shrink-0 items-center gap-2 border-b border-gray-200 px-4 py-2 lg:hidden">
                    <MobileMenuButton onClick={() => setMobileMenuOpen(true)} />
                    <span className="text-sm font-medium text-gray-900">Archive</span>
                  </div>
                  <div className="min-h-[220px] min-w-0 flex-1 lg:min-h-0">
                    <ArchiveList
                      onSelectEntry={handleSelectArchiveEntry}
                      selectedUrlHash={selectedArchiveHash || undefined}
                    />
                  </div>
                </div>
                <ArchiveDetail urlHash={selectedArchiveHash} />
              </div>
            )}
          </>
        )}

        {currentView === "documents" && (
          <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden bg-white">
            <div className="flex shrink-0 items-center gap-2 border-b border-gray-200 px-4 py-2 lg:hidden">
              <MobileMenuButton onClick={() => setMobileMenuOpen(true)} />
              <span className="text-sm font-medium text-gray-900">Documents</span>
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto">
              <DocumentLibrary />
            </div>
          </div>
        )}

        {currentView === "settings" && (
          <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden bg-white">
            <div className="flex shrink-0 items-center gap-2 border-b border-gray-200 px-4 py-2 lg:hidden">
              <MobileMenuButton onClick={() => setMobileMenuOpen(true)} />
              <span className="text-sm font-medium text-gray-900">Settings</span>
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto">
              <SettingsView />
            </div>
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
