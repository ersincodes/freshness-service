import { X } from "lucide-react";
import { Button } from "../ui/button";

interface ChatErrorBannerProps {
  message: string | null;
  onDismiss: () => void;
}

export function ChatErrorBanner({ message, onDismiss }: ChatErrorBannerProps) {
  if (!message) return null;

  return (
    <div
      className="border-b border-red-200 bg-red-50 px-4 py-2"
      role="alert"
    >
      <div className="mx-auto flex max-w-3xl items-start gap-2">
        <p className="flex-1 text-sm text-red-800">{message}</p>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-8 w-8 shrink-0 text-red-700 hover:bg-red-100 hover:text-red-900"
          onClick={onDismiss}
          aria-label="Dismiss error"
        >
          <X className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
