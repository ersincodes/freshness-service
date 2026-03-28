import { cn } from "../../lib/utils";

interface SpinnerProps {
  className?: string;
  /** Accessible status text; defaults to "Loading". */
  label?: string;
}

export function Spinner({ className, label = "Loading" }: SpinnerProps) {
  return (
    <div
      className={cn(
        "h-8 w-8 shrink-0 animate-spin rounded-full border-2 border-primary-600 border-t-transparent",
        className
      )}
      role="status"
      aria-label={label}
    />
  );
}
