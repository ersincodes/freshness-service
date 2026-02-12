import { Badge } from "../ui/badge";
import { Globe, Database, Cpu } from "lucide-react";
import type { RetrievalMode } from "../../lib/types";

interface ModeBadgeProps {
  mode: RetrievalMode;
}

const modeConfig: Record<RetrievalMode, { label: string; variant: "success" | "warning" | "info"; icon: typeof Globe }> = {
  ONLINE: {
    label: "Online",
    variant: "success",
    icon: Globe,
  },
  OFFLINE_ARCHIVE: {
    label: "Archive",
    variant: "warning",
    icon: Database,
  },
  LOCAL_WEIGHTS: {
    label: "Local",
    variant: "info",
    icon: Cpu,
  },
};

export function ModeBadge({ mode }: ModeBadgeProps) {
  const config = modeConfig[mode];
  const Icon = config.icon;
  
  return (
    <Badge variant={config.variant} className="gap-1">
      <Icon className="h-3 w-3" />
      {config.label}
    </Badge>
  );
}
