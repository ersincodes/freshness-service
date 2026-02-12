import { RefreshCw, Server, Globe, Database, Cpu, CheckCircle, XCircle, AlertCircle } from "lucide-react";
import { Button } from "../ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "../ui/card";
import { Badge } from "../ui/badge";
import { useSettings, useHealth } from "../../lib/hooks";
import type { HealthStatus } from "../../lib/types";

function HealthIndicator({ status }: { status: HealthStatus }) {
  const config = {
    ok: { icon: CheckCircle, color: "text-green-500", label: "Healthy" },
    error: { icon: XCircle, color: "text-red-500", label: "Error" },
    unavailable: { icon: AlertCircle, color: "text-yellow-500", label: "Unavailable" },
  };
  
  const { icon: Icon, color, label } = config[status.status];
  
  return (
    <div className="flex items-center gap-2">
      <Icon className={`h-5 w-5 ${color}`} />
      <div>
        <span className="font-medium">{label}</span>
        {status.message && (
          <p className="text-xs text-gray-500">{status.message}</p>
        )}
        {status.latency_ms !== undefined && status.latency_ms !== null && (
          <p className="text-xs text-gray-400">{status.latency_ms}ms latency</p>
        )}
      </div>
    </div>
  );
}

export function SettingsView() {
  const { data: settings, isLoading: settingsLoading, refetch: refetchSettings } = useSettings();
  const { data: health, isLoading: healthLoading, refetch: refetchHealth } = useHealth();
  
  const handleRefresh = () => {
    refetchSettings();
    refetchHealth();
  };
  
  return (
    <div className="p-6 max-w-4xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Settings & Health</h1>
          <p className="text-gray-500 mt-1">View system configuration and service health</p>
        </div>
        <Button variant="outline" onClick={handleRefresh} disabled={settingsLoading || healthLoading}>
          <RefreshCw className={`h-4 w-4 mr-2 ${(settingsLoading || healthLoading) ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>
      
      {/* Health Status */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Server className="h-5 w-5" />
            Service Health
          </CardTitle>
        </CardHeader>
        <CardContent>
          {healthLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
            </div>
          ) : health ? (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="p-4 bg-gray-50 rounded-lg">
                <div className="flex items-center gap-2 mb-3">
                  <Server className="h-4 w-4 text-gray-500" />
                  <span className="text-sm font-medium text-gray-700">Backend</span>
                </div>
                <HealthIndicator status={health.backend} />
              </div>
              
              <div className="p-4 bg-gray-50 rounded-lg">
                <div className="flex items-center gap-2 mb-3">
                  <Cpu className="h-4 w-4 text-gray-500" />
                  <span className="text-sm font-medium text-gray-700">LM Studio</span>
                </div>
                <HealthIndicator status={health.lm_studio} />
              </div>
              
              <div className="p-4 bg-gray-50 rounded-lg">
                <div className="flex items-center gap-2 mb-3">
                  <Globe className="h-4 w-4 text-gray-500" />
                  <span className="text-sm font-medium text-gray-700">Brave Search</span>
                </div>
                <HealthIndicator status={health.brave_search} />
              </div>
            </div>
          ) : (
            <p className="text-gray-500">Unable to load health status</p>
          )}
        </CardContent>
      </Card>
      
      {/* Configuration */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Configuration
          </CardTitle>
        </CardHeader>
        <CardContent>
          {settingsLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
            </div>
          ) : settings ? (
            <div className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {/* Brave API Key */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    Brave API Key
                  </label>
                  <div className="mt-1">
                    <Badge variant={settings.brave_api_key_set ? "success" : "warning"}>
                      {settings.brave_api_key_set ? "Configured" : "Not Set"}
                    </Badge>
                  </div>
                </div>
                
                {/* LM Studio URL */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    LM Studio URL
                  </label>
                  <p className="mt-1 text-sm text-gray-700 font-mono">
                    {settings.lm_studio_base_url}
                  </p>
                </div>
                
                {/* Model Name */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    Model Name
                  </label>
                  <p className="mt-1 text-sm text-gray-700 font-mono">
                    {settings.model_name}
                  </p>
                </div>
                
                {/* Retrieval Mode */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    Offline Retrieval Mode
                  </label>
                  <div className="mt-1">
                    <Badge variant="info">
                      {settings.offline_retrieval_mode === "semantic" ? "Semantic" : "Keyword"}
                    </Badge>
                  </div>
                </div>
                
                {/* Max Search Results */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    Max Search Results
                  </label>
                  <p className="mt-1 text-sm text-gray-700">
                    {settings.max_search_results}
                  </p>
                </div>
                
                {/* Request Timeout */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    Request Timeout
                  </label>
                  <p className="mt-1 text-sm text-gray-700">
                    {settings.request_timeout_s}s
                  </p>
                </div>
                
                {/* Max Chars Per Source */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    Max Chars Per Source
                  </label>
                  <p className="mt-1 text-sm text-gray-700">
                    {settings.max_chars_per_source.toLocaleString()}
                  </p>
                </div>
                
                {/* Semantic Top K */}
                <div className="p-4 bg-gray-50 rounded-lg">
                  <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                    Semantic Top K
                  </label>
                  <p className="mt-1 text-sm text-gray-700">
                    {settings.semantic_top_k}
                  </p>
                </div>
              </div>
            </div>
          ) : (
            <p className="text-gray-500">Unable to load settings</p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
