import type { ReactNode } from "react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { ChartSpec } from "../../lib/types";

type Row = Record<string, string | number | undefined> & { x: string };

function buildRows(chart: ChartSpec): Row[] {
  const byX = new Map<string, Row>();
  const order: string[] = [];
  const ensure = (date: string): Row => {
    let r = byX.get(date);
    if (!r) {
      r = { x: date };
      byX.set(date, r);
      order.push(date);
    }
    return r;
  };

  for (const s of chart.series) {
    for (const p of s.data) {
      ensure(p.date)[s.name] = p.value;
    }
    const band = s.area_band ?? s.confidence_band;
    if (band) {
      for (const p of band.lower) {
        ensure(p.date)[`${s.name}__lower`] = p.value;
      }
      for (const p of band.upper) {
        ensure(p.date)[`${s.name}__upper`] = p.value;
      }
    }
  }

  return order.map((x) => byX.get(x)!);
}

function formatCompact(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
}

function formatTooltipValue(value: number): string {
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}

const COLOR_HISTORICAL = "#2563eb";
const COLOR_FORECAST = "#7c3aed";
const COLOR_BAND = "#c4b5fd";

interface CustomTooltipProps {
  active?: boolean;
  payload?: Array<{ name: string; value: number; color: string }>;
  label?: string;
}

function CustomTooltip({ active, payload, label }: CustomTooltipProps) {
  if (!active || !payload?.length) return null;
  const visible = payload.filter(
    (p) => !p.name.includes("__lower") && !p.name.includes("__upper") && !p.name.includes("_band")
  );
  return (
    <div className="rounded-lg border border-gray-200 bg-white px-3 py-2 shadow-md text-xs">
      <p className="font-medium text-gray-700 mb-1">{label}</p>
      {visible.map((entry) => (
        <p key={entry.name} style={{ color: entry.color }} className="flex justify-between gap-4">
          <span>{entry.name}:</span>
          <span className="font-mono font-medium">{formatTooltipValue(entry.value)}</span>
        </p>
      ))}
    </div>
  );
}

export function ChartView({ chart }: { chart: ChartSpec }) {
  const data = buildRows(chart);
  const lines: ReactNode[] = [];
  const areas: ReactNode[] = [];

  for (const s of chart.series) {
    const isHistorical = s.name === "Historical";
    const color = isHistorical ? COLOR_HISTORICAL : COLOR_FORECAST;
    const dash = s.style === "dashed" ? "6 4" : undefined;

    if (s.data.length > 0) {
      lines.push(
        <Line
          key={s.name}
          type="monotone"
          dataKey={s.name}
          name={s.name}
          stroke={color}
          strokeWidth={isHistorical ? 2.5 : 2}
          strokeDasharray={dash}
          dot={isHistorical ? { r: 3, fill: color, strokeWidth: 0 } : { r: 3, fill: color, strokeWidth: 0 }}
          connectNulls
        />
      );
    }

    const band = s.area_band ?? s.confidence_band;
    if (band) {
      areas.push(
        <Area
          key={`${s.name}_band`}
          type="monotone"
          dataKey={`${s.name}__upper`}
          name={`${s.name}_band`}
          stroke="none"
          fill={COLOR_BAND}
          fillOpacity={0.3}
          connectNulls
          legendType="none"
        />,
        <Area
          key={`${s.name}_lower_mask`}
          type="monotone"
          dataKey={`${s.name}__lower`}
          name={`${s.name}__lower`}
          stroke={COLOR_BAND}
          strokeWidth={1}
          strokeDasharray="3 3"
          strokeOpacity={0.6}
          fill="white"
          fillOpacity={1}
          connectNulls
          legendType="none"
        />
      );
    }
  }

  return (
    <div className="mt-4 w-full min-h-[320px]">
      <p className="text-sm font-semibold text-gray-800">{chart.title}</p>
      {chart.subtitle && (
        <p className="text-xs text-gray-500 mb-2">{chart.subtitle}</p>
      )}
      <div className="h-80 w-full mt-1">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 12, right: 16, left: 4, bottom: 24 }}>
            <defs>
              <linearGradient id="bandGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={COLOR_BAND} stopOpacity={0.35} />
                <stop offset="100%" stopColor={COLOR_BAND} stopOpacity={0.08} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis
              dataKey="x"
              tick={{ fontSize: 10 }}
              angle={-30}
              textAnchor="end"
              height={48}
              label={{
                value: chart.x_label,
                position: "insideBottom",
                offset: -16,
                style: { fontSize: 11, fill: "#6b7280", fontWeight: 500 },
              }}
            />
            <YAxis
              tick={{ fontSize: 10 }}
              width={56}
              tickFormatter={formatCompact}
              label={{
                value: chart.y_label,
                angle: -90,
                position: "insideLeft",
                offset: 8,
                style: { fontSize: 11, fill: "#6b7280", fontWeight: 500 },
              }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Legend
              wrapperStyle={{ fontSize: 12, paddingTop: 4 }}
              formatter={(value: string) => {
                if (value === "Historical") return <span style={{ color: COLOR_HISTORICAL }}>Historical</span>;
                if (value === "Forecast") return <span style={{ color: COLOR_FORECAST }}>Forecast</span>;
                return value;
              }}
            />
            {chart.forecast_start ? (
              <ReferenceLine
                x={chart.forecast_start}
                stroke="#94a3b8"
                strokeDasharray="4 4"
                label={{
                  value: "Forecast start",
                  position: "top",
                  fill: "#64748b",
                  fontSize: 10,
                }}
              />
            ) : null}
            {areas}
            {lines}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
