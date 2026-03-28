import type { ReactNode } from "react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
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
      const lineStart = s.data[0]?.date;
      const bandStart = band.lower[0]?.date ?? band.upper[0]?.date;
      if (lineStart && bandStart && lineStart !== bandStart) {
        const bridgeVal = s.data[0]?.value;
        if (bridgeVal != null) {
          ensure(lineStart)[`${s.name}__lower`] = bridgeVal;
          ensure(lineStart)[`${s.name}__upper`] = bridgeVal;
        }
      }
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

/**
 * Return the last x that has a Historical value.
 * The reference line is placed at `position="end"` of this band,
 * which visually sits on the boundary between historical and forecast.
 */
function getLastHistoricalX(rows: Row[]): string | undefined {
  for (let i = rows.length - 1; i >= 0; i--) {
    const v = rows[i].Historical;
    if (v === undefined || v === null) continue;
    const n = typeof v === "number" ? v : Number(v);
    if (!Number.isNaN(n)) return rows[i].x;
  }
  return undefined;
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
const COLOR_FORECAST_START = "#64748b";

interface CustomTooltipProps {
  active?: boolean;
  payload?: Array<{ name: string; value: number; color: string }>;
  label?: string;
}

function SeriesLegendRow({
  chart,
  forecastStartX,
}: {
  chart: ChartSpec;
  forecastStartX?: string;
}) {
  const items = chart.series.filter(
    (s) => s.data.length > 0 && (s.name === "Historical" || s.name === "Forecast")
  );
  if (!items.length && !forecastStartX) return null;
  return (
    <div className="flex flex-wrap justify-center gap-x-8 gap-y-1.5 pb-1 text-xs">
      {items.map((s) => {
        const dashed = s.style === "dashed" || s.name === "Forecast";
        const color = s.name === "Historical" ? COLOR_HISTORICAL : COLOR_FORECAST;
        return (
          <span key={s.name} className="inline-flex items-center gap-2">
            <svg width={28} height={10} className="shrink-0" aria-hidden>
              <line
                x1={1}
                y1={5}
                x2={27}
                y2={5}
                stroke={color}
                strokeWidth={2}
                strokeLinecap="round"
                strokeDasharray={dashed ? "5 4" : undefined}
              />
            </svg>
            <span style={{ color }}>{s.name}</span>
          </span>
        );
      })}
      {forecastStartX ? (
        <span className="inline-flex items-center gap-2">
          <svg width={28} height={10} className="shrink-0" aria-hidden>
            <line
              x1={14}
              y1={1}
              x2={14}
              y2={9}
              stroke={COLOR_FORECAST_START}
              strokeWidth={1.75}
              strokeLinecap="round"
              strokeDasharray="3 2.5"
            />
          </svg>
          <span style={{ color: COLOR_FORECAST_START }} className="font-medium">
            Forecast start
          </span>
        </span>
      ) : null}
    </div>
  );
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
  const forecastStartX = getLastHistoricalX(data) ?? chart.forecast_start;
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
      <SeriesLegendRow chart={chart} forecastStartX={forecastStartX} />
      <div className="mt-1 flex h-80 w-full flex-col">
        <div className="min-h-0 min-w-0 flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
              data={data}
              margin={{ top: 8, right: 32, left: 4, bottom: 4 }}
            >
              <defs>
                <linearGradient id="bandGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={COLOR_BAND} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={COLOR_BAND} stopOpacity={0.08} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="x"
                textAnchor="middle"
                tick={{ fontSize: 10 }}
                angle={-42}
                interval="preserveStartEnd"
                minTickGap={18}
                height={54}
                tickMargin={10}
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
              {forecastStartX ? (
                <ReferenceLine
                  x={forecastStartX}
                  stroke="#64748b"
                  strokeDasharray="4 4"
                  strokeWidth={1.5}
                  ifOverflow="visible"
                  position="end"
                />
              ) : null}
              {areas}
              {lines}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
        <p className="shrink-0 pt-1 text-center text-[11px] font-medium leading-tight text-gray-600">
          {chart.x_label}
        </p>
      </div>
    </div>
  );
}
