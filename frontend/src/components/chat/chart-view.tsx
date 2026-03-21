import type { ReactNode } from "react";
import {
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
  const ensure = (date: string): Row => {
    let r = byX.get(date);
    if (!r) {
      r = { x: date };
      byX.set(date, r);
    }
    return r;
  };

  for (const s of chart.series) {
    for (const p of s.data) {
      ensure(p.date)[s.name] = p.value;
    }
    if (s.confidence_band) {
      for (const p of s.confidence_band.lower) {
        ensure(p.date)[`${s.name}__lower`] = p.value;
      }
      for (const p of s.confidence_band.upper) {
        ensure(p.date)[`${s.name}__upper`] = p.value;
      }
    }
  }

  return Array.from(byX.values()).sort((a, b) =>
    String(a.x).localeCompare(String(b.x))
  );
}

const PALETTE = ["#2563eb", "#7c3aed", "#059669", "#d97706"];

export function ChartView({ chart }: { chart: ChartSpec }) {
  const data = buildRows(chart);
  const lines: ReactNode[] = [];
  let colorIdx = 0;

  for (const s of chart.series) {
    const col = PALETTE[colorIdx % PALETTE.length];
    colorIdx += 1;
    const dash = s.style === "dashed" ? "6 4" : undefined;
    if (s.data.length > 0) {
      lines.push(
        <Line
          key={s.name}
          type="monotone"
          dataKey={s.name}
          name={s.name}
          stroke={col}
          strokeWidth={2}
          strokeDasharray={dash}
          dot={false}
          connectNulls
        />
      );
    }
    if (s.confidence_band) {
      lines.push(
        <Line
          key={`${s.name}-lower`}
          type="monotone"
          dataKey={`${s.name}__lower`}
          name={`${s.name} lower`}
          stroke={col}
          strokeWidth={1}
          strokeOpacity={0.45}
          strokeDasharray="2 3"
          dot={false}
          connectNulls
        />,
        <Line
          key={`${s.name}-upper`}
          type="monotone"
          dataKey={`${s.name}__upper`}
          name={`${s.name} upper`}
          stroke={col}
          strokeWidth={1}
          strokeOpacity={0.45}
          strokeDasharray="2 3"
          dot={false}
          connectNulls
        />
      );
    }
  }

  return (
    <div className="mt-4 w-full min-h-[280px]">
      <p className="text-sm font-medium text-gray-800 mb-2">{chart.title}</p>
      <div className="h-72 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="x"
              tick={{ fontSize: 11 }}
              label={{
                value: chart.x_label,
                position: "insideBottom",
                offset: -4,
                style: { fontSize: 11, fill: "#6b7280" },
              }}
            />
            <YAxis
              tick={{ fontSize: 11 }}
              width={48}
              label={{
                value: chart.y_label,
                angle: -90,
                position: "insideLeft",
                style: { fontSize: 11, fill: "#6b7280" },
              }}
            />
            <Tooltip />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            {chart.forecast_start ? (
              <ReferenceLine
                x={chart.forecast_start}
                stroke="#94a3b8"
                strokeDasharray="4 4"
                label={{ value: "Forecast", position: "top", fill: "#64748b", fontSize: 10 }}
              />
            ) : null}
            {lines}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
