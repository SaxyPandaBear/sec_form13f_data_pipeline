import { useRef, useState } from "react";
import type { HolderHistoryPoint } from "../types";
import { formatPctChange, formatQuarter, formatValueThousands, niceCeil, pctChangeClass } from "../format";

interface Props {
  points: HolderHistoryPoint[];
}

const CHART_WIDTH = 720;
const CHART_HEIGHT = 220;
const MARGIN = { top: 16, right: 16, bottom: 28, left: 64 };
const PLOT_WIDTH = CHART_WIDTH - MARGIN.left - MARGIN.right;
const PLOT_HEIGHT = CHART_HEIGHT - MARGIN.top - MARGIN.bottom;

export default function TrendChart({ points }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  if (points.length === 0) {
    return <p className="empty-state">No history for this holder and security.</p>;
  }

  const maxValue = niceCeil(Math.max(...points.map((p) => p.total_value), 1));
  const xFor = (i: number) => (points.length === 1 ? 0 : (i / (points.length - 1)) * PLOT_WIDTH);
  const yFor = (v: number) => PLOT_HEIGHT - (v / maxValue) * PLOT_HEIGHT;

  const linePath = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${xFor(i).toFixed(1)} ${yFor(p.total_value).toFixed(1)}`)
    .join(" ");

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => maxValue * f);
  const last = points[points.length - 1];

  function handleMove(e: React.PointerEvent<SVGSVGElement>) {
    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const scaleX = CHART_WIDTH / rect.width;
    const localX = (e.clientX - rect.left) * scaleX - MARGIN.left;
    if (points.length === 1) {
      setHoverIndex(0);
      return;
    }
    const step = PLOT_WIDTH / (points.length - 1);
    const idx = Math.round(localX / step);
    setHoverIndex(Math.min(Math.max(idx, 0), points.length - 1));
  }

  const hovered = hoverIndex !== null ? points[hoverIndex] : null;

  return (
    <div style={{ position: "relative" }}>
      <svg
        ref={svgRef}
        className="viz-root"
        viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`}
        width="100%"
        role="img"
        aria-label="Total reported value across quarters"
        onPointerMove={handleMove}
        onPointerLeave={() => setHoverIndex(null)}
      >
        <g transform={`translate(${MARGIN.left},${MARGIN.top})`}>
          {yTicks.map((t, i) => (
            <g key={i}>
              <line
                x1={0}
                x2={PLOT_WIDTH}
                y1={yFor(t)}
                y2={yFor(t)}
                stroke="var(--gridline)"
                strokeWidth={1}
              />
              <text x={-8} y={yFor(t)} textAnchor="end" dominantBaseline="middle" className="axis-text">
                {formatValueThousands(t)}
              </text>
            </g>
          ))}

          <line
            x1={0}
            x2={PLOT_WIDTH}
            y1={PLOT_HEIGHT}
            y2={PLOT_HEIGHT}
            stroke="var(--baseline)"
            strokeWidth={1}
          />

          {points.map((p, i) =>
            i % Math.ceil(points.length / 6) === 0 || i === points.length - 1 ? (
              <text
                key={p.periodofreport}
                x={xFor(i)}
                y={PLOT_HEIGHT + 18}
                textAnchor="middle"
                className="axis-text"
              >
                {formatQuarter(p.periodofreport)}
              </text>
            ) : null
          )}

          <path d={linePath} fill="none" stroke="var(--series-1)" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />

          {hoverIndex !== null && (
            <line
              x1={xFor(hoverIndex)}
              x2={xFor(hoverIndex)}
              y1={0}
              y2={PLOT_HEIGHT}
              stroke="var(--text-muted)"
              strokeWidth={1}
              strokeDasharray="2,2"
            />
          )}

          {points.map((p, i) => (
            <circle
              key={p.periodofreport}
              cx={xFor(i)}
              cy={yFor(p.total_value)}
              r={i === points.length - 1 || i === hoverIndex ? 4 : 3}
              fill="var(--series-1)"
              stroke="var(--surface-1)"
              strokeWidth={2}
            />
          ))}

          <text
            x={xFor(points.length - 1) + 8}
            y={yFor(last.total_value)}
            dominantBaseline="middle"
            className="chart-end-label"
          >
            {formatValueThousands(last.total_value)}
          </text>
        </g>
      </svg>
      {hovered && (
        <div
          className="chart-tooltip"
          style={{
            left: `${MARGIN.left + xFor(hoverIndex!) * ((svgRef.current?.getBoundingClientRect().width ?? CHART_WIDTH) / CHART_WIDTH)}px`,
            top: 4,
          }}
        >
          <div className="tt-label">{formatQuarter(hovered.periodofreport)}</div>
          <div className="tt-value">{formatValueThousands(hovered.total_value)}</div>
          <div className={pctChangeClass(hovered.total_value_pct_change)}>
            {formatPctChange(hovered.total_value_pct_change)} vs. prior quarter
          </div>
        </div>
      )}
    </div>
  );
}
