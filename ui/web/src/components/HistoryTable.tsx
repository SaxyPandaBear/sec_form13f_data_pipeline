import type { HolderHistoryPoint } from "../types";
import { formatPctChange, formatPeriod, formatShares, formatValueThousands, pctChangeClass } from "../format";

interface Props {
  points: HolderHistoryPoint[];
}

export default function HistoryTable({ points }: Props) {
  if (points.length === 0) return null;

  return (
    <table>
      <thead>
        <tr>
          <th>Quarter</th>
          <th className="num">Value</th>
          <th className="num">Δ Value</th>
          <th className="num">Shares</th>
          <th className="num">Δ Shares</th>
        </tr>
      </thead>
      <tbody>
        {[...points].reverse().map((point) => (
          <tr key={point.periodofreport}>
            <td>{formatPeriod(point.periodofreport)}</td>
            <td className="num">{formatValueThousands(point.total_value)}</td>
            <td className={`num ${pctChangeClass(point.total_value_pct_change)}`}>
              {formatPctChange(point.total_value_pct_change)}
            </td>
            <td className="num">{formatShares(point.total_shares)}</td>
            <td className={`num ${pctChangeClass(point.total_shares_pct_change)}`}>
              {formatPctChange(point.total_shares_pct_change)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
