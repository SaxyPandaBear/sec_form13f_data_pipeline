import type { TopPositionRow } from "../types";
import { formatPctChange, formatShares, formatValueThousands, pctChangeClass } from "../format";

interface Props {
  rows: TopPositionRow[];
  onSelect: (row: TopPositionRow) => void;
}

export default function PositionsTable({ rows, onSelect }: Props) {
  if (rows.length === 0) {
    return <p className="empty-state">No positions found for this quarter.</p>;
  }

  return (
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Holder</th>
          <th>Security</th>
          <th className="num">Value</th>
          <th className="num">Δ Value</th>
          <th className="num">Shares</th>
          <th className="num">Δ Shares</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={`${row.cik}-${row.cusip}`} className="holder-row" onClick={() => onSelect(row)}>
            <td>{i + 1}</td>
            <td>{row.filingmanager_name}</td>
            <td>
              {row.nameofissuer} <span className="cusip">{row.cusip}</span>
            </td>
            <td className="num">{formatValueThousands(row.total_value)}</td>
            <td className={`num ${pctChangeClass(row.total_value_pct_change)}`}>
              {formatPctChange(row.total_value_pct_change)}
            </td>
            <td className="num">{formatShares(row.total_shares)}</td>
            <td className={`num ${pctChangeClass(row.total_shares_pct_change)}`}>
              {formatPctChange(row.total_shares_pct_change)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
