import { useEffect, useState } from "react";
import { fetchHolderHistory, fetchPeriods, fetchTopPositions } from "./api";
import type { HolderHistoryPoint, TopPositionRow } from "./types";
import PositionsTable from "./components/PositionsTable";
import HistoryTable from "./components/HistoryTable";
import TrendChart from "./components/TrendChart";
import { formatQuarter } from "./format";

export default function App() {
  const [periods, setPeriods] = useState<string[]>([]);
  const [period, setPeriod] = useState<string | null>(null);
  const [positions, setPositions] = useState<TopPositionRow[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [selected, setSelected] = useState<TopPositionRow | null>(null);
  const [history, setHistory] = useState<HolderHistoryPoint[]>([]);

  useEffect(() => {
    fetchPeriods()
      .then((values) => {
        setPeriods(values);
        if (values.length > 0) setPeriod(values[0]);
      })
      .catch(() => setLoadError("Couldn't reach the gold API. Is docker compose up?"));
  }, []);

  useEffect(() => {
    if (!period) {
      setPositions([]);
      return;
    }
    fetchTopPositions(period, 25)
      .then(setPositions)
      .catch(() => setPositions([]));
  }, [period]);

  function openDetail(row: TopPositionRow) {
    setSelected(row);
    setHistory([]);
    fetchHolderHistory(row.cik, row.cusip)
      .then(setHistory)
      .catch(() => setHistory([]));
  }

  function backToTable() {
    setSelected(null);
    setHistory([]);
  }

  return (
    <>
      <h1>SEC 13F Gold Explorer</h1>
      <p className="subtitle">
        Largest reported positions from the pipeline's <code>holder_positions</code> gold table.
      </p>

      {loadError && <p className="empty-state">{loadError}</p>}

      {!loadError && periods.length === 0 && (
        <p className="empty-state">
          No data yet — the <code>holder_positions</code> table is empty or hasn't been built. Trigger
          the <code>sec_13f_pipeline</code> DAG in Airflow and check back once
          <code> build_gold_holder_positions</code> has run.
        </p>
      )}

      {periods.length > 0 && !selected && (
        <>
          <div className="filter-row">
            <div className="field">
              <label htmlFor="period-select">Reporting quarter</label>
              <select id="period-select" value={period ?? ""} onChange={(e) => setPeriod(e.target.value)}>
                {periods.map((p) => (
                  <option key={p} value={p}>
                    {formatQuarter(p)}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="card">
            <h2>Largest positions — {period ? formatQuarter(period) : ""}</h2>
            <p className="card-subtitle">Ranked by total reported value · click a row for its history</p>
            <PositionsTable rows={positions} onSelect={openDetail} />
          </div>
        </>
      )}

      {selected && (
        <>
          <button className="back-link" onClick={backToTable}>
            ← Back to {period ? formatQuarter(period) : "quarter"} positions
          </button>

          <div className="card">
            <h2>
              {selected.filingmanager_name} — {selected.nameofissuer}
            </h2>
            <p className="card-subtitle">
              CUSIP {selected.cusip} · total reported value by quarter
            </p>
            <TrendChart points={history} />
          </div>

          <div className="card">
            <h2>Supporting data</h2>
            <p className="card-subtitle">Every reported quarter for this holder and CUSIP</p>
            <HistoryTable points={history} />
          </div>
        </>
      )}
    </>
  );
}
