import type { HolderHistoryPoint, TopPositionRow } from "./types";

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    throw new Error(`${path} -> HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

export function fetchPeriods(): Promise<string[]> {
  return getJSON("/api/periods");
}

export function fetchTopPositions(period: string, limit = 25): Promise<TopPositionRow[]> {
  const params = new URLSearchParams({ period, limit: String(limit) });
  return getJSON(`/api/top-positions?${params}`);
}

export function fetchHolderHistory(cik: string, cusip: string): Promise<HolderHistoryPoint[]> {
  const params = new URLSearchParams({ cik, cusip });
  return getJSON(`/api/holder-history?${params}`);
}
