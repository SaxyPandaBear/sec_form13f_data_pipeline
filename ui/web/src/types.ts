export interface TopPositionRow {
  cik: string;
  filingmanager_name: string;
  cusip: string;
  nameofissuer: string;
  total_value: number;
  total_shares: number;
  total_value_pct_change: number | null;
  total_shares_pct_change: number | null;
}

export interface HolderHistoryPoint {
  periodofreport: string;
  total_value: number;
  total_shares: number;
  total_value_pct_change: number | null;
  total_shares_pct_change: number | null;
}
