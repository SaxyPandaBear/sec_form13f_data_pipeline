const compactUSD = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 1,
});

const shareCount = new Intl.NumberFormat("en-US");

// total_value is reported in thousands of USD (SEC's own convention, see README).
export function formatValueThousands(valueThousands: number): string {
  return compactUSD.format(valueThousands * 1000);
}

export function formatShares(shares: number): string {
  return shareCount.format(shares);
}

export function formatPctChange(pct: number | null): string {
  if (pct === null) return "—";
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}%`;
}

export function pctChangeClass(pct: number | null): string {
  if (pct === null || pct === 0) return "delta-flat";
  return pct > 0 ? "delta-up" : "delta-down";
}

// periodofreport is always a quarter-end date (SEC's own convention), so it reads better
// as the quarter it closes out ("Q2 2026") than as that literal date ("Jun 30, 2026").
export function formatQuarter(iso: string): string {
  const date = new Date(`${iso}T00:00:00Z`);
  const quarter = Math.floor(date.getUTCMonth() / 3) + 1;
  return `Q${quarter} ${date.getUTCFullYear()}`;
}

// Rounds up to a "nice" axis max (1/2/5 x 10^n) so gridline ticks land on clean numbers.
export function niceCeil(value: number): number {
  if (value <= 0) return 1;
  const exponent = Math.floor(Math.log10(value));
  const magnitude = 10 ** exponent;
  const fraction = value / magnitude;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * magnitude;
}
