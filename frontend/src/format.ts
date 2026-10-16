// Fixed colour per service so a service looks the same on every chart and table.
export const SERVICE_COLORS: Record<string, string> = {
  'auth-service': '#60a5fa',
  'inventory-service': '#34d399',
  'notification-service': '#c084fc',
  'order-service': '#fbbf24',
  'payment-service': '#f472b6',
};

export const colorFor = (service: string) => SERVICE_COLORS[service] ?? '#94a3b8';

export const ms = (v: number | null | undefined, digits = 0) =>
  v == null || Number.isNaN(v) ? '—' : `${Number(v).toLocaleString(undefined, { maximumFractionDigits: digits })} ms`;

export const num = (v: number | null | undefined) => (v == null ? '—' : Number(v).toLocaleString());

export const time = (iso: string | null | undefined) =>
  iso ? new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—';

export const dateTime = (iso: string | null | undefined) =>
  iso ? new Date(iso).toLocaleString([], { dateStyle: 'medium', timeStyle: 'medium' }) : '—';

export function duration(fromIso: string, toIso?: string | null) {
  const secs = Math.max(0, Math.round(((toIso ? Date.parse(toIso) : Date.now()) - Date.parse(fromIso)) / 1000));
  if (secs < 60) return `${secs}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `${m}m ${secs % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}
