// Typed client for the trace-producer REST API. All paths are relative (/api/...):
// in dev Vite proxies them to :8081, in Docker nginx proxies them to trace-producer.

export interface WindowMetric {
  serviceName: string;
  windowStart: string;
  windowEnd: string;
  p99LatencyMs: number;
  avgLatencyMs: number;
  totalRequests: number;
  errorCount: number;
  slaBreached: boolean;
}

export interface AnomalyState {
  anomalyActive: boolean;
  targetService: string;
  since: string;
}

export interface Incident {
  id: string;
  serviceName: string;
  startedAt: string;
  resolvedAt: string | null;
  peakP99Ms: number;
  breachedWindows: number;
  active: boolean;
}

export type HistoryPoint = { time: string } & Record<string, number | string>;

export interface Snapshot {
  generatedAt: string;
  slaThresholdMs: number;
  windowSeconds: number;
  slideSeconds: number;
  maxP99Ms: number;
  avgLatencyMs: number;
  windowSpans: number;
  windowErrors: number;
  activeBreaches: number;
  throughputPerSec: number;
  consumedTotal: number;
  lastSpanAt: string | null;
  anomaly: AnomalyState;
  services: WindowMetric[];
  historyPoint?: HistoryPoint | null;
  incidents?: Incident[];
}

export interface TraceSpan {
  serviceName: string;
  traceId: string;
  spanId: string;
  timestamp: string;
  executionDurationMs: number;
  statusCode: number;
}

export interface PipelineStage {
  id: string;
  name: string;
  tech: string;
  status: 'up' | 'degraded' | 'down';
  detail: string;
  meta: Record<string, unknown>;
}

export interface NamedQuery {
  id: string;
  title: string;
  description: string;
  sql: string;
}

export interface QueryResult {
  query: NamedQuery;
  columns: string[];
  rows: Record<string, string | number | null>[];
  elapsedMs: number;
  executedAt: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  const body = await res.json().catch(() => null);
  if (!res.ok) {
    throw new Error(body?.error ?? body?.message ?? `${res.status} ${res.statusText}`);
  }
  return body as T;
}

export const api = {
  services: () => request<string[]>('/api/services'),
  live: () => request<Snapshot>('/api/metrics/live'),
  history: (points = 120) => request<HistoryPoint[]>(`/api/metrics/history?points=${points}`),
  incidents: () => request<Incident[]>('/api/incidents'),
  recentSpans: (limit = 100, service?: string) =>
    request<TraceSpan[]>(`/api/spans/recent?limit=${limit}${service ? `&service=${encodeURIComponent(service)}` : ''}`),
  anomalyStatus: () => request<AnomalyState>('/api/anomaly/status'),
  toggleAnomaly: (enable: boolean, service: string) =>
    request<AnomalyState>(`/api/anomaly/toggle?enable=${enable}&service=${encodeURIComponent(service)}`, {
      method: 'POST',
    }),
  pipelineHealth: () => request<{ checkedAt: string; stages: PipelineStage[] }>('/api/pipeline/health'),
  warehouseQueries: () => request<NamedQuery[]>('/api/warehouse/queries'),
  runQuery: (id: string, service: string, refresh = false) =>
    request<QueryResult>(
      `/api/warehouse/queries/${id}?service=${encodeURIComponent(service)}${refresh ? '&refresh=true' : ''}`,
    ),
};
