import { useEffect, useState } from 'react';
import { Pause, Play } from 'lucide-react';
import { api, type TraceSpan } from '../api';
import { Badge, Card, Empty, ServiceName } from '../components';
import { ms, time } from '../format';
import { useLive } from '../live';

export default function Spans() {
  const { snapshot } = useLive();
  const [spans, setSpans] = useState<TraceSpan[]>([]);
  const [service, setService] = useState('');
  const [slowOnly, setSlowOnly] = useState(false);
  const [paused, setPaused] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const threshold = snapshot?.slaThresholdMs ?? 1000;

  useEffect(() => {
    if (paused) return;
    let cancelled = false;
    const load = () =>
      api.recentSpans(200, service || undefined)
        .then((s) => { if (!cancelled) { setSpans(s); setError(null); } })
        .catch((e) => !cancelled && setError(e.message));
    load();
    const t = window.setInterval(load, 1500);
    return () => { cancelled = true; window.clearInterval(t); };
  }, [service, paused]);

  const shown = slowOnly ? spans.filter((s) => s.executionDurationMs > threshold) : spans;

  return (
    <Card
      title="Live span stream"
      actions={
        <>
          <select value={service} onChange={(e) => setService(e.target.value)} aria-label="Filter by service">
            <option value="">All services</option>
            {snapshot?.services.map((s) => <option key={s.serviceName}>{s.serviceName}</option>)}
          </select>
          <label className="check">
            <input type="checkbox" checked={slowOnly} onChange={(e) => setSlowOnly(e.target.checked)} />
            Over SLA only
          </label>
          <button className="btn" onClick={() => setPaused((p) => !p)}>
            {paused ? <Play size={15} /> : <Pause size={15} />}
            {paused ? 'Resume' : 'Pause'}
          </button>
        </>
      }
    >
      <p className="muted small">
        Latest spans consumed from Kafka topic <code>telemetry-spans</code>, refreshed every 1.5s.
      </p>
      {error && <div className="error-box">{error}</div>}
      {shown.length === 0 ? (
        <Empty>{spans.length ? 'No spans match the filter.' : 'No spans received yet.'}</Empty>
      ) : (
        <div className="table-wrap tall">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Service</th>
                <th className="num">Duration</th>
                <th>Status</th>
                <th>Trace ID</th>
                <th>Span ID</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((s) => {
                const slow = s.executionDurationMs > threshold;
                return (
                  <tr key={s.spanId}>
                    <td className="mono">{time(s.timestamp)}</td>
                    <td><ServiceName name={s.serviceName} /></td>
                    <td className={`num mono ${slow ? 'text-bad' : ''}`}>{ms(s.executionDurationMs)}</td>
                    <td>{s.statusCode >= 500 ? <Badge tone="bad">{s.statusCode}</Badge> : <Badge tone="good">{s.statusCode}</Badge>}</td>
                    <td className="mono dim">{s.traceId.slice(0, 18)}…</td>
                    <td className="mono dim">{s.spanId.slice(0, 8)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}
