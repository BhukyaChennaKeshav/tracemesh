import { Link } from 'react-router-dom';
import { AnomalyControl, Badge, Card, Empty, Kpi, LatencyChart, ServiceName } from '../components';
import { duration, ms, num, time } from '../format';
import { useLive } from '../live';

export default function Overview() {
  const { snapshot, history, incidents } = useLive();
  if (!snapshot) return <Empty>Connecting to the trace producer…</Empty>;

  const threshold = snapshot.slaThresholdMs;
  const services = snapshot.services.map((s) => s.serviceName);
  const breaching = snapshot.activeBreaches > 0;
  const errorRate = snapshot.windowSpans ? (snapshot.windowErrors / snapshot.windowSpans) * 100 : 0;

  return (
    <div className="stack">
      <AnomalyControl />

      <div className="kpis">
        <Kpi label="Max P99 latency" value={ms(snapshot.maxP99Ms)} tone={breaching ? 'bad' : 'good'}
          hint={breaching ? 'SLA violation in progress' : `Within SLA (${threshold} ms)`} />
        <Kpi label="Avg latency" value={ms(snapshot.avgLatencyMs, 1)} hint={`Across ${services.length} services`} />
        <Kpi label="Throughput" value={`${num(snapshot.throughputPerSec)}/s`}
          hint={`${num(snapshot.windowSpans)} spans in last ${snapshot.windowSeconds}s`} />
        <Kpi label="5xx error rate" value={`${errorRate.toFixed(2)}%`} tone={errorRate > 1 ? 'warn' : 'neutral'}
          hint={`${num(snapshot.windowErrors)} errors in window`} />
        <Kpi label="Active SLA breaches" value={snapshot.activeBreaches} tone={breaching ? 'bad' : 'good'}
          hint={`P99 > ${threshold} ms`} />
      </div>

      <div className="grid-2-1">
        <Card title="P99 latency per service"
          actions={<span className="pill">{snapshot.windowSeconds}s window · {snapshot.slideSeconds}s slide</span>}>
          <LatencyChart history={history} services={services} threshold={threshold} />
        </Card>

        <Card title="SLA breach log" actions={<Link to="/incidents" className="link">View all</Link>}>
          {incidents.length === 0 ? (
            <Empty>No SLA breaches recorded. Inject an anomaly to test detection.</Empty>
          ) : (
            <ul className="incident-list">
              {incidents.slice(0, 6).map((i) => (
                <li key={i.id} className={i.active ? 'is-active' : ''}>
                  <div>
                    <ServiceName name={i.serviceName} />
                    <div className="sub">Peak P99 {ms(i.peakP99Ms)} · {duration(i.startedAt, i.resolvedAt)}</div>
                  </div>
                  <div className="right">
                    {i.active ? <Badge tone="bad">Active</Badge> : <Badge tone="muted">Resolved</Badge>}
                    <div className="sub mono">{time(i.startedAt)}</div>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>

      <Card title="Service telemetry matrix" actions={<span className="pill">Updated {time(snapshot.generatedAt)}</span>}>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Service</th>
                <th className="num">P99</th>
                <th className="num">Avg</th>
                <th className="num">Spans</th>
                <th className="num">5xx</th>
                <th>P99 vs SLA</th>
                <th>Health</th>
              </tr>
            </thead>
            <tbody>
              {snapshot.services.map((s) => (
                <tr key={s.serviceName}>
                  <td><ServiceName name={s.serviceName} /></td>
                  <td className={`num mono ${s.slaBreached ? 'text-bad' : ''}`}>{ms(s.p99LatencyMs)}</td>
                  <td className="num mono">{ms(s.avgLatencyMs, 1)}</td>
                  <td className="num mono">{num(s.totalRequests)}</td>
                  <td className="num mono">{num(s.errorCount)}</td>
                  <td>
                    <div className="meter">
                      <span className={s.slaBreached ? 'bad' : ''}
                        style={{ width: `${Math.min(100, (s.p99LatencyMs / (threshold * 3)) * 100)}%` }} />
                      <i style={{ left: '33.3%' }} />
                    </div>
                  </td>
                  <td>{s.slaBreached ? <Badge tone="bad">SLA breach</Badge> : <Badge tone="good">Healthy</Badge>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
