import { Badge, Card, Empty, Kpi, ServiceName } from '../components';
import { dateTime, duration, ms } from '../format';
import { useLive } from '../live';

export default function Incidents() {
  const { incidents, snapshot } = useLive();
  const active = incidents.filter((i) => i.active);
  const resolved = incidents.filter((i) => !i.active);
  const mttr = resolved.length
    ? resolved.reduce((sum, i) => sum + (Date.parse(i.resolvedAt!) - Date.parse(i.startedAt)), 0) / resolved.length / 1000
    : null;

  return (
    <div className="stack">
      <div className="kpis">
        <Kpi label="Active incidents" value={active.length} tone={active.length ? 'bad' : 'good'} />
        <Kpi label="Resolved" value={resolved.length} />
        <Kpi label="Mean time to recover" value={mttr == null ? '—' : `${Math.round(mttr)}s`} />
        <Kpi label="Breach rule" value={`P99 > ${snapshot?.slaThresholdMs ?? 1000} ms`}
          hint={`per ${snapshot?.windowSeconds ?? 10}s sliding window`} />
      </div>

      <Card title="Incident history">
        <p className="muted small">
          An incident opens on the first window whose P99 exceeds the SLA and resolves on the first window back under it.
          Kept in memory on the producer; the durable record is in the Hive warehouse.
        </p>
        {incidents.length === 0 ? (
          <Empty>No incidents yet.</Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Service</th>
                  <th>Started</th>
                  <th>Resolved</th>
                  <th className="num">Duration</th>
                  <th className="num">Peak P99</th>
                  <th className="num">Breached windows</th>
                </tr>
              </thead>
              <tbody>
                {incidents.map((i) => (
                  <tr key={i.id}>
                    <td>{i.active ? <Badge tone="bad">Active</Badge> : <Badge tone="muted">Resolved</Badge>}</td>
                    <td><ServiceName name={i.serviceName} /></td>
                    <td className="mono">{dateTime(i.startedAt)}</td>
                    <td className="mono">{i.resolvedAt ? dateTime(i.resolvedAt) : '—'}</td>
                    <td className="num mono">{duration(i.startedAt, i.resolvedAt)}</td>
                    <td className="num mono text-bad">{ms(i.peakP99Ms)}</td>
                    <td className="num mono">{i.breachedWindows}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
