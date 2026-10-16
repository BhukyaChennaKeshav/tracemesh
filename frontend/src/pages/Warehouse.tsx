import { useCallback, useEffect, useState } from 'react';
import { Bar, BarChart, CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { Database, RefreshCw } from 'lucide-react';
import { api, type NamedQuery, type QueryResult } from '../api';
import { Card, Empty, ServiceName } from '../components';
import { colorFor, ms, time } from '../format';
import { useLive } from '../live';

const tooltipStyle = { background: 'var(--panel-2)', border: '1px solid var(--border)', borderRadius: 8 };

export default function Warehouse() {
  const { snapshot } = useLive();
  const [queries, setQueries] = useState<NamedQuery[]>([]);
  const [active, setActive] = useState('historical-summary');
  const [service, setService] = useState('payment-service');
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.warehouseQueries().then(setQueries).catch((e) => setError(e.message));
  }, []);

  const run = useCallback(async (refresh = false) => {
    setLoading(true);
    setError(null);
    try {
      setResult(await api.runQuery(active, service, refresh));
    } catch (e) {
      setResult(null);
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [active, service]);

  useEffect(() => { run(); }, [run]);

  const current = queries.find((q) => q.id === active);
  const threshold = snapshot?.slaThresholdMs ?? 1000;

  return (
    <div className="stack">
      <div className="tabs" role="tablist">
        {queries.map((q) => (
          <button key={q.id} role="tab" aria-selected={q.id === active}
            className={q.id === active ? 'tab is-active' : 'tab'} onClick={() => setActive(q.id)}>
            {q.title}
          </button>
        ))}
      </div>

      <Card
        title={<><Database size={16} /> {current?.title ?? 'Warehouse'}</>}
        actions={
          <>
            {active === 'service-trend' && (
              <select value={service} onChange={(e) => setService(e.target.value)} aria-label="Service">
                {(snapshot?.services.map((s) => s.serviceName) ?? [service]).map((s) => <option key={s}>{s}</option>)}
              </select>
            )}
            <button className="btn" onClick={() => run(true)} disabled={loading}>
              <RefreshCw size={15} className={loading ? 'spin' : ''} /> {loading ? 'Running…' : 'Re-run'}
            </button>
          </>
        }
      >
        <p className="muted small">{current?.description}</p>
        {current && <pre className="sql">{current.sql}</pre>}
        {result && !loading && (
          <p className="muted small">
            {result.rows.length} row(s) in {(result.elapsedMs / 1000).toFixed(1)}s · executed {time(result.executedAt)} on HiveServer2
          </p>
        )}
        {error && (
          <div className="error-box">
            {error}
            <div className="small">Hive takes a minute or two to boot, and Spark only writes a window once the
              1-minute watermark has passed. Try again shortly.</div>
          </div>
        )}
        {loading && !result && <Empty>Running query on Hive… MapReduce stages can take 10–30s.</Empty>}
        {result && <ResultView result={result} threshold={threshold} service={service} />}
      </Card>
    </div>
  );
}

function ResultView({ result, threshold, service }: { result: QueryResult; threshold: number; service: string }) {
  if (!result.rows.length) return <Empty>No rows yet. Spark appends windows to HDFS about a minute after they close.</Empty>;
  const id = result.query.id;

  return (
    <>
      {id === 'service-trend' && (
        <div style={{ height: 240 }} className="chart-gap">
          <ResponsiveContainer>
            <LineChart data={[...result.rows].reverse()} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis dataKey="window_start" stroke="var(--muted)" fontSize={11} minTickGap={48}
                tickFormatter={(v) => String(v).slice(11, 19)} />
              <YAxis stroke="var(--muted)" fontSize={11} width={56} unit=" ms" />
              <Tooltip contentStyle={tooltipStyle} formatter={(v: number) => ms(v)} />
              <ReferenceLine y={threshold} stroke="var(--bad)" strokeDasharray="6 4" />
              <Line dataKey="p99_latency_ms" name="P99" stroke={colorFor(service)} strokeWidth={2} dot={false} />
              <Line dataKey="avg_latency_ms" name="Avg" stroke="var(--muted)" strokeWidth={1.5} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
      {id === 'historical-summary' && (
        <div style={{ height: 240 }} className="chart-gap">
          <ResponsiveContainer>
            <BarChart data={result.rows} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="var(--grid)" vertical={false} />
              <XAxis dataKey="servicename" stroke="var(--muted)" fontSize={11} />
              <YAxis stroke="var(--muted)" fontSize={11} width={56} unit=" ms" />
              <Tooltip contentStyle={tooltipStyle} formatter={(v: number) => ms(v)} cursor={{ fill: 'var(--hover)' }} />
              <Bar dataKey="peak_p99_latency" name="Peak P99" fill="var(--bad)" radius={[4, 4, 0, 0]} />
              <Bar dataKey="overall_avg_p99" name="Avg P99" fill="var(--accent)" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
      <div className="table-wrap tall">
        <table>
          <thead>
            <tr>{result.columns.map((c) => <th key={c}>{c}</th>)}</tr>
          </thead>
          <tbody>
            {result.rows.map((row, i) => (
              <tr key={i}>
                {result.columns.map((c) => {
                  const v = row[c];
                  if (c.toLowerCase() === 'servicename') return <td key={c}><ServiceName name={String(v)} /></td>;
                  const isNum = typeof v === 'number';
                  const bad = isNum && c.includes('p99') && v > threshold;
                  return (
                    <td key={c} className={`mono ${isNum ? 'num' : ''} ${bad ? 'text-bad' : ''}`}>
                      {isNum ? Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(v ?? '—')}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
