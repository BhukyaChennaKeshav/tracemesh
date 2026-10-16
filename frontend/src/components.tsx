import { useState, type ReactNode } from 'react';
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { Zap, ZapOff } from 'lucide-react';
import { api, type HistoryPoint } from './api';
import { colorFor, ms, time } from './format';
import { useLive } from './live';

export function Card({ title, actions, children, className = '' }: {
  title?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string;
}) {
  return (
    <section className={`card ${className}`}>
      {(title || actions) && (
        <header className="card-head">
          {title && <h2>{title}</h2>}
          {actions && <div className="card-actions">{actions}</div>}
        </header>
      )}
      {children}
    </section>
  );
}

export function Kpi({ label, value, hint, tone = 'neutral' }: {
  label: string; value: ReactNode; hint?: ReactNode; tone?: 'neutral' | 'good' | 'bad' | 'warn';
}) {
  return (
    <div className={`kpi tone-${tone}`}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value}</div>
      {hint && <div className="kpi-hint">{hint}</div>}
    </div>
  );
}

export function Badge({ tone, children }: { tone: 'good' | 'bad' | 'warn' | 'muted'; children: ReactNode }) {
  return <span className={`badge badge-${tone}`}>{children}</span>;
}

export function ServiceName({ name }: { name: string }) {
  return (
    <span className="svc">
      <i style={{ background: colorFor(name) }} />
      {name}
    </span>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

export function LatencyChart({ history, services, threshold, height = 300 }: {
  history: HistoryPoint[]; services: string[]; threshold: number; height?: number;
}) {
  if (!history.length) return <Empty>Waiting for the first window…</Empty>;
  return (
    <div style={{ height }}>
      <ResponsiveContainer>
        <LineChart data={history} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
          <CartesianGrid stroke="var(--grid)" vertical={false} />
          <XAxis dataKey="time" tickFormatter={time} stroke="var(--muted)" fontSize={11} minTickGap={48} />
          <YAxis stroke="var(--muted)" fontSize={11} width={56} tickFormatter={(v) => `${v}`} unit=" ms" />
          <Tooltip
            contentStyle={{ background: 'var(--panel-2)', border: '1px solid var(--border)', borderRadius: 8 }}
            labelFormatter={(l) => time(String(l))}
            formatter={(v: number, n: string) => [ms(v), n]}
          />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <ReferenceLine y={threshold} stroke="var(--bad)" strokeDasharray="6 4"
            label={{ value: `SLA ${threshold} ms`, fill: 'var(--bad)', fontSize: 11, position: 'insideTopRight' }} />
          {services.map((s) => (
            <Line key={s} type="monotone" dataKey={s} stroke={colorFor(s)} strokeWidth={2} dot={false}
              isAnimationActive={false} connectNulls />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

/** Calls the producer's chaos endpoint to inject latency into one service. */
export function AnomalyControl({ compact = false }: { compact?: boolean }) {
  const { snapshot, applySnapshot } = useLive();
  const anomaly = snapshot?.anomaly;
  const services = snapshot?.services.map((s) => s.serviceName) ?? [];
  const [target, setTarget] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const selected = target ?? anomaly?.targetService ?? 'payment-service';
  const active = !!anomaly?.anomalyActive;

  const toggle = async () => {
    setBusy(true);
    setError(null);
    try {
      const next = await api.toggleAnomaly(!active, selected);
      if (snapshot) applySnapshot({ ...snapshot, anomaly: next, historyPoint: null, incidents: undefined });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className={`anomaly ${active ? 'is-active' : ''} ${compact ? 'compact' : ''}`}>
      <div className="anomaly-text">
        <div className="anomaly-title">Chaos: latency injection</div>
        <div className="anomaly-sub">
          {active ? <>Spiking <b>{anomaly?.targetService}</b> to 1.5–3.5 s</> : 'All services at baseline'}
        </div>
      </div>
      <select value={selected} disabled={active || busy} onChange={(e) => setTarget(e.target.value)}
        aria-label="Target service">
        {(services.length ? services : [selected]).map((s) => <option key={s}>{s}</option>)}
      </select>
      <button className={active ? 'btn' : 'btn btn-danger'} onClick={toggle} disabled={busy || !snapshot}>
        {active ? <ZapOff size={15} /> : <Zap size={15} />}
        {active ? 'Stop anomaly' : 'Inject anomaly'}
      </button>
      {error && <div className="anomaly-error">{error}</div>}
    </div>
  );
}
