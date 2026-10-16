import { useEffect, useState } from 'react';
import { ArrowRight, Boxes, Cpu, Database, ExternalLink, HardDrive, RadioTower, RefreshCw } from 'lucide-react';
import { api, type PipelineStage } from '../api';
import { Badge, Card, Empty } from '../components';
import { time } from '../format';

const ICONS: Record<string, typeof Cpu> = {
  producer: RadioTower, kafka: Boxes, spark: Cpu, hdfs: HardDrive, hive: Database,
};

// Host-side UIs published by docker-compose
const CONSOLES = [
  { name: 'Spark Master UI', url: 'http://localhost:8080', note: 'Workers, running streaming app' },
  { name: 'HDFS NameNode UI', url: 'http://localhost:9870', note: 'Browse /user/hive/warehouse/tracemesh_metrics' },
];

const tone = (s: PipelineStage['status']) => (s === 'up' ? 'good' : s === 'degraded' ? 'warn' : 'bad');

export default function Pipeline() {
  const [stages, setStages] = useState<PipelineStage[]>([]);
  const [checkedAt, setCheckedAt] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    try {
      const h = await api.pipelineHealth();
      setStages(h.stages);
      setCheckedAt(h.checkedAt);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    const t = window.setInterval(load, 10000);
    return () => window.clearInterval(t);
  }, []);

  return (
    <div className="stack">
      <Card
        title="Data flow"
        actions={
          <>
            <span className="pill">Checked {time(checkedAt)}</span>
            <button className="btn" onClick={load} disabled={loading}>
              <RefreshCw size={15} className={loading ? 'spin' : ''} /> Refresh
            </button>
          </>
        }
      >
        {error && <div className="error-box">{error}</div>}
        {!stages.length && !error ? <Empty>Probing pipeline…</Empty> : (
          <div className="flow">
            {stages.map((s, i) => {
              const Icon = ICONS[s.id] ?? Cpu;
              return (
                <div key={s.id} className="flow-item">
                  <div className={`stage stage-${s.status}`}>
                    <div className="stage-head">
                      <Icon size={18} />
                      <Badge tone={tone(s.status)}>{s.status}</Badge>
                    </div>
                    <div className="stage-name">{s.name}</div>
                    <div className="stage-tech">{s.tech}</div>
                    <div className="stage-detail">{s.detail}</div>
                  </div>
                  {i < stages.length - 1 && <ArrowRight className="flow-arrow" size={18} />}
                </div>
              );
            })}
          </div>
        )}
      </Card>

      <div className="grid-2">
        <Card title="How it works">
          <ol className="steps">
            <li><b>Trace Producer</b> emits ~100 synthetic spans/s for 5 microservices into Kafka.</li>
            <li><b>Kafka</b> buffers them on <code>telemetry-spans</code> (3 partitions).</li>
            <li><b>Spark Structured Streaming</b> computes P99/avg/errors over 10s windows sliding every 5s, with a 1-minute watermark.</li>
            <li>Windows are appended as partitioned <b>Parquet on HDFS</b>.</li>
            <li><b>Hive</b> exposes them as <code>tracemesh_db.service_latency_metrics</code> for SQL analysis.</li>
            <li>This dashboard tails Kafka for the live view and queries Hive for history.</li>
          </ol>
        </Card>
        <Card title="Cluster consoles">
          <ul className="links">
            {CONSOLES.map((c) => (
              <li key={c.url}>
                <a href={c.url} target="_blank" rel="noreferrer">{c.name} <ExternalLink size={13} /></a>
                <span className="muted small">{c.note}</span>
              </li>
            ))}
          </ul>
        </Card>
      </div>
    </div>
  );
}
