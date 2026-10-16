import { NavLink, Route, Routes, useLocation } from 'react-router-dom';
import { Activity, AlertTriangle, Database, LayoutDashboard, ListTree, Network } from 'lucide-react';
import { useLive } from './live';
import Overview from './pages/Overview';
import Spans from './pages/Spans';
import Incidents from './pages/Incidents';
import Warehouse from './pages/Warehouse';
import Pipeline from './pages/Pipeline';

const NAV = [
  { to: '/', label: 'Overview', icon: LayoutDashboard, title: 'Service health overview' },
  { to: '/spans', label: 'Live spans', icon: ListTree, title: 'Live span stream' },
  { to: '/incidents', label: 'Incidents', icon: AlertTriangle, title: 'SLA incidents' },
  { to: '/warehouse', label: 'Warehouse', icon: Database, title: 'Hive warehouse analytics' },
  { to: '/pipeline', label: 'Pipeline', icon: Network, title: 'Pipeline status' },
];

const CONN_LABEL = { connecting: 'Connecting', live: 'Live', polling: 'Polling', offline: 'Backend offline' };

export default function App() {
  const { conn, snapshot } = useLive();
  const { pathname } = useLocation();
  const page = NAV.find((n) => n.to === pathname) ?? NAV[0];
  const breaches = snapshot?.activeBreaches ?? 0;

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <Activity size={20} />
          <div>
            <div className="brand-name">TraceMesh</div>
            <div className="brand-sub">SRE observability</div>
          </div>
        </div>
        <nav>
          {NAV.map(({ to, label, icon: Icon }) => (
            <NavLink key={to} to={to} end className={({ isActive }) => (isActive ? 'nav is-active' : 'nav')}>
              <Icon size={17} />
              <span>{label}</span>
              {to === '/incidents' && breaches > 0 && <span className="nav-count">{breaches}</span>}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-foot">Kafka → Spark → HDFS → Hive</div>
      </aside>

      <main className="main">
        <header className="topbar">
          <h1>{page.title}</h1>
          <div className={`conn conn-${conn}`}>
            <span className="dot" />
            {CONN_LABEL[conn]}
          </div>
        </header>
        <div className="content">
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/spans" element={<Spans />} />
            <Route path="/incidents" element={<Incidents />} />
            <Route path="/warehouse" element={<Warehouse />} />
            <Route path="/pipeline" element={<Pipeline />} />
            <Route path="*" element={<Overview />} />
          </Routes>
        </div>
      </main>
    </div>
  );
}
