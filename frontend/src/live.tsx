import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from 'react';
import { api, type HistoryPoint, type Incident, type Snapshot } from './api';

// One SSE connection for the whole app; falls back to polling if the stream drops.

type ConnState = 'connecting' | 'live' | 'polling' | 'offline';

interface LiveData {
  snapshot: Snapshot | null;
  history: HistoryPoint[];
  incidents: Incident[];
  conn: ConnState;
  applySnapshot: (s: Snapshot) => void;
}

const MAX_POINTS = 120;
const LiveContext = createContext<LiveData | null>(null);

export function LiveProvider({ children }: { children: ReactNode }) {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [conn, setConn] = useState<ConnState>('connecting');
  const pollTimer = useRef<number | undefined>(undefined);

  const applySnapshot = useCallback((s: Snapshot) => {
    setSnapshot(s);
    if (s.incidents) setIncidents(s.incidents);
    const point = s.historyPoint;
    if (point) {
      setHistory((prev) =>
        prev.length && prev[prev.length - 1].time === point.time ? prev : [...prev, point].slice(-MAX_POINTS),
      );
    }
  }, []);

  useEffect(() => {
    let es: EventSource | null = null;
    let cancelled = false;

    const poll = async () => {
      try {
        const [s, inc, hist] = await Promise.all([api.live(), api.incidents(), api.history(MAX_POINTS)]);
        if (cancelled) return;
        setSnapshot(s);
        setIncidents(inc);
        setHistory(hist);
        setConn('polling');
      } catch {
        if (!cancelled) setConn('offline');
      }
    };

    const startPolling = () => {
      if (pollTimer.current) return;
      poll();
      pollTimer.current = window.setInterval(poll, 5000);
    };
    const stopPolling = () => {
      window.clearInterval(pollTimer.current);
      pollTimer.current = undefined;
    };

    const connect = () => {
      es = new EventSource('/api/stream');
      es.addEventListener('snapshot', (e) => {
        stopPolling();
        setConn('live');
        applySnapshot(JSON.parse((e as MessageEvent).data));
      });
      es.onerror = () => {
        // EventSource retries on its own; poll meanwhile so the UI stays fresh.
        setConn((c) => (c === 'live' ? 'polling' : c));
        startPolling();
      };
    };

    api.history(MAX_POINTS).then((h) => !cancelled && setHistory(h)).catch(() => {});
    connect();
    return () => {
      cancelled = true;
      es?.close();
      stopPolling();
    };
  }, [applySnapshot]);

  return (
    <LiveContext.Provider value={{ snapshot, history, incidents, conn, applySnapshot }}>
      {children}
    </LiveContext.Provider>
  );
}

export function useLive() {
  const ctx = useContext(LiveContext);
  if (!ctx) throw new Error('useLive must be used inside LiveProvider');
  return ctx;
}
