/**
 * Aorta - Real-Time Hospital Admission Dashboard
 *
 * Main application component
 */

import { useSSE } from './hooks/useSSE';
import { Header } from './components/Header';
import { ConnectionStatus } from './components/ConnectionStatus';
import { AdmissionFeed } from './components/AdmissionFeed';
import './App.css';

function App() {
  // Connect to SSE stream
  const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
  const { data: admissions, connectionStatus } = useSSE(`${API_URL}/stream/admissions`);

  // Calculate statistics
  const totalAdmissions = admissions.length;
  const highPriorityCount = admissions.filter(a => a.is_high_priority).length;
  const priorityRate = totalAdmissions > 0
    ? ((highPriorityCount / totalAdmissions) * 100).toFixed(1)
    : 0;

  return (
    <div className="app">
      <Header />

      <div className="dashboard-controls">
        <ConnectionStatus status={connectionStatus} />
      </div>

      <div className="stats-bar">
        <div className="stat-card">
          <div className="stat-value">{totalAdmissions}</div>
          <div className="stat-label">Total Admissions</div>
        </div>

        <div className="stat-card highlight">
          <div className="stat-value">{highPriorityCount}</div>
          <div className="stat-label">High Priority</div>
        </div>

        <div className="stat-card">
          <div className="stat-value">{priorityRate}%</div>
          <div className="stat-label">Priority Rate</div>
        </div>
      </div>

      <main className="dashboard-main">
        <AdmissionFeed admissions={admissions} />
      </main>

      <footer className="dashboard-footer">
        <p>Aorta v0.1.0 • Real-time monitoring powered by Confluent Cloud + FastAPI</p>
      </footer>
    </div>
  );
}

export default App;
