/**
 * Aorta - Real-Time Hospital Timeline Dashboard
 *
 * Main application component with multi-patient timeline visualization
 */

import { useMultiStreamSSE } from './hooks/useMultiStreamSSE';
import { Header } from './components/Header';
import { ConnectionStatus } from './components/ConnectionStatus';
import TimelineContainer from './components/Timeline/TimelineContainer';
import ErrorBoundary from './components/ErrorBoundary';
import { ClockProvider } from './contexts/ClockContext';
import './App.css';

function App() {
  // Connect to both SSE streams
  const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
  const { patients, connectionStatus } = useMultiStreamSSE(
    `${API_URL}/stream/admissions`,
    `${API_URL}/stream/labs`,
    20 // Max patients
  );

  // Calculate statistics
  const totalAdmissions = patients.reduce((sum, p) => sum + p.admissions.length, 0);
  const totalLabs = patients.reduce((sum, p) => sum + p.labs.length, 0);
  const abnormalLabs = patients.reduce(
    (sum, p) => sum + p.labs.filter(lab =>
      lab.lab.flag && lab.lab.flag.toUpperCase() === 'ABNORMAL'
    ).length,
    0
  );

  // Overall connection status
  const overallStatus =
    connectionStatus.admissions === 'connected' && connectionStatus.labs === 'connected'
      ? 'connected'
      : connectionStatus.admissions === 'connecting' || connectionStatus.labs === 'connecting'
      ? 'connecting'
      : 'disconnected';

  return (
    <ClockProvider>
      <div className="app">
        <Header />

      <div className="dashboard-controls">
        <ConnectionStatus status={overallStatus} />
        <div className="stream-status">
          <span className={`status-dot ${connectionStatus.admissions}`}></span>
          Admissions
          <span className={`status-dot ${connectionStatus.labs}`} style={{ marginLeft: '16px' }}></span>
          Labs
        </div>
      </div>

      <div className="stats-bar">
        <div className="stat-card">
          <div className="stat-label">Patients</div>
          <strong>{patients.length}</strong>
        </div>

        <div className="stat-card">
          <div className="stat-label">Admissions</div>
          <strong>{totalAdmissions}</strong>
        </div>

        <div className="stat-card">
          <div className="stat-label">Labs</div>
          <strong>{totalLabs}</strong>
        </div>

        <div className="stat-card highlight">
          <div className="stat-label">Abnormal Labs</div>
          <strong>{abnormalLabs}</strong>
        </div>
      </div>

      <main className="dashboard-main">
        <ErrorBoundary>
          <TimelineContainer patients={patients} />
        </ErrorBoundary>

        {/* Timeline Legend */}
        <div className="timeline-legend">
          <div className="legend-section">
            <span className="legend-title">Admission Types:</span>
            <div className="legend-item">
              <div className="legend-color-box" style={{ background: '#dc2626' }}></div>
              Emergency/Urgent
            </div>
            <div className="legend-item">
              <div className="legend-color-box" style={{ background: '#3b82f6' }}></div>
              Elective
            </div>
            <div className="legend-item">
              <div className="legend-color-box" style={{ background: '#8b5cf6' }}></div>
              Observation
            </div>
          </div>

          <div className="legend-section">
            <span className="legend-title">Lab Results:</span>
            <div className="legend-item">
              <div className="legend-color-circle" style={{
                background: '#6366f1',
                color: 'white',
                fontSize: '10px',
                fontWeight: 'bold',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '20px',
                height: '20px'
              }}>L</div>
              Click lab dots to view details
            </div>
          </div>
        </div>
      </main>

      <footer className="dashboard-footer">
        <p>Aorta v0.2.0 • Multi-patient timeline powered by Confluent Cloud + FastAPI + D3.js</p>
      </footer>
      </div>
    </ClockProvider>
  );
}

export default App;
