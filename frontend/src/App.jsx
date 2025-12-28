/**
 * Aorta - Real-Time Hospital Timeline Dashboard
 *
 * Main application component with multi-patient timeline visualization
 */

import { useMultiStreamSSE } from './hooks/useMultiStreamSSE';
import { Header } from './components/Header';
import TimelineContainer from './components/Timeline/TimelineContainer';
import ErrorBoundary from './components/ErrorBoundary';
import { ClockProvider } from './contexts/ClockContext';
import './App.css';

function App() {
  // Connect to all SSE streams
  const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
  const { patients, connectionStatus } = useMultiStreamSSE(
    `${API_URL}/stream/admissions`,
    `${API_URL}/stream/labs`,
    `${API_URL}/stream/icu-admissions`,
    `${API_URL}/stream/vitals`,
    20 // Max patients
  );

  // Calculate statistics
  const totalAdmissions = patients.reduce((sum, p) => sum + p.admissions.length, 0);
  const totalLabs = patients.reduce((sum, p) => sum + p.labs.length, 0);
  const icuPatients = patients.filter(p => p.icuStays && p.icuStays.length > 0).length;
  const abnormalLabs = patients.reduce(
    (sum, p) => sum + p.labs.filter(lab =>
      lab.lab.flag && lab.lab.flag.toUpperCase() === 'ABNORMAL'
    ).length,
    0
  );

  // Overall connection status
  const allConnected = connectionStatus.admissions === 'connected' &&
                       connectionStatus.labs === 'connected' &&
                       connectionStatus.icu === 'connected' &&
                       connectionStatus.vitals === 'connected';
  const anyConnecting = connectionStatus.admissions === 'connecting' ||
                        connectionStatus.labs === 'connecting' ||
                        connectionStatus.icu === 'connecting' ||
                        connectionStatus.vitals === 'connecting';
  const overallStatus = allConnected ? 'connected' : (anyConnecting ? 'connecting' : 'disconnected');

  return (
    <ClockProvider>
      <div className="app">
        <Header
          overallStatus={overallStatus}
          connectionStatus={connectionStatus}
        />

      <div className="stats-bar">
        <div className="stat-card">
          <div className="stat-label">Patients</div>
          <strong>{patients.length}</strong>
        </div>

        <div className="stat-card">
          <div className="stat-label">Admissions</div>
          <strong>{totalAdmissions}</strong>
        </div>

        <div className="stat-card highlight">
          <div className="stat-label">ICU Patients</div>
          <strong>{icuPatients}</strong>
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
            <span className="legend-title">Care Levels:</span>
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
            <div className="legend-item">
              <div className="legend-color-box" style={{ background: '#f97316', border: '2px solid #fff' }}></div>
              ICU Stay
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
