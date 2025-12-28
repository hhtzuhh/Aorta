/**
 * Header Component
 *
 * Dashboard header with title, branding, simulation clock, and connection status
 */

import { useSimulationClock } from '../contexts/ClockContext';
import { ConnectionStatus } from './ConnectionStatus';

export const Header = ({ overallStatus, connectionStatus }) => {
  const { currentTime } = useSimulationClock();

  return (
    <header className="header">
      <div className="header-content">
        <div className="header-top">
          <div>
            <h1 className="title">
              <span className="icon">🏥</span>
              Aorta
            </h1>
            <p className="subtitle">
              Multi-Patient Clinical Timeline Dashboard
              {currentTime && (
                <span className="simulation-time">
                  {' '} • Simulation Time: {currentTime}
                </span>
              )}
            </p>
          </div>
          <div className="header-status">
            <ConnectionStatus status={overallStatus} />
            <div className="stream-status">
              <span className={`status-dot ${connectionStatus.admissions}`}></span>
              Admissions
              <span className={`status-dot ${connectionStatus.labs}`} style={{ marginLeft: '16px' }}></span>
              Labs
              <span className={`status-dot ${connectionStatus.icu}`} style={{ marginLeft: '16px' }}></span>
              ICU
              <span className={`status-dot ${connectionStatus.vitals}`} style={{ marginLeft: '16px' }}></span>
              Vitals
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};
