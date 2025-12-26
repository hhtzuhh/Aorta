/**
 * Header Component
 *
 * Dashboard header with title, branding, and simulation clock
 */

import { useSimulationClock } from '../contexts/ClockContext';

export const Header = () => {
  const { currentTime } = useSimulationClock();

  return (
    <header className="header">
      <div className="header-content">
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
    </header>
  );
};
