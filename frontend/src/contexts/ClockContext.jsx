/**
 * ClockContext - Simulation Clock State Management
 *
 * Provides simulation clock state to all components via React Context.
 * Polls clock service every 5 seconds and shares state across the app.
 */

import { createContext, useState, useEffect, useContext } from 'react';

export const ClockContext = createContext(null);

export const ClockProvider = ({ children }) => {
  const [currentTime, setCurrentTime] = useState(null);
  const [isRunning, setIsRunning] = useState(false);
  const [tickSizeMinutes, setTickSizeMinutes] = useState(10);
  const [tickIntervalSeconds, setTickIntervalSeconds] = useState(2.0);

  useEffect(() => {
    const CLOCK_URL = 'http://localhost:9000/status';

    const fetchClock = async () => {
      try {
        const response = await fetch(CLOCK_URL);
        if (!response.ok) {
          console.warn('Clock service returned error:', response.status);
          return;
        }

        const data = await response.json();
        setCurrentTime(data.current_time);
        setIsRunning(data.is_running);
        setTickSizeMinutes(data.tick_size_minutes);
        setTickIntervalSeconds(data.tick_interval_seconds || 2.0);  // Default to 2 seconds if not provided
      } catch (error) {
        console.warn('Clock service not available:', error.message);
      }
    };

    // Initial fetch
    fetchClock();

    // Poll at same rate as clock ticks (convert seconds to milliseconds)
    const intervalMs = tickIntervalSeconds * 1000;
    const interval = setInterval(fetchClock, intervalMs);

    return () => clearInterval(interval);
  }, [tickIntervalSeconds]);  // Re-create interval when tick rate changes

  const value = {
    currentTime,
    isRunning,
    tickSizeMinutes,
    tickIntervalSeconds
  };

  return (
    <ClockContext.Provider value={value}>
      {children}
    </ClockContext.Provider>
  );
};

/**
 * Custom hook for consuming clock context
 * @returns {Object} { currentTime, isRunning, tickSizeMinutes, tickIntervalSeconds }
 */
export const useSimulationClock = () => {
  const context = useContext(ClockContext);
  if (!context) {
    throw new Error('useSimulationClock must be used within ClockProvider');
  }
  return context;
};
