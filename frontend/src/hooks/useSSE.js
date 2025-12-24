/**
 * useSSE - React hook for Server-Sent Events (SSE)
 *
 * Manages EventSource connection for real-time admission streaming
 */

import { useState, useEffect, useRef } from 'react';

export const useSSE = (url) => {
  const [data, setData] = useState([]);
  const [connectionStatus, setConnectionStatus] = useState('connecting');
  const [error, setError] = useState(null);

  const eventSourceRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const reconnectAttempts = useRef(0);

  useEffect(() => {
    const connect = () => {
      console.log(`Connecting to SSE: ${url}`);
      setConnectionStatus('connecting');

      const eventSource = new EventSource(url);
      eventSourceRef.current = eventSource;

      // Connection opened
      eventSource.addEventListener('connected', (event) => {
        console.log('SSE Connected:', event.data);
        setConnectionStatus('connected');
        setError(null);
        reconnectAttempts.current = 0;
      });

      eventSource.addEventListener('open', () => {
        console.log('SSE Connection opened');
        setConnectionStatus('connected');
        reconnectAttempts.current = 0;
      });

      // Admission event received
      eventSource.addEventListener('admission', (event) => {
        try {
          const admission = JSON.parse(event.data);

          setData((prevData) => {
            // Add new admission at the beginning
            const newData = [admission, ...prevData];
            // Keep only last 100 admissions
            return newData.slice(0, 100);
          });
        } catch (err) {
          console.error('Failed to parse admission event:', err);
        }
      });

      // Connection error
      eventSource.onerror = (err) => {
        console.error('SSE Error:', err);
        setConnectionStatus('disconnected');
        setError('Connection lost');

        // Close the connection
        eventSource.close();

        // Attempt reconnection with exponential backoff
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempts.current), 30000);
        reconnectAttempts.current += 1;

        console.log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts.current})...`);

        reconnectTimeoutRef.current = setTimeout(() => {
          connect();
        }, delay);
      };
    };

    // Initial connection
    connect();

    // Cleanup on unmount
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
    };
  }, [url]);

  return { data, connectionStatus, error };
};
