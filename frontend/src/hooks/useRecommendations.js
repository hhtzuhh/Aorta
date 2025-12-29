/**
 * Custom hook for fetching clinical recommendations via SSE
 */

import { useState, useEffect } from 'react';

export const useRecommendations = (apiUrl = 'http://localhost:8000') => {
  const [recommendations, setRecommendations] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    console.log('[useRecommendations] Initializing...');

    // Fetch initial recommendations
    fetch(`${apiUrl}/api/clinical-recommendations`)
      .then(res => res.json())
      .then(data => {
        console.log('[useRecommendations] Fetched initial recommendations:', data.length);
        setRecommendations(data);
      })
      .catch(err => {
        console.error('[useRecommendations] Error fetching recommendations:', err);
        setError(err);
      });

    // Connect to SSE stream for real-time updates
    const eventSource = new EventSource(`${apiUrl}/stream/clinical-recommendations`);

    eventSource.addEventListener('connected', (event) => {
      console.log('[useRecommendations] Connected to recommendations stream');
      setIsConnected(true);
      setError(null);
    });

    eventSource.addEventListener('recommendation', (event) => {
      try {
        const recommendation = JSON.parse(event.data);
        console.log('[useRecommendations] Received new recommendation:', recommendation.recommendation_id);
        setRecommendations(prev => [recommendation, ...prev]);
      } catch (err) {
        console.error('[useRecommendations] Error parsing recommendation:', err);
      }
    });

    eventSource.onerror = (err) => {
      console.error('[useRecommendations] SSE error:', err);
      setIsConnected(false);
      setError(err);
    };

    return () => {
      console.log('[useRecommendations] Cleaning up SSE connection');
      eventSource.close();
    };
  }, [apiUrl]);

  // Helper function to find recommendation for a specific alert
  const getRecommendationForAlert = (subjectId, alertTime) => {
    return recommendations.find(rec =>
      rec.sepsis_alert.subject_id === subjectId &&
      Math.abs(new Date(rec.sepsis_alert.alert_time) - new Date(alertTime)) < 60000 // Within 1 minute
    );
  };

  return {
    recommendations,
    isConnected,
    error,
    getRecommendationForAlert
  };
};
