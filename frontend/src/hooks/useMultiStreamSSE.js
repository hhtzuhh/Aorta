/**
 * useMultiStreamSSE - React hook for coordinating dual SSE streams
 *
 * Manages connections to both admission and lab event streams,
 * groups events by patient_id, and maintains FIFO eviction.
 */

import { useState, useEffect, useRef } from 'react';

export const useMultiStreamSSE = (admissionsUrl, labsUrl, maxPatients = 20) => {
  const [patients, setPatients] = useState(new Map());
  const [connectionStatus, setConnectionStatus] = useState({
    admissions: 'connecting',
    labs: 'connecting'
  });

  const admissionSourceRef = useRef(null);
  const labSourceRef = useRef(null);
  const reconnectTimeoutsRef = useRef({ admissions: null, labs: null });
  const reconnectAttemptsRef = useRef({ admissions: 0, labs: 0 });

  useEffect(() => {
    // Connect to admissions stream
    const connectAdmissions = () => {
      console.log(`Connecting to admission stream: ${admissionsUrl}`);
      setConnectionStatus(prev => ({ ...prev, admissions: 'connecting' }));

      const eventSource = new EventSource(admissionsUrl);
      admissionSourceRef.current = eventSource;

      eventSource.addEventListener('connected', (event) => {
        console.log('Admission stream connected:', event.data);
        setConnectionStatus(prev => ({ ...prev, admissions: 'connected' }));
        reconnectAttemptsRef.current.admissions = 0;
      });

      eventSource.addEventListener('admission', (event) => {
        try {
          const admission = JSON.parse(event.data);

          setPatients((prevPatients) => {
            const newPatients = new Map(prevPatients);
            const patientId = admission.patient.subject_id;

            if (newPatients.has(patientId)) {
              // Add admission to existing patient
              const patientData = newPatients.get(patientId);
              newPatients.set(patientId, {
                ...patientData,
                admissions: [...patientData.admissions, admission].sort(
                  (a, b) => new Date(a.event_time) - new Date(b.event_time)
                )
              });
            } else {
              // Create new patient entry
              const newEntry = {
                patient: admission.patient,
                admissions: [admission],
                labs: [],
                firstSeen: new Date(admission.event_time)
              };

              // FIFO eviction if exceeding max patients
              if (newPatients.size >= maxPatients) {
                // Find oldest patient by firstSeen timestamp
                let oldestKey = null;
                let oldestTime = new Date();

                for (const [key, value] of newPatients.entries()) {
                  if (value.firstSeen < oldestTime) {
                    oldestTime = value.firstSeen;
                    oldestKey = key;
                  }
                }

                if (oldestKey) {
                  newPatients.delete(oldestKey);
                }
              }

              newPatients.set(patientId, newEntry);
            }

            return newPatients;
          });
        } catch (err) {
          console.error('Failed to parse admission event:', err);
        }
      });

      eventSource.onerror = (err) => {
        console.error('Admission stream error:', err);
        setConnectionStatus(prev => ({ ...prev, admissions: 'disconnected' }));
        eventSource.close();

        const delay = Math.min(
          1000 * Math.pow(2, reconnectAttemptsRef.current.admissions),
          30000
        );
        reconnectAttemptsRef.current.admissions += 1;

        console.log(`Reconnecting to admissions in ${delay}ms...`);
        reconnectTimeoutsRef.current.admissions = setTimeout(connectAdmissions, delay);
      };
    };

    // Connect to labs stream
    const connectLabs = () => {
      console.log(`Connecting to lab stream: ${labsUrl}`);
      setConnectionStatus(prev => ({ ...prev, labs: 'connecting' }));

      const eventSource = new EventSource(labsUrl);
      labSourceRef.current = eventSource;

      eventSource.addEventListener('connected', (event) => {
        console.log('Lab stream connected:', event.data);
        setConnectionStatus(prev => ({ ...prev, labs: 'connected' }));
        reconnectAttemptsRef.current.labs = 0;
      });

      eventSource.addEventListener('lab', (event) => {
        try {
          const lab = JSON.parse(event.data);

          setPatients((prevPatients) => {
            const newPatients = new Map(prevPatients);
            const patientId = lab.patient.subject_id;

            if (newPatients.has(patientId)) {
              // Add lab to existing patient
              const patientData = newPatients.get(patientId);
              newPatients.set(patientId, {
                ...patientData,
                labs: [...patientData.labs, lab].sort(
                  (a, b) => new Date(a.event_time) - new Date(b.event_time)
                )
              });
            } else {
              // Handle late-arriving labs (before admission event)
              const newEntry = {
                patient: lab.patient,
                admissions: [],
                labs: [lab],
                firstSeen: new Date(lab.event_time)
              };

              // FIFO eviction if exceeding max patients
              if (newPatients.size >= maxPatients) {
                let oldestKey = null;
                let oldestTime = new Date();

                for (const [key, value] of newPatients.entries()) {
                  if (value.firstSeen < oldestTime) {
                    oldestTime = value.firstSeen;
                    oldestKey = key;
                  }
                }

                if (oldestKey) {
                  newPatients.delete(oldestKey);
                }
              }

              newPatients.set(patientId, newEntry);
            }

            return newPatients;
          });
        } catch (err) {
          console.error('Failed to parse lab event:', err);
        }
      });

      eventSource.onerror = (err) => {
        console.error('Lab stream error:', err);
        setConnectionStatus(prev => ({ ...prev, labs: 'disconnected' }));
        eventSource.close();

        const delay = Math.min(
          1000 * Math.pow(2, reconnectAttemptsRef.current.labs),
          30000
        );
        reconnectAttemptsRef.current.labs += 1;

        console.log(`Reconnecting to labs in ${delay}ms...`);
        reconnectTimeoutsRef.current.labs = setTimeout(connectLabs, delay);
      };
    };

    // Initial connections
    connectAdmissions();
    connectLabs();

    // Cleanup on unmount
    return () => {
      if (admissionSourceRef.current) {
        admissionSourceRef.current.close();
      }
      if (labSourceRef.current) {
        labSourceRef.current.close();
      }
      if (reconnectTimeoutsRef.current.admissions) {
        clearTimeout(reconnectTimeoutsRef.current.admissions);
      }
      if (reconnectTimeoutsRef.current.labs) {
        clearTimeout(reconnectTimeoutsRef.current.labs);
      }
    };
  }, [admissionsUrl, labsUrl, maxPatients]);

  // Convert Map to Array for rendering
  const patientArray = Array.from(patients.values()).sort(
    (a, b) => b.firstSeen - a.firstSeen
  );

  return {
    patients: patientArray,
    patientMap: patients,
    connectionStatus
  };
};
