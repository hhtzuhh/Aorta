/**
 * useMultiStreamSSE - React hook for coordinating multiple SSE streams
 *
 * Manages connections to admission, lab, ICU, and vitals event streams,
 * groups events by patient_id, and maintains FIFO eviction.
 */

import { useState, useEffect, useRef } from 'react';

export const useMultiStreamSSE = (admissionsUrl, labsUrl, icuUrl, vitalsUrl, sepsisAlertsUrl, maxPatients = 20) => {
  const [patients, setPatients] = useState(new Map());
  const [connectionStatus, setConnectionStatus] = useState({
    admissions: 'connecting',
    labs: 'connecting',
    icu: 'connecting',
    vitals: 'connecting',
    sepsisAlerts: 'connecting'
  });

  const admissionSourceRef = useRef(null);
  const labSourceRef = useRef(null);
  const icuSourceRef = useRef(null);
  const vitalsSourceRef = useRef(null);
  const sepsisAlertsSourceRef = useRef(null);
  const reconnectTimeoutsRef = useRef({ admissions: null, labs: null, icu: null, vitals: null, sepsisAlerts: null });
  const reconnectAttemptsRef = useRef({ admissions: 0, labs: 0, icu: 0, vitals: 0, sepsisAlerts: 0 });

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
                icuStays: [],
                chartevents: {},
                sepsisAlerts: [],
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
                icuStays: [],
                chartevents: {},
                sepsisAlerts: [],
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

    // Connect to ICU stream
    const connectICU = () => {
      console.log(`Connecting to ICU stream: ${icuUrl}`);
      setConnectionStatus(prev => ({ ...prev, icu: 'connecting' }));

      const eventSource = new EventSource(icuUrl);
      icuSourceRef.current = eventSource;

      eventSource.addEventListener('connected', (event) => {
        console.log('ICU stream connected:', event.data);
        setConnectionStatus(prev => ({ ...prev, icu: 'connected' }));
        reconnectAttemptsRef.current.icu = 0;
      });

      eventSource.addEventListener('icu', (event) => {
        try {
          const icuAdmission = JSON.parse(event.data);

          setPatients((prevPatients) => {
            const newPatients = new Map(prevPatients);
            const patientId = icuAdmission.patient.subject_id;

            if (newPatients.has(patientId)) {
              // Add ICU stay to existing patient
              const patientData = newPatients.get(patientId);
              const existingStay = patientData.icuStays.find(
                stay => stay.stay_id === icuAdmission.icu_stay.stay_id
              );

              if (!existingStay) {
                newPatients.set(patientId, {
                  ...patientData,
                  icuStays: [...patientData.icuStays, {
                    stay_id: icuAdmission.icu_stay.stay_id,
                    intime: icuAdmission.icu_stay.intime,
                    outtime: icuAdmission.icu_stay.outtime,
                    careunit: icuAdmission.icu_stay.first_careunit || icuAdmission.icu_stay.last_careunit,
                    first_careunit: icuAdmission.icu_stay.first_careunit,
                    last_careunit: icuAdmission.icu_stay.last_careunit,
                    los_days: icuAdmission.icu_stay.los_days,
                    status: icuAdmission.icu_stay.status
                  }]
                });
              }
            } else {
              // Create new patient entry for ICU admission
              const newEntry = {
                patient: icuAdmission.patient,
                admissions: [],
                labs: [],
                icuStays: [{
                  stay_id: icuAdmission.icu_stay.stay_id,
                  intime: icuAdmission.icu_stay.intime,
                  outtime: icuAdmission.icu_stay.outtime,
                  careunit: icuAdmission.icu_stay.first_careunit || icuAdmission.icu_stay.last_careunit,
                  first_careunit: icuAdmission.icu_stay.first_careunit,
                  last_careunit: icuAdmission.icu_stay.last_careunit,
                  los_days: icuAdmission.icu_stay.los_days,
                  status: icuAdmission.icu_stay.status
                }],
                chartevents: {},
                sepsisAlerts: [],
                firstSeen: new Date(icuAdmission.event_time)
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
          console.error('Failed to parse ICU event:', err);
        }
      });

      eventSource.onerror = (err) => {
        console.error('ICU stream error:', err);
        setConnectionStatus(prev => ({ ...prev, icu: 'disconnected' }));
        eventSource.close();

        const delay = Math.min(
          1000 * Math.pow(2, reconnectAttemptsRef.current.icu),
          30000
        );
        reconnectAttemptsRef.current.icu += 1;

        console.log(`Reconnecting to ICU in ${delay}ms...`);
        reconnectTimeoutsRef.current.icu = setTimeout(connectICU, delay);
      };
    };

    // Connect to vitals stream
    const connectVitals = () => {
      console.log(`Connecting to vitals stream: ${vitalsUrl}`);
      setConnectionStatus(prev => ({ ...prev, vitals: 'connecting' }));

      const eventSource = new EventSource(vitalsUrl);
      vitalsSourceRef.current = eventSource;

      eventSource.addEventListener('connected', (event) => {
        console.log('Vitals stream connected:', event.data);
        setConnectionStatus(prev => ({ ...prev, vitals: 'connected' }));
        reconnectAttemptsRef.current.vitals = 0;
      });

      eventSource.addEventListener('chartevent', (event) => {
        try {
          const charteventData = JSON.parse(event.data);

          setPatients((prevPatients) => {
            const newPatients = new Map(prevPatients);
            const patientId = charteventData.patient.subject_id;

            if (newPatients.has(patientId)) {
              // Add chartevent to existing patient
              const patientData = newPatients.get(patientId);
              const chartevents = { ...patientData.chartevents };
              const itemid = charteventData.chartevent.itemid;

              if (!chartevents[itemid]) {
                chartevents[itemid] = {
                  label: charteventData.chartevent.label,
                  category: charteventData.chartevent.category,
                  param_type: charteventData.chartevent.param_type,
                  unit: charteventData.chartevent.unit,
                  values: []
                };
              }

              // Limit values to last 100 points per itemid
              const values = [...chartevents[itemid].values, {
                time: charteventData.event_time,
                value_numeric: charteventData.chartevent.value_numeric,
                value_text: charteventData.chartevent.value_text
              }];

              if (values.length > 100) {
                values.shift();
              }

              chartevents[itemid].values = values;

              newPatients.set(patientId, {
                ...patientData,
                chartevents
              });
            } else {
              // Create new patient entry for chartevent
              const itemid = charteventData.chartevent.itemid;
              const newEntry = {
                patient: charteventData.patient,
                admissions: [],
                labs: [],
                icuStays: [],
                chartevents: {
                  [itemid]: {
                    label: charteventData.chartevent.label,
                    category: charteventData.chartevent.category,
                    param_type: charteventData.chartevent.param_type,
                    unit: charteventData.chartevent.unit,
                    values: [{
                      time: charteventData.event_time,
                      value_numeric: charteventData.chartevent.value_numeric,
                      value_text: charteventData.chartevent.value_text
                    }]
                  }
                },
                sepsisAlerts: [],
                firstSeen: new Date(charteventData.event_time)
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
          console.error('Failed to parse chartevent:', err);
        }
      });

      eventSource.onerror = (err) => {
        console.error('Vitals stream error:', err);
        setConnectionStatus(prev => ({ ...prev, vitals: 'disconnected' }));
        eventSource.close();

        const delay = Math.min(
          1000 * Math.pow(2, reconnectAttemptsRef.current.vitals),
          30000
        );
        reconnectAttemptsRef.current.vitals += 1;

        console.log(`Reconnecting to vitals in ${delay}ms...`);
        reconnectTimeoutsRef.current.vitals = setTimeout(connectVitals, delay);
      };
    };

    // Connect to sepsis alerts stream
    const connectSepsisAlerts = () => {
      console.log(`Connecting to sepsis alerts stream: ${sepsisAlertsUrl}`);
      setConnectionStatus(prev => ({ ...prev, sepsisAlerts: 'connecting' }));

      const eventSource = new EventSource(sepsisAlertsUrl);
      sepsisAlertsSourceRef.current = eventSource;

      eventSource.addEventListener('connected', (event) => {
        console.log('Sepsis alerts stream connected:', event.data);
        setConnectionStatus(prev => ({ ...prev, sepsisAlerts: 'connected' }));
        reconnectAttemptsRef.current.sepsisAlerts = 0;
      });

      eventSource.addEventListener('sepsis-alert', (event) => {
        try {
          const alert = JSON.parse(event.data);

          setPatients((prevPatients) => {
            const newPatients = new Map(prevPatients);
            const patientId = alert.patient.subject_id;

            if (newPatients.has(patientId)) {
              // Add sepsis alert to existing patient
              const patientData = newPatients.get(patientId);
              newPatients.set(patientId, {
                ...patientData,
                sepsisAlerts: [...patientData.sepsisAlerts, alert].sort(
                  (a, b) => new Date(a.event_time) - new Date(b.event_time)
                )
              });
            } else {
              // Create new patient entry for sepsis alert
              const newEntry = {
                patient: alert.patient,
                admissions: [],
                labs: [],
                icuStays: [],
                chartevents: {},
                sepsisAlerts: [alert],
                firstSeen: new Date(alert.event_time)
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
          console.error('Failed to parse sepsis alert event:', err);
        }
      });

      eventSource.onerror = (err) => {
        console.error('Sepsis alerts stream error:', err);
        setConnectionStatus(prev => ({ ...prev, sepsisAlerts: 'disconnected' }));
        eventSource.close();

        const delay = Math.min(
          1000 * Math.pow(2, reconnectAttemptsRef.current.sepsisAlerts),
          30000
        );
        reconnectAttemptsRef.current.sepsisAlerts += 1;

        console.log(`Reconnecting to sepsis alerts in ${delay}ms...`);
        reconnectTimeoutsRef.current.sepsisAlerts = setTimeout(connectSepsisAlerts, delay);
      };
    };

    // Initial connections
    connectAdmissions();
    connectLabs();
    connectICU();
    connectVitals();
    connectSepsisAlerts();

    // Cleanup on unmount
    return () => {
      if (admissionSourceRef.current) {
        admissionSourceRef.current.close();
      }
      if (labSourceRef.current) {
        labSourceRef.current.close();
      }
      if (icuSourceRef.current) {
        icuSourceRef.current.close();
      }
      if (vitalsSourceRef.current) {
        vitalsSourceRef.current.close();
      }
      if (sepsisAlertsSourceRef.current) {
        sepsisAlertsSourceRef.current.close();
      }
      if (reconnectTimeoutsRef.current.admissions) {
        clearTimeout(reconnectTimeoutsRef.current.admissions);
      }
      if (reconnectTimeoutsRef.current.labs) {
        clearTimeout(reconnectTimeoutsRef.current.labs);
      }
      if (reconnectTimeoutsRef.current.icu) {
        clearTimeout(reconnectTimeoutsRef.current.icu);
      }
      if (reconnectTimeoutsRef.current.vitals) {
        clearTimeout(reconnectTimeoutsRef.current.vitals);
      }
      if (reconnectTimeoutsRef.current.sepsisAlerts) {
        clearTimeout(reconnectTimeoutsRef.current.sepsisAlerts);
      }
    };
  }, [admissionsUrl, labsUrl, icuUrl, vitalsUrl, sepsisAlertsUrl, maxPatients]);

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
