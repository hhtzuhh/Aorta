/**
 * TimelineContainer - Main container for patient timeline visualization
 *
 * Manages D3 scales, handles resizing, and orchestrates timeline rendering
 */

import { useState, useEffect, useRef, useMemo } from 'react';
import * as d3 from 'd3';
import TimelineAxis from './TimelineAxis';
import TimelineRow from './TimelineRow';
import PatientDetailPanel from './PatientDetailPanel';
import { useSimulationClock } from '../../contexts/ClockContext';
import './Timeline.css';

const TimelineContainer = ({ patients }) => {
  const containerRef = useRef(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0 });
  const [zoomDays, setZoomDays] = useState(5); // Default: 5-day view
  const [selectedPatient, setSelectedPatient] = useState(null); // For patient detail panel
  const { currentTime } = useSimulationClock();

  // Handle container resize
  useEffect(() => {
    if (!containerRef.current) return;

    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width } = entry.contentRect;
        setDimensions({ width, height: 600 });
      }
    });

    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
    };
  }, []);

  // Calculate time scale using useMemo (synchronous, re-calculates when dependencies change)
  const timeScale = useMemo(() => {
    if (!patients || patients.length === 0 || dimensions.width === 0) {
      return null;
    }

    // Use current simulation time as center, or fallback to earliest event
    let centerTime;
    if (currentTime) {
      centerTime = new Date(currentTime);
    } else {
      // Find earliest event as fallback
      let minTime = null;
      patients.forEach((patient) => {
        patient.admissions.forEach((admission) => {
          const admitTime = new Date(admission.event_time);
          if (!minTime || admitTime < minTime) minTime = admitTime;
        });
        patient.labs.forEach((lab) => {
          const labTime = new Date(lab.event_time);
          if (!minTime || labTime < minTime) minTime = labTime;
        });
        // Also check ICU stays for earliest time
        if (patient.icuStays) {
          patient.icuStays.forEach((stay) => {
            const stayTime = new Date(stay.intime);
            if (!minTime || stayTime < minTime) minTime = stayTime;
          });
        }
      });
      if (!minTime) return null;
      centerTime = minTime;
    }

    // Create fixed window around center time (default 5 days)
    const halfWindow = (zoomDays * 24 * 60 * 60 * 1000) / 2;  // Half of window in ms
    const windowStart = new Date(centerTime.getTime() - halfWindow);
    const windowEnd = new Date(centerTime.getTime() + halfWindow);

    // Create D3 time scale
    // Use larger width for better detail (1200px base * zoom factor)
    // This makes the timeline scrollable
    const timelineWidth = Math.max(1200, dimensions.width * 1.5);
    const scale = d3
      .scaleTime()
      .domain([windowStart, windowEnd])
      .range([70, timelineWidth]);

    return scale;
  }, [patients, dimensions.width, currentTime, zoomDays]);

  if (!patients || patients.length === 0) {
    return (
      <div className="timeline-container" ref={containerRef}>
        <div className="timeline-empty">
          <p>Waiting for patient data...</p>
        </div>
      </div>
    );
  }

  if (!timeScale) {
    return (
      <div className="timeline-container" ref={containerRef}>
        <div className="timeline-loading">
          <p>Preparing timeline...</p>
        </div>
      </div>
    );
  }

  return (
    <>
      {/* Zoom controls - outside timeline container */}
      <div className="timeline-zoom-controls-external">
        <span className="zoom-label-text">Timeline View:</span>
        <button
          onClick={() => setZoomDays(Math.min(zoomDays * 2, 30))}
          disabled={zoomDays >= 30}
          className="zoom-btn"
          title="Zoom out (show more days)"
        >
          −
        </button>
        <span className="zoom-label">{zoomDays}d</span>
        <button
          onClick={() => setZoomDays(Math.max(zoomDays / 2, 1))}
          disabled={zoomDays <= 1}
          className="zoom-btn"
          title="Zoom in (show fewer days)"
        >
          +
        </button>
      </div>

      <div className="timeline-container" ref={containerRef}>
      {/* Scrollable patient rows */}
      <div className="timeline-rows">
        {patients.map((patient, index) => (
          <TimelineRow
            key={patient.patient.subject_id}
            patient={patient}
            timeScale={timeScale}
            width={dimensions.width}
            index={index}
            onRowClick={() => setSelectedPatient(patient)}
            onLabClick={(labs) => {
              // Open patient panel and select the lab
              setSelectedPatient(patient);
            }}
            onSepsisAlertClick={(alert) => {
              // Open patient panel when sepsis alert is clicked
              setSelectedPatient(patient);
            }}
            icuStays={patient.icuStays || []}
          />
        ))}
      </div>

      {/* Time axis at bottom */}
      <TimelineAxis timeScale={timeScale} width={dimensions.width} />
    </div>

    {/* Patient Detail Panel (replaces old lab panel and ICU expansion) */}
    {selectedPatient && (
      <PatientDetailPanel
        patient={selectedPatient}
        icuStays={selectedPatient.icuStays || []}
        chartevents={selectedPatient.chartevents || []}
        onClose={() => setSelectedPatient(null)}
      />
    )}
    </>
  );
};

export default TimelineContainer;
