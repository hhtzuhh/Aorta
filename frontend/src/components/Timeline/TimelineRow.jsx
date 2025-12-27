/**
 * TimelineRow - Single patient row with D3 SVG rendering
 *
 * Renders admission segments as colored bars and lab events as dots
 */

import { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { useSimulationClock } from '../../contexts/ClockContext';
import CharteventPanel from './CharteventPanel';

const TimelineRow = ({
  patient,
  timeScale,
  width,
  index,
  onLabClick,
  icuStays = [],
  chartevents = {},
  isExpanded = false,
  onToggleExpand
}) => {
  const svgRef = useRef(null);
  const [tooltip, setTooltip] = useState(null);
  const { currentTime } = useSimulationClock();

  const ROW_HEIGHT = 60;
  const BAR_HEIGHT = 30;

  // Check if patient has ICU stay
  const hasICU = icuStays && icuStays.length > 0;

  // Color mapping for admission types
  const admissionColors = {
    'EMERGENCY': '#dc2626',      // Red
    'URGENT': '#f59e0b',          // Orange
    'EW EMER.': '#dc2626',        // Red
    'ELECTIVE': '#3b82f6',        // Blue
    'OBSERVATION': '#8b5cf6',     // Purple
    'NEWBORN': '#10b981',         // Green
    'SURGICAL SAME DAY ADMISSION': '#06b6d4', // Cyan
    'EU OBSERVATION': '#8b5cf6'   // Purple
  };

  const getAdmissionColor = (type) => {
    // Override with ICU red color if patient has ICU stay
    if (hasICU) {
      return '#ef4444'; // Tailwind red-500 for ICU patients
    }
    return admissionColors[type] || '#6b7280'; // Default gray
  };

  useEffect(() => {
    if (!svgRef.current || !timeScale) {
      return;
    }

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const g = svg.append('g');

    // Draw admission segments (ghost + growing bars)
    patient.admissions.forEach((admission, admIdx) => {

      const admitTime = new Date(admission.event_time);
      const plannedDischarge = admission.discharge.time
        ? new Date(admission.discharge.time)
        : new Date(admitTime.getTime() + 24 * 60 * 60 * 1000); // Default 1 day

      const currentSimTime = currentTime ? new Date(currentTime) : new Date();

      // Calculate positions
      const x1 = timeScale(admitTime);
      const xPlanned = timeScale(plannedDischarge);

      // GHOST BAR: Full planned duration (faint, dashed)
      g.append('rect')
        .attr('x', x1)
        .attr('y', (ROW_HEIGHT - BAR_HEIGHT) / 2)
        .attr('width', Math.max(xPlanned - x1, 2))
        .attr('height', BAR_HEIGHT)
        .attr('rx', 4)
        .attr('fill', getAdmissionColor(admission.admission.type))
        .attr('opacity', 0.2)  // Faint
        .attr('stroke', getAdmissionColor(admission.admission.type))
        .attr('stroke-width', 1)
        .attr('stroke-dasharray', '4,4');  // Dashed outline

      // Skip solid bar if admission hasn't started yet
      if (currentSimTime < admitTime) {
        return;
      }

      // Current bar ends at MIN(currentSimTime, plannedDischarge)
      const actualEndTime = currentSimTime < plannedDischarge ? currentSimTime : plannedDischarge;
      const x2 = timeScale(actualEndTime);

      // Determine opacity based on completion status
      const isCompleted = currentSimTime >= plannedDischarge;
      const barOpacity = isCompleted ? 0.5 : 0.7;

      // SOLID BAR: Current progress (admit → currentTime)
      const rect = g
        .append('rect')
        .attr('x', x1)
        .attr('y', (ROW_HEIGHT - BAR_HEIGHT) / 2)
        .attr('width', Math.max(x2 - x1, 2))
        .attr('height', BAR_HEIGHT)
        .attr('rx', 4)
        .attr('fill', getAdmissionColor(admission.admission.type))
        .attr('opacity', barOpacity)
        .attr('stroke', '#fff')
        .attr('stroke-width', 1)
        .style('cursor', 'pointer');

      // Admission hover events
      rect.on('mouseenter', (event) => {
        rect.attr('opacity', 1);
        setTooltip({
          x: event.pageX,
          y: event.pageY,
          content: {
            type: 'admission',
            data: admission
          }
        });
      });

      rect.on('mousemove', (event) => {
        setTooltip(prev => ({
          ...prev,
          x: event.pageX,
          y: event.pageY
        }));
      });

      rect.on('mouseleave', () => {
        rect.attr('opacity', 0.7);
        setTooltip(null);
      });

      // Group labs by timestamp within this admission period
      const labsByTime = new Map();
      patient.labs.forEach((lab) => {
        const labTime = new Date(lab.event_time);
        if (labTime >= admitTime && labTime <= plannedDischarge) {
          const timeKey = labTime.getTime();
          if (!labsByTime.has(timeKey)) {
            labsByTime.set(timeKey, []);
          }
          labsByTime.get(timeKey).push(lab);
        }
      });

      // Draw grouped lab markers
      labsByTime.forEach((labs, timeKey) => {
        const labTime = new Date(timeKey);
        const labX = timeScale(labTime);
        const labY = ROW_HEIGHT / 2;

        const isMultiple = labs.length > 1;

        const circle = g
          .append('circle')
          .attr('cx', labX)
          .attr('cy', labY)
          .attr('r', isMultiple ? 7 : 6)  // Slightly larger to fit "L"
          .attr('fill', '#6366f1')  // Neutral indigo color
          .attr('stroke', '#fff')
          .attr('stroke-width', 2)
          .style('cursor', 'pointer');

        // Add "L" label for all labs
        g.append('text')
          .attr('x', labX)
          .attr('y', labY)
          .attr('text-anchor', 'middle')
          .attr('dominant-baseline', 'middle')
          .attr('font-size', '10px')
          .attr('font-weight', 'bold')
          .attr('fill', '#fff')
          .style('pointer-events', 'none')
          .text('L');

        // Hover events
        circle.on('mouseenter', (event) => {
          circle.attr('r', isMultiple ? 9 : 8);
          setTooltip({
            x: event.pageX,
            y: event.pageY,
            content: {
              type: 'lab-group',
              data: labs,
              count: labs.length
            }
          });
        });

        circle.on('mousemove', (event) => {
          setTooltip(prev => ({
            ...prev,
            x: event.pageX,
            y: event.pageY
          }));
        });

        circle.on('mouseleave', () => {
          circle.attr('r', isMultiple ? 7 : 6);
          setTooltip(null);
        });

        // Click event - show detail panel
        circle.on('click', (event) => {
          event.stopPropagation();
          if (onLabClick) {
            onLabClick(labs);
          }
        });
      });
    });

    // Draw current time indicator (vertical line)
    if (currentTime) {
      const currentSimTime = new Date(currentTime);
      const xCurrent = timeScale(currentSimTime);

      // Vertical dashed line
      g.append('line')
        .attr('x1', xCurrent)
        .attr('x2', xCurrent)
        .attr('y1', 0)
        .attr('y2', ROW_HEIGHT)
        .attr('stroke', '#ef4444')  // Red
        .attr('stroke-width', 2)
        .attr('stroke-dasharray', '5,5')
        .attr('opacity', 0.6);

      // Small circle marker at center
      g.append('circle')
        .attr('cx', xCurrent)
        .attr('cy', ROW_HEIGHT / 2)
        .attr('r', 3)
        .attr('fill', '#ef4444');
    }

  }, [patient, timeScale, currentTime]);  // Add currentTime to trigger re-renders

  // Get timeline width from scale range
  const timelineWidth = timeScale ? timeScale.range()[1] : width;

  return (
    <>
      <div
        className={`timeline-row ${hasICU ? 'icu-patient' : ''}`}
        onClick={hasICU && onToggleExpand ? onToggleExpand : undefined}
        style={{
          height: ROW_HEIGHT,
          cursor: hasICU ? 'pointer' : 'default'
        }}
      >
        {/* Patient label on left */}
        <div className="timeline-patient-label">
          <div className="patient-id">
            {patient.patient.subject_id}
            {hasICU && <span className="icu-badge">ICU</span>}
          </div>
          <div className="patient-info">
            {patient.patient.age ? `${patient.patient.age}y` : '?'}
            {patient.patient.gender ? `, ${patient.patient.gender}` : ''}
          </div>
        </div>

        {/* SVG timeline */}
        <svg
          ref={svgRef}
          width={timelineWidth}
          height={ROW_HEIGHT}
          className="timeline-svg"
        ></svg>
      </div>

      {/* Expandable Chartevent Panel */}
      {isExpanded && hasICU && (
        <CharteventPanel
          chartevents={chartevents}
          width={timelineWidth}
        />
      )}

      {/* Tooltip */}
      {tooltip && (
        <div
          className="timeline-tooltip"
          style={{
            left: tooltip.x + 10,
            top: tooltip.y + 10
          }}
        >
          {tooltip.content.type === 'admission' && (
            <div>
              <div className="tooltip-title">Admission</div>
              <div className="tooltip-item">
                <strong>Type:</strong> {tooltip.content.data.admission.type}
              </div>
              <div className="tooltip-item">
                <strong>Location:</strong> {tooltip.content.data.admission.location}
              </div>
              <div className="tooltip-item">
                <strong>Admit:</strong> {tooltip.content.data.event_time}
              </div>
              {tooltip.content.data.discharge.time && (
                <div className="tooltip-item">
                  <strong>Discharge:</strong> {tooltip.content.data.discharge.time}
                </div>
              )}
            </div>
          )}

          {tooltip.content.type === 'lab-group' && (
            <div>
              <div className="tooltip-title">
                Lab Results ({tooltip.content.count})
              </div>
              {tooltip.content.count === 1 ? (
                // Single lab - show details
                <>
                  <div className="tooltip-item">
                    <strong>Test:</strong> {tooltip.content.data[0].lab.test_name}
                  </div>
                  <div className="tooltip-item">
                    <strong>Value:</strong> {tooltip.content.data[0].lab.value_numeric} {tooltip.content.data[0].lab.unit}
                  </div>
                  {tooltip.content.data[0].lab.flag && (
                    <div className="tooltip-item">
                      <strong>Flag:</strong> {tooltip.content.data[0].lab.flag}
                    </div>
                  )}
                  <div className="tooltip-item">
                    <strong>Time:</strong> {tooltip.content.data[0].event_time}
                  </div>
                </>
              ) : (
                // Multiple labs - show summary
                <>
                  <div className="tooltip-item">
                    {tooltip.content.count} tests at {tooltip.content.data[0].event_time}
                  </div>
                  <div className="tooltip-item" style={{ fontStyle: 'italic', fontSize: '11px' }}>
                    Click to view details
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      )}
    </>
  );
};

export default TimelineRow;
