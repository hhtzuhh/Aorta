/**
 * TimelineRow - Single patient row with D3 SVG rendering
 *
 * Renders admission segments as colored bars and lab events as dots
 */

import { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { useSimulationClock } from '../../contexts/ClockContext';

const TimelineRow = ({
  patient,
  timeScale,
  width,
  index,
  onRowClick,
  onLabClick,
  onSepsisAlertClick,
  icuStays = []
}) => {
  const svgRef = useRef(null);
  const [tooltip, setTooltip] = useState(null);
  const { currentTime } = useSimulationClock();

  const ROW_HEIGHT = 70;        // Increased for padding
  const BAR_HEIGHT = 28;        // Slightly smaller bar
  const VERTICAL_PADDING = (ROW_HEIGHT - BAR_HEIGHT) / 2;  // Center the bar vertically

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
    return admissionColors[type] || '#6b7280'; // Default gray
  };

  // ICU segment color (distinct from regular admissions)
  const ICU_COLOR = '#f97316'; // Orange for ICU periods

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
        .attr('y', VERTICAL_PADDING)
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

    });

    // Draw ICU stay segments (overlaid on admission bars)
    if (icuStays && icuStays.length > 0) {
      icuStays.forEach((icuStay) => {
        const icuInTime = new Date(icuStay.intime);
        const icuOutTime = icuStay.outtime ? new Date(icuStay.outtime) : new Date(icuInTime.getTime() + 48 * 60 * 60 * 1000); // Default 2 days if no outtime

        const currentSimTime = currentTime ? new Date(currentTime) : new Date();

        // Calculate positions
        const xIcuIn = timeScale(icuInTime);
        const xIcuPlanned = timeScale(icuOutTime);

        // Skip solid ICU bar if hasn't started yet
        if (currentSimTime < icuInTime) {
          return;
        }

        // Current ICU bar ends at MIN(currentSimTime, icuOutTime)
        const actualIcuEnd = currentSimTime < icuOutTime ? currentSimTime : icuOutTime;
        const xIcuEnd = timeScale(actualIcuEnd);

        // Determine opacity based on completion
        const isIcuCompleted = currentSimTime >= icuOutTime;
        const icuOpacity = isIcuCompleted ? 0.6 : 0.85;

        // SOLID ICU BAR: Current progress (not clickable - use panel buttons)
        const icuRect = g
          .append('rect')
          .attr('x', xIcuIn)
          .attr('y', VERTICAL_PADDING)
          .attr('width', Math.max(xIcuEnd - xIcuIn, 2))
          .attr('height', BAR_HEIGHT)
          .attr('rx', 4)
          .attr('fill', ICU_COLOR)
          .attr('opacity', icuOpacity)
          .attr('stroke', '#fff')
          .attr('stroke-width', 2)
          .style('cursor', 'default');  // Not clickable

        // ICU hover events
        icuRect.on('mouseenter', (event) => {
          icuRect.attr('opacity', 1);
          setTooltip({
            x: event.pageX,
            y: event.pageY,
            content: {
              type: 'icu',
              data: icuStay
            }
          });
        });

        icuRect.on('mousemove', (event) => {
          setTooltip(prev => ({
            ...prev,
            x: event.pageX,
            y: event.pageY
          }));
        });

        icuRect.on('mouseleave', () => {
          icuRect.attr('opacity', icuOpacity);
          setTooltip(null);
        });
      });
    }

    // Draw ALL lab markers independently (not tied to admission periods)
    const labsByTime = new Map();
    patient.labs.forEach((lab) => {
      const labTime = new Date(lab.event_time);
      const timeKey = labTime.getTime();
      if (!labsByTime.has(timeKey)) {
        labsByTime.set(timeKey, []);
      }
      labsByTime.get(timeKey).push(lab);
    });

    // Draw grouped lab markers
    labsByTime.forEach((labs, timeKey) => {
      const labTime = new Date(timeKey);
      const labX = timeScale(labTime);
      const labY = VERTICAL_PADDING + (BAR_HEIGHT / 2);  // Center in bar area

      const isMultiple = labs.length > 1;

      const circle = g
        .append('circle')
        .attr('cx', labX)
        .attr('cy', labY)
        .attr('r', isMultiple ? 7 : 6)
        .attr('fill', '#6366f1')  // Neutral indigo color
        .style('cursor', 'pointer');

      // Add "L" label
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

    // Draw sepsis alert markers
    if (patient.sepsisAlerts && patient.sepsisAlerts.length > 0) {
      console.log(`Drawing ${patient.sepsisAlerts.length} sepsis alerts for patient ${patient.patient.subject_id}`, patient.sepsisAlerts);
      patient.sepsisAlerts.forEach((alert) => {
        const alertTime = new Date(alert.event_time);
        const alertX = timeScale(alertTime);
        const alertY = VERTICAL_PADDING + 2;  // Just above the bar with padding

        console.log(`  Alert at x=${alertX}, y=${alertY}, time=${alert.event_time}`);

        // Determine color based on risk level
        const riskLevel = alert.prediction?.risk_level || 'LOW';
        const riskColors = {
          'CRITICAL': '#dc2626',  // Red
          'HIGH': '#f97316',      // Orange
          'MEDIUM': '#eab308',    // Yellow
          'LOW': '#84cc16'        // Lime
        };
        const alertColor = riskColors[riskLevel] || '#84cc16';

        const circle = g
          .append('circle')
          .attr('cx', alertX)
          .attr('cy', alertY)
          .attr('r', 6)  // Same size as labs
          .attr('fill', alertColor)
          .style('cursor', 'pointer');

        // Add "S" label
        g.append('text')
          .attr('x', alertX)
          .attr('y', alertY)
          .attr('text-anchor', 'middle')
          .attr('dominant-baseline', 'middle')
          .attr('font-size', '10px')  // Same as labs
          .attr('font-weight', 'bold')
          .attr('fill', 'white')
          .style('pointer-events', 'none')
          .text('S');

        // Hover events
        circle.on('mouseenter', (event) => {
          circle.attr('r', 8);  // Grow on hover (same as labs)
          setTooltip({
            x: event.pageX,
            y: event.pageY,
            content: {
              type: 'sepsis-alert',
              data: alert
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
          circle.attr('r', 6);  // Back to normal size
          setTooltip(null);
        });

        // Click event - show detail panel
        circle.on('click', (event) => {
          event.stopPropagation();
          if (onSepsisAlertClick) {
            onSepsisAlertClick(alert);
          }
        });
      });
    }

    // Draw current time indicator (vertical line)
    if (currentTime) {
      const currentSimTime = new Date(currentTime);
      const xCurrent = timeScale(currentSimTime);

      // Vertical dashed line
      g.append('line')
        .attr('x1', xCurrent)
        .attr('x2', xCurrent)
        .attr('y1', VERTICAL_PADDING)
        .attr('y2', VERTICAL_PADDING + BAR_HEIGHT)
        .attr('stroke', '#ef4444')  // Red
        .attr('stroke-width', 2)
        .attr('stroke-dasharray', '5,5')
        .attr('opacity', 0.6);

      // Small circle marker at center
      g.append('circle')
        .attr('cx', xCurrent)
        .attr('cy', VERTICAL_PADDING + (BAR_HEIGHT / 2))
        .attr('r', 3)
        .attr('fill', '#ef4444');
    }

  }, [patient, timeScale, currentTime, patient.sepsisAlerts?.length]);  // Re-render when sepsis alerts added

  // Get timeline width from scale range
  const timelineWidth = timeScale ? timeScale.range()[1] : width;

  return (
    <>
      <div
        className={`timeline-row ${hasICU ? 'icu-patient' : ''}`}
        onClick={onRowClick}
        style={{
          height: ROW_HEIGHT,
          cursor: 'pointer'
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

          {tooltip.content.type === 'icu' && (
            <div>
              <div className="tooltip-title" style={{ color: '#f97316' }}>ICU Stay</div>
              <div className="tooltip-item">
                <strong>Unit:</strong> {tooltip.content.data.first_careunit || 'Unknown'}
              </div>
              <div className="tooltip-item">
                <strong>In:</strong> {tooltip.content.data.intime}
              </div>
              {tooltip.content.data.outtime && (
                <div className="tooltip-item">
                  <strong>Out:</strong> {tooltip.content.data.outtime}
                </div>
              )}
              {tooltip.content.data.los_days && (
                <div className="tooltip-item">
                  <strong>LOS:</strong> {tooltip.content.data.los_days.toFixed(1)} days
                </div>
              )}
            </div>
          )}

          {tooltip.content.type === 'sepsis-alert' && (
            <div>
              <div className="tooltip-title" style={{
                color: tooltip.content.data.prediction?.risk_level === 'CRITICAL' ? '#dc2626' :
                       tooltip.content.data.prediction?.risk_level === 'HIGH' ? '#f97316' :
                       tooltip.content.data.prediction?.risk_level === 'MEDIUM' ? '#eab308' : '#84cc16'
              }}>
                Sepsis Alert - {tooltip.content.data.prediction?.risk_level || 'UNKNOWN'}
              </div>
              <div className="tooltip-item">
                <strong>Risk:</strong> {((tooltip.content.data.prediction?.sepsis_probability || 0) * 100).toFixed(1)}%
              </div>
              <div className="tooltip-item">
                <strong>SOFA Score:</strong> {tooltip.content.data.prediction?.sofa_score || 'N/A'}
              </div>
              <div className="tooltip-item">
                <strong>Time:</strong> {tooltip.content.data.event_time}
              </div>
              <div className="tooltip-item" style={{ fontStyle: 'italic', fontSize: '11px', marginTop: '4px' }}>
                Click to view details
              </div>
            </div>
          )}
        </div>
      )}
    </>
  );
};

export default TimelineRow;
