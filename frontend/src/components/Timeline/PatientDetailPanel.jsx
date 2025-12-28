/**
 * PatientDetailPanel - Comprehensive patient detail view (Bottom Panel)
 *
 * Shows when clicking a patient row. Contains:
 * - Patient demographics (left column)
 * - ICU stays with vital signs (middle column)
 * - Lab results by batch (right column)
 */

import { useState, useRef, useEffect } from 'react';
import * as d3 from 'd3';
import './PatientDetailPanel.css';

const PatientDetailPanel = ({ patient, icuStays = [], onClose, chartevents = [] }) => {
  const [selectedIcuIndex, setSelectedIcuIndex] = useState(null);
  const [currentBatchIndex, setCurrentBatchIndex] = useState(0);
  const [labsCollapsed, setLabsCollapsed] = useState(false);

  // Group labs by timestamp (batch)
  const labBatches = [];
  const labsByTime = new Map();

  patient.labs.forEach((lab) => {
    const timeKey = lab.event_time;
    if (!labsByTime.has(timeKey)) {
      labsByTime.set(timeKey, []);
    }
    labsByTime.get(timeKey).push(lab);
  });

  // Convert to sorted array of batches
  Array.from(labsByTime.entries())
    .sort((a, b) => new Date(a[0]) - new Date(b[0]))
    .forEach(([time, labs]) => {
      labBatches.push({ time, labs });
    });

  const currentBatch = labBatches[currentBatchIndex];

  const handlePrevBatch = () => {
    if (currentBatchIndex > 0) {
      setCurrentBatchIndex(currentBatchIndex - 1);
    }
  };

  const handleNextBatch = () => {
    if (currentBatchIndex < labBatches.length - 1) {
      setCurrentBatchIndex(currentBatchIndex + 1);
    }
  };

  const formatFullDateTime = (dateStr) => {
    const date = new Date(dateStr);
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${month}/${day} ${hours}:${minutes}:${seconds}`;
  };

  // Get vitals for selected ICU stay
  const getVitalsForIcu = (icuStay) => {
    if (!chartevents || Object.keys(chartevents).length === 0) return [];

    const intime = new Date(icuStay.intime);
    const outtime = icuStay.outtime ? new Date(icuStay.outtime) : new Date();

    // Convert chartevents object to array of events
    const vitalEvents = [];
    Object.entries(chartevents).forEach(([itemid, vitalData]) => {
      vitalData.values.forEach(valuePoint => {
        const eventTime = new Date(valuePoint.time);
        // Filter by time range (intime to outtime)
        if (eventTime >= intime && eventTime <= outtime) {
          vitalEvents.push({
            event_time: valuePoint.time,
            chartevent: {
              itemid: parseInt(itemid),
              label: vitalData.label,
              category: vitalData.category,
              param_type: vitalData.param_type,
              value_numeric: valuePoint.value_numeric,
              value_text: valuePoint.value_text,
              unit: vitalData.unit
            }
          });
        }
      });
    });

    // Sort by time
    return vitalEvents.sort((a, b) => new Date(a.event_time) - new Date(b.event_time));
  };

  const selectedIcu = selectedIcuIndex !== null ? icuStays[selectedIcuIndex] : null;
  const selectedIcuVitals = selectedIcu ? getVitalsForIcu(selectedIcu) : [];

  return (
    <div className="patient-detail-panel">
      {/* Close button */}
      <button className="panel-close-btn" onClick={onClose}>×</button>

      {/* Row 1: Demographics */}
      <div className="panel-section">
        <h3>Patient {patient.patient.subject_id} • {patient.patient.age || '?'}y {patient.patient.gender || '?'} • {patient.admissions.length > 0 ? patient.admissions[0].admission.type : 'No admission'} • {patient.labs.length} Labs</h3>

        {patient.admissions.length > 0 && (
          <div style={{ display: 'flex', gap: '30px', fontSize: '13px', color: '#6b7280' }}>
            <div>
              <strong>Location:</strong> {patient.admissions[0].admission.location}
            </div>
            <div>
              <strong>Admitted:</strong> {formatFullDateTime(patient.admissions[0].event_time)}
            </div>
            {patient.admissions[0].discharge?.time && (
              <div>
                <strong>Discharged:</strong> {formatFullDateTime(patient.admissions[0].discharge.time)}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Row 2: Sepsis Alerts */}
      <div className="panel-section">
        <h3>🚨 Sepsis Risk Over Time ({patient.sepsisAlerts?.length || 0} alerts)</h3>

        {!patient.sepsisAlerts || patient.sepsisAlerts.length === 0 ? (
          <div className="empty-state-small">No sepsis alerts</div>
        ) : (
          <SepsisRiskChart
            alerts={patient.sepsisAlerts}
            formatFullDateTime={formatFullDateTime}
          />
        )}
      </div>

      {/* Row 3: ICU Stays */}
      <div className="panel-section">
        <h3>ICU Stays ({icuStays.length})</h3>

        {icuStays.length === 0 ? (
          <div className="empty-state-small">No ICU stays recorded</div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {icuStays.map((stay, index) => (
              <div key={stay.stay_id || index}>
                <button
                  className="icu-stay-button"
                  onClick={() => setSelectedIcuIndex(index === selectedIcuIndex ? null : index)}
                >
                  <div className="icu-stay-header">
                    <span className="icu-stay-number">ICU #{index + 1}</span>
                    <span className="icu-stay-unit">
                      {stay.careunit || stay.first_careunit || stay.last_careunit || 'ICU'}
                    </span>
                  </div>
                  <div className="icu-stay-details">
                    <div>In: {formatFullDateTime(stay.intime)}</div>
                    {stay.outtime && (
                      <div>Out: {formatFullDateTime(stay.outtime)}</div>
                    )}
                    {stay.los_days && (
                      <div className="icu-stay-los">{stay.los_days.toFixed(1)} days</div>
                    )}
                  </div>
                </button>

                {/* Vital Signs for this ICU stay */}
                {selectedIcuIndex === index && (
                  <VitalSignsChart
                    vitals={selectedIcuVitals}
                    stay={stay}
                    formatFullDateTime={formatFullDateTime}
                  />
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Row 3: Lab Results */}
      <div className="panel-section">
        <div className="section-header-with-nav">
          <h3 onClick={() => setLabsCollapsed(!labsCollapsed)} style={{ cursor: 'pointer' }}>
            {labsCollapsed ? '▶' : '▼'} Lab Results
          </h3>
          {!labsCollapsed && (
            <div className="lab-navigation">
            <button
              className="nav-btn"
              onClick={handlePrevBatch}
              disabled={currentBatchIndex === 0}
            >
              ◀
            </button>
            <span className="lab-counter">
              {currentBatchIndex + 1} / {labBatches.length}
            </span>
            <button
              className="nav-btn"
              onClick={handleNextBatch}
              disabled={currentBatchIndex === labBatches.length - 1}
            >
              ▶
            </button>
          </div>
          )}
        </div>

        {!labsCollapsed && (
          <>
            {labBatches.length === 0 ? (
              <div className="empty-state-small">No lab results available</div>
            ) : currentBatch ? (
              <div className="lab-batch-card">
                <div className="lab-batch-time">
                  {formatFullDateTime(currentBatch.time)} ({currentBatch.labs.length} tests)
                </div>
                <div className="lab-batch-items">
                  {currentBatch.labs.map((lab, idx) => {
                    const isAbnormal = lab.lab.flag && lab.lab.flag.toUpperCase() === 'ABNORMAL';
                    return (
                      <div key={idx} className={`lab-item ${isAbnormal ? 'abnormal' : ''}`}>
                        <div className="lab-detail-card">
                          <span className={`lab-test-name ${isAbnormal ? 'abnormal' : ''}`}>
                            {lab.lab.test_name}
                          </span>
                          {lab.lab.ref_range && (
                            <div className="lab-ref-range">
                              Ref: {lab.lab.ref_range}
                            </div>
                          )}
                        </div>
                        <div style={{ display: 'flex', alignItems: 'baseline', gap: '6px' }}>
                          <span className={`value-number ${isAbnormal ? 'abnormal' : ''}`}>
                            {lab.lab.value_numeric}
                          </span>
                          <span className="value-unit">{lab.lab.unit}</span>
                          {isAbnormal && (
                            <span className="lab-flag abnormal">ABN</span>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ) : null}

            {/* Quick navigation to other batches */}
            {labBatches.length > 1 && (
              <div className="lab-list-preview">
                <div className="preview-label">All Batches:</div>
                <div className="lab-preview-scroll">
                  {labBatches.map((batch, index) => (
                    <button
                      key={index}
                      className={`lab-preview-item ${index === currentBatchIndex ? 'active' : ''}`}
                      onClick={() => setCurrentBatchIndex(index)}
                    >
                      <span className="preview-test-name">{formatFullDateTime(batch.time)}</span>
                      <span className="preview-value">{batch.labs.length} tests</span>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};

// Time Series Chart Component for Vital Signs - Separate row per vital type
const VitalSignsChart = ({ vitals, stay, formatFullDateTime }) => {
  // Group vitals by parameter type
  const vitalsByType = {};
  vitals.forEach(v => {
    const label = v.chartevent.label;
    if (!vitalsByType[label]) {
      vitalsByType[label] = [];
    }
    vitalsByType[label].push({
      time: new Date(v.event_time),
      timeStr: v.event_time,
      value_numeric: v.chartevent.value_numeric,
      value_text: v.chartevent.value_text,
      unit: v.chartevent.unit
    });
  });

  const vitalTypes = Object.keys(vitalsByType);

  return (
    <div className="vitals-chart">
      <div className="icu-timeline">
        <div className="icu-timeline-time">
          {formatFullDateTime(stay.intime)} → {stay.outtime ? formatFullDateTime(stay.outtime) : 'Ongoing'}
        </div>
        <div className="icu-timeline-duration">
          Duration: {stay.los_days ? stay.los_days.toFixed(2) : '?'} days
        </div>
      </div>

      <h4>Vital Signs Time Series ({vitals.length} readings)</h4>
      {vitals.length === 0 ? (
        <div className="empty-state-small">No vitals recorded</div>
      ) : (
        <div className="vitals-rows">
          {vitalTypes.map((vitalType, idx) => (
            <VitalRow
              key={vitalType}
              vitalType={vitalType}
              data={vitalsByType[vitalType]}
              stay={stay}
              color={d3.schemeCategory10[idx % 10]}
              formatFullDateTime={formatFullDateTime}
            />
          ))}
        </div>
      )}
    </div>
  );
};

// Individual vital sign row with mini chart or text badges
const VitalRow = ({ vitalType, data, stay, color, formatFullDateTime }) => {
  const chartRef = useRef(null);
  const [selectedBadge, setSelectedBadge] = useState(null);

  // Check if data is numeric or text
  const hasNumericData = data.some(d => d.value_numeric !== null && d.value_numeric !== undefined);

  useEffect(() => {
    if (!hasNumericData || !chartRef.current || data.length === 0) return;

    // Clear previous chart
    d3.select(chartRef.current).selectAll('*').remove();

    // Filter numeric data
    const numericData = data.filter(d => d.value_numeric !== null && d.value_numeric !== undefined);
    if (numericData.length === 0) return;

    // Chart dimensions
    const margin = { top: 25, right: 10, bottom: 25, left: 60 };
    const width = chartRef.current.clientWidth - margin.left - margin.right;
    const height = 90 - margin.top - margin.bottom;

    const svg = d3.select(chartRef.current)
      .append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Time scale (x-axis)
    const xScale = d3.scaleTime()
      .domain([
        new Date(stay.intime),
        stay.outtime ? new Date(stay.outtime) : new Date()
      ])
      .range([0, width]);

    // Y scale for this vital type
    const values = numericData.map(d => d.value_numeric);
    const yMin = Math.min(...values);
    const yMax = Math.max(...values);
    const yPadding = (yMax - yMin) * 0.15 || 1;

    const yScale = d3.scaleLinear()
      .domain([yMin - yPadding, yMax + yPadding])
      .range([height, 0]);

    // Draw X and Y axis lines
    // Y-axis line
    svg.append('line')
      .attr('x1', 0)
      .attr('y1', 0)
      .attr('x2', 0)
      .attr('y2', height)
      .attr('stroke', '#1f2937')
      .attr('stroke-width', 2);

    // X-axis line
    svg.append('line')
      .attr('x1', 0)
      .attr('y1', height)
      .attr('x2', width)
      .attr('y2', height)
      .attr('stroke', '#1f2937')
      .attr('stroke-width', 2);

    // Grid lines
    svg.append('g')
      .attr('class', 'grid')
      .attr('opacity', 0.1)
      .call(d3.axisLeft(yScale).ticks(3).tickSize(-width).tickFormat(''));

    // X-axis with smart time formatting (show date only when it changes)
    const formatDate = d3.timeFormat('%m/%d');
    const formatTime = d3.timeFormat('%H:%M');

    const xAxis = d3.axisBottom(xScale).ticks(6);
    const tickValues = xScale.ticks(6);

    xAxis.tickFormat((d, i) => {
      if (i === 0) {
        // First tick: show date + time
        return `${formatDate(d)}\n${formatTime(d)}`;
      } else {
        // Check if date changed from previous tick
        const prevDate = formatDate(tickValues[i - 1]);
        const currDate = formatDate(d);

        if (prevDate !== currDate) {
          // Date changed: show new date + time
          return `${formatDate(d)}\n${formatTime(d)}`;
        } else {
          // Same date: show only time
          return formatTime(d);
        }
      }
    });

    svg.append('g')
      .attr('transform', `translate(0,${height})`)
      .call(xAxis)
      .select('.domain').remove(); // Remove default axis line (we drew our own)

    svg.selectAll('.tick line').remove(); // Remove tick lines

    svg.selectAll('.tick text')
      .style('font-size', '10px')
      .style('font-weight', '500')
      .style('fill', '#1f2937')
      .style('text-anchor', 'middle');

    // Y-axis with unit label
    const unit = numericData[0]?.unit || '';
    svg.append('g')
      .call(d3.axisLeft(yScale).ticks(4))
      .select('.domain').remove(); // Remove default axis line (we drew our own)

    svg.selectAll('.tick line').remove(); // Remove tick lines

    svg.selectAll('.tick text')
      .style('font-size', '10px')
      .style('fill', '#1f2937');

    // Y-axis label (unit) - horizontal above the axis
    svg.append('text')
      .attr('x', -10)
      .attr('y', -5)
      .style('text-anchor', 'start')
      .style('font-size', '11px')
      .style('font-weight', '600')
      .style('fill', '#1f2937')
      .text(unit);

    // Line generator
    const line = d3.line()
      .x(d => xScale(d.time))
      .y(d => yScale(d.value_numeric))
      .curve(d3.curveMonotoneX);

    // Draw line
    svg.append('path')
      .datum(numericData)
      .attr('fill', 'none')
      .attr('stroke', color)
      .attr('stroke-width', 2.5)
      .attr('d', line);

    // Tooltip div
    const tooltip = d3.select('body')
      .selectAll('.vital-tooltip')
      .data([null])
      .join('div')
      .attr('class', 'vital-tooltip')
      .style('position', 'absolute')
      .style('visibility', 'hidden')
      .style('background', 'rgba(0, 0, 0, 0.8)')
      .style('color', 'white')
      .style('padding', '8px 12px')
      .style('border-radius', '6px')
      .style('font-size', '12px')
      .style('pointer-events', 'none')
      .style('z-index', 10000);

    // Draw points with better tooltips
    svg.selectAll('.point')
      .data(numericData)
      .enter()
      .append('circle')
      .attr('class', 'point')
      .attr('cx', d => xScale(d.time))
      .attr('cy', d => yScale(d.value_numeric))
      .attr('r', 4)
      .attr('fill', color)
      .attr('stroke', 'white')
      .attr('stroke-width', 2)
      .style('cursor', 'pointer')
      .on('mouseover', function(event, d) {
        d3.select(this).attr('r', 6);
        tooltip
          .style('visibility', 'visible')
          .html(`
            <div><strong>${vitalType}</strong></div>
            <div><strong style="font-size: 16px;">${d.value_numeric} ${unit}</strong></div>
            <div style="font-size: 10px; margin-top: 4px;">${d.time.toLocaleString()}</div>
          `);
      })
      .on('mousemove', function(event) {
        tooltip
          .style('top', (event.pageY - 60) + 'px')
          .style('left', (event.pageX + 10) + 'px');
      })
      .on('mouseout', function() {
        d3.select(this).attr('r', 4);
        tooltip.style('visibility', 'hidden');
      });

  }, [data, stay, color, hasNumericData, vitalType]);

  const unit = data[0]?.unit || '';
  const latestNumeric = data.filter(d => d.value_numeric !== null).slice(-1)[0]?.value_numeric;
  const latestText = data.filter(d => d.value_text !== null).slice(-1)[0]?.value_text;

  return (
    <div className="vital-row-chart">
      <div className="vital-row-header">
        <span className="vital-row-label" style={{ color }}>{vitalType}</span>
        <span className="vital-row-latest">
          {latestNumeric !== undefined ? `${latestNumeric} ${unit}` : latestText || 'N/A'}
        </span>
      </div>

      {hasNumericData ? (
        <div ref={chartRef} className="vital-row-graph"></div>
      ) : (
        <div className="vital-text-items">
          {data.map((item, idx) => (
            <div
              key={idx}
              className="vital-text-badge"
              onClick={() => setSelectedBadge(selectedBadge === idx ? null : idx)}
              title={`${item.value_text || 'N/A'} at ${item.time.toLocaleString()}`}
            >
              <span className="vital-text-badge-value">{item.value_text || 'N/A'}</span>
              <span className="vital-text-badge-time">{formatFullDateTime(item.timeStr)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

// Sepsis Risk Chart - shows probability over time
const SepsisRiskChart = ({ alerts, formatFullDateTime }) => {
  const chartRef = useRef(null);
  const [selectedPoint, setSelectedPoint] = useState(null);

  useEffect(() => {
    if (!chartRef.current || alerts.length === 0) return;

    // Clear previous chart
    d3.select(chartRef.current).selectAll('*').remove();

    // Prepare data
    const data = alerts
      .map(alert => ({
        time: new Date(alert.event_time),
        timeStr: alert.event_time,
        probability: (alert.prediction?.sepsis_probability || 0) * 100,
        riskLevel: alert.prediction?.risk_level || 'UNKNOWN',
        sofa: alert.prediction?.sofa_score || 0
      }))
      .sort((a, b) => a.time - b.time);

    // Chart dimensions
    const margin = { top: 20, right: 20, bottom: 40, left: 50 };
    const width = chartRef.current.clientWidth - margin.left - margin.right;
    const height = 150 - margin.top - margin.bottom;

    const svg = d3.select(chartRef.current)
      .append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Scales
    const xScale = d3.scaleTime()
      .domain(d3.extent(data, d => d.time))
      .range([0, width]);

    const yScale = d3.scaleLinear()
      .domain([0, 100])
      .range([height, 0]);

    // Grid lines
    svg.append('g')
      .attr('class', 'grid')
      .attr('opacity', 0.1)
      .call(d3.axisLeft(yScale).tickSize(-width).tickFormat(''));

    // Line
    const line = d3.line()
      .x(d => xScale(d.time))
      .y(d => yScale(d.probability));

    svg.append('path')
      .datum(data)
      .attr('fill', 'none')
      .attr('stroke', '#000')
      .attr('stroke-width', 1.5)
      .attr('d', line);

    // Axes
    svg.append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(xScale).ticks(5).tickFormat(d3.timeFormat('%m/%d %H:%M')))
      .style('font-size', '10px')
      .style('color', '#000');

    svg.append('g')
      .call(d3.axisLeft(yScale).ticks(5))
      .style('font-size', '10px')
      .style('color', '#000');

    // Y-axis label
    svg.append('text')
      .attr('transform', 'rotate(-90)')
      .attr('y', 0 - margin.left)
      .attr('x', 0 - (height / 2))
      .attr('dy', '1em')
      .style('text-anchor', 'middle')
      .style('font-size', '11px')
      .style('fill', '#000')
      .text('Sepsis Risk (%)');

    // Tooltip
    const tooltip = d3.select('body')
      .append('div')
      .style('position', 'absolute')
      .style('visibility', 'hidden')
      .style('background', 'rgba(0, 0, 0, 0.8)')
      .style('color', 'white')
      .style('padding', '8px 12px')
      .style('border-radius', '4px')
      .style('font-size', '12px')
      .style('pointer-events', 'none')
      .style('z-index', '1000');

    // Data points
    const riskColors = {
      'CRITICAL': '#dc2626',
      'HIGH': '#f97316',
      'MEDIUM': '#eab308',
      'LOW': '#84cc16'
    };

    svg.selectAll('.dot')
      .data(data)
      .enter()
      .append('circle')
      .attr('class', 'dot')
      .attr('cx', d => xScale(d.time))
      .attr('cy', d => yScale(d.probability))
      .attr('r', 5)
      .attr('fill', d => riskColors[d.riskLevel] || '#6b7280')
      .attr('stroke', 'white')
      .attr('stroke-width', 2)
      .style('cursor', 'pointer')
      .on('mouseover', function(event, d) {
        d3.select(this).attr('r', 7);
        tooltip
          .style('visibility', 'visible')
          .html(`
            <div><strong>${d.riskLevel} RISK</strong></div>
            <div style="font-size: 16px; margin: 4px 0;"><strong>${d.probability.toFixed(1)}%</strong></div>
            <div>SOFA: ${d.sofa}</div>
            <div style="font-size: 10px; margin-top: 4px; opacity: 0.8;">${formatFullDateTime(d.timeStr)}</div>
          `);
      })
      .on('mousemove', function(event) {
        tooltip
          .style('top', (event.pageY - 70) + 'px')
          .style('left', (event.pageX + 10) + 'px');
      })
      .on('mouseout', function() {
        d3.select(this).attr('r', 5);
        tooltip.style('visibility', 'hidden');
      })
      .on('click', function(event, d) {
        setSelectedPoint(d);
      });

    return () => {
      tooltip.remove();
    };
  }, [alerts, formatFullDateTime]);

  return (
    <div style={{ marginTop: '10px' }}>
      <div ref={chartRef} style={{ width: '100%', minHeight: '150px' }}></div>
      {selectedPoint && (
        <div style={{
          marginTop: '10px',
          padding: '12px',
          background: '#f9fafb',
          borderRadius: '4px',
          borderLeft: `4px solid ${
            selectedPoint.riskLevel === 'CRITICAL' ? '#dc2626' :
            selectedPoint.riskLevel === 'HIGH' ? '#f97316' :
            selectedPoint.riskLevel === 'MEDIUM' ? '#eab308' : '#84cc16'
          }`
        }}>
          <div style={{ fontWeight: '600', marginBottom: '4px', color: '#000' }}>
            {selectedPoint.riskLevel} RISK at {formatFullDateTime(selectedPoint.timeStr)}
          </div>
          <div style={{ display: 'flex', gap: '20px', fontSize: '13px', color: '#000' }}>
            <div><strong>Probability:</strong> {selectedPoint.probability.toFixed(1)}%</div>
            <div><strong>SOFA Score:</strong> {selectedPoint.sofa}</div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PatientDetailPanel;
