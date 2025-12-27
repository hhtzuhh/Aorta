/**
 * CharteventRow - Displays a single chartevent (vital sign) with line chart or text value
 *
 * For numeric chartevents: Renders D3 line chart showing trend over time
 * For text chartevents: Displays latest value as text
 */

import { useRef, useEffect } from 'react';
import * as d3 from 'd3';
import './CharteventRow.css';

const CharteventRow = ({ itemid, label, category, param_type, unit, values }) => {
  const chartRef = useRef();

  useEffect(() => {
    if (param_type === 'Numeric' && values.length > 0) {
      renderLineChart();
    }
  }, [values, param_type]);

  const renderLineChart = () => {
    const svg = d3.select(chartRef.current);
    svg.selectAll('*').remove();

    const margin = { top: 5, right: 5, bottom: 5, left: 5 };
    const width = 300 - margin.left - margin.right;
    const height = 50 - margin.top - margin.bottom;

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    // Filter out null values
    const validValues = values.filter(d => d.value_numeric !== null && d.value_numeric !== undefined);

    if (validValues.length === 0) {
      return;
    }

    // Create scales
    const xScale = d3.scaleTime()
      .domain(d3.extent(validValues, d => new Date(d.time)))
      .range([0, width]);

    const yScale = d3.scaleLinear()
      .domain([
        d3.min(validValues, d => d.value_numeric) * 0.95,
        d3.max(validValues, d => d.value_numeric) * 1.05
      ])
      .range([height, 0]);

    // Draw line
    const line = d3.line()
      .x(d => xScale(new Date(d.time)))
      .y(d => yScale(d.value_numeric))
      .curve(d3.curveMonotoneX);

    g.append('path')
      .datum(validValues)
      .attr('d', line)
      .attr('fill', 'none')
      .attr('stroke', '#3b82f6')
      .attr('stroke-width', 2);

    // Add circles for data points
    g.selectAll('circle')
      .data(validValues)
      .enter()
      .append('circle')
      .attr('cx', d => xScale(new Date(d.time)))
      .attr('cy', d => yScale(d.value_numeric))
      .attr('r', 3)
      .attr('fill', '#3b82f6');
  };

  // Get latest value for display
  const latestValue = values[values.length - 1];

  return (
    <div className="chartevent-row">
      <div className="chartevent-label">
        <div className="chartevent-name">{label}</div>
        <div className="chartevent-category">{category}</div>
      </div>

      <div className="chartevent-chart">
        {param_type === 'Numeric' ? (
          <svg ref={chartRef} width="300" height="50" />
        ) : (
          <span className="chartevent-text-value">
            {latestValue?.value_text || '--'}
          </span>
        )}
      </div>

      <div className="chartevent-value">
        {param_type === 'Numeric' && latestValue && (
          <>
            <span className="value">{latestValue.value_numeric?.toFixed(1)}</span>
            <span className="unit">{unit}</span>
          </>
        )}
      </div>
    </div>
  );
};

export default CharteventRow;
