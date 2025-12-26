/**
 * TimelineAxis - Sticky time axis at the top of timeline
 *
 * Renders D3 axis with formatted time labels
 */

import { useEffect, useRef } from 'react';
import * as d3 from 'd3';

const TimelineAxis = ({ timeScale, width }) => {
  const axisRef = useRef(null);

  useEffect(() => {
    if (!axisRef.current || !timeScale) return;

    // Clear previous axis
    d3.select(axisRef.current).selectAll('*').remove();

    // Create axis generator
    const axis = d3
      .axisBottom(timeScale)  // Bottom axis now
      .ticks(5)  // Fewer ticks to avoid overlap
      .tickFormat(d3.timeFormat('%m/%d %H:%M'));  // Shorter format: "06/06 15:00"

    // Render axis - shift to align with timeline (patient label width = 80px)
    const svg = d3
      .select(axisRef.current)
      .append('g')
      .attr('class', 'axis-group')
      .attr('transform', 'translate(0, 0)')  // No vertical offset needed
      .call(axis);

    // Style the axis
    svg.selectAll('text')
      .style('font-size', '11px')
      .style('fill', '#4b5563');

    svg.selectAll('line')
      .style('stroke', '#d1d5db');

    svg.selectAll('path')
      .style('stroke', '#d1d5db');

  }, [timeScale]);

  if (!timeScale) return null;

  // Get timeline width from scale range
  const timelineWidth = timeScale.range()[1];

  return (
    <div className="timeline-axis">
      <svg width={timelineWidth} height={40} ref={axisRef}></svg>
    </div>
  );
};

export default TimelineAxis;
