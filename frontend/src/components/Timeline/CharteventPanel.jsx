/**
 * CharteventPanel - Expandable panel showing all chartevents (vital signs) for a patient
 *
 * Displays when user clicks on an ICU patient row
 * Shows all chartevent itemids with their line charts or text values
 */

import CharteventRow from './CharteventRow';
import './CharteventPanel.css';

const CharteventPanel = ({ chartevents, width }) => {
  if (!chartevents || Object.keys(chartevents).length === 0) {
    return (
      <div className="chartevent-panel" style={{ width }}>
        <div className="no-chartevents">
          <p>No vital signs data available for this patient yet.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="chartevent-panel" style={{ width }}>
      <div className="chartevent-panel-header">
        <h3>Vital Signs & Chartevents</h3>
        <span className="chartevent-count">
          {Object.keys(chartevents).length} metrics
        </span>
      </div>

      <div className="chartevent-list">
        {Object.entries(chartevents).map(([itemid, data]) => (
          <CharteventRow
            key={itemid}
            itemid={itemid}
            label={data.label}
            category={data.category}
            param_type={data.param_type}
            unit={data.unit}
            values={data.values}
          />
        ))}
      </div>
    </div>
  );
};

export default CharteventPanel;
