/**
 * LabDetailPanel - Shows detailed lab results when clicked
 */

import './LabDetailPanel.css';

const LabDetailPanel = ({ labs, onClose }) => {
  if (!labs || labs.length === 0) return null;

  return (
    <div className="lab-detail-panel">
      <div className="lab-detail-header">
        <h3>Lab Results - {labs[0].event_time}</h3>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>

      <div className="lab-detail-content">
        <table className="lab-table">
          <thead>
            <tr>
              <th>Test</th>
              <th>Value</th>
              <th>Unit</th>
              <th>Reference Range</th>
              <th>Flag</th>
            </tr>
          </thead>
          <tbody>
            {labs.map((lab, idx) => (
              <tr key={idx} className={lab.lab.flag === 'abnormal' ? 'abnormal-row' : ''}>
                <td className="test-name">{lab.lab.test_name}</td>
                <td className="test-value">{lab.lab.value_numeric || 'N/A'}</td>
                <td className="test-unit">{lab.lab.unit || '-'}</td>
                <td className="test-range">
                  {lab.lab.ref_range_lower && lab.lab.ref_range_upper
                    ? `${lab.lab.ref_range_lower} - ${lab.lab.ref_range_upper}`
                    : '-'}
                </td>
                <td className="test-flag">
                  {lab.lab.flag ? (
                    <span className="flag-badge">{lab.lab.flag}</span>
                  ) : (
                    <span className="flag-normal">Normal</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default LabDetailPanel;
