/**
 * AdmissionCard Component
 *
 * Displays individual hospital admission with priority styling
 */

const formatTimestamp = (timestamp) => {
  try {
    const date = new Date(timestamp);
    return date.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  } catch {
    return timestamp;
  }
};

export const AdmissionCard = ({ admission }) => {
  const isHighPriority = admission.is_high_priority;

  return (
    <div className={`admission-card ${isHighPriority ? 'high-priority' : ''}`}>
      <div className="priority-indicator">
        {isHighPriority ? '🚨' : '📋'}
      </div>

      <div className="admission-details">
        <div className="admission-header">
          <span className="admission-type">{admission.admission.type}</span>
          {isHighPriority && <span className="priority-badge">URGENT</span>}
        </div>

        <div className="patient-info">
          <span className="patient-id">Patient {admission.patient.subject_id}</span>
          <span className="separator">•</span>
          <span className="demographics">
            {admission.patient.age}y {admission.patient.gender}
          </span>
        </div>

        <div className="admission-location">
          <span className="location-icon">📍</span>
          {admission.admission.location}
        </div>

        <div className="admission-meta">
          <span className="timestamp">{formatTimestamp(admission.timestamp)}</span>
          <span className="separator">•</span>
          <span className="insurance">{admission.admission.insurance}</span>
        </div>
      </div>
    </div>
  );
};
