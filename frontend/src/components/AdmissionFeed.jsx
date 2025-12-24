/**
 * AdmissionFeed Component
 *
 * Scrollable container for admission cards with auto-scroll
 */

import { useEffect, useRef } from 'react';
import { AdmissionCard } from './AdmissionCard';

export const AdmissionFeed = ({ admissions }) => {
  const feedRef = useRef(null);
  const prevLengthRef = useRef(0);

  useEffect(() => {
    // Auto-scroll to top when new admission arrives
    if (admissions.length > prevLengthRef.current && feedRef.current) {
      feedRef.current.scrollTop = 0;
    }
    prevLengthRef.current = admissions.length;
  }, [admissions.length]);

  if (admissions.length === 0) {
    return (
      <div className="admission-feed empty">
        <div className="empty-state">
          <div className="empty-icon">📭</div>
          <div className="empty-text">Waiting for admissions...</div>
          <div className="empty-hint">
            Admissions will appear here in real-time as they arrive
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="admission-feed" ref={feedRef}>
      {admissions.map((admission, index) => (
        <AdmissionCard
          key={`${admission.admission.hadm_id}-${index}`}
          admission={admission}
        />
      ))}
    </div>
  );
};
