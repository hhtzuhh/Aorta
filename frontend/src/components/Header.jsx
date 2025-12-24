/**
 * Header Component
 *
 * Dashboard header with title and branding
 */

export const Header = () => {
  return (
    <header className="header">
      <div className="header-content">
        <h1 className="title">
          <span className="icon">🏥</span>
          Aorta
        </h1>
        <p className="subtitle">Real-Time Hospital Admission Monitor</p>
      </div>
    </header>
  );
};
