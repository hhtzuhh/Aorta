/**
 * ConnectionStatus Component
 *
 * Visual indicator of SSE connection status
 */

export const ConnectionStatus = ({ status }) => {
  const statusConfig = {
    connected: {
      color: 'green',
      text: 'Live',
      icon: '●'
    },
    connecting: {
      color: 'yellow',
      text: 'Connecting...',
      icon: '○'
    },
    disconnected: {
      color: 'red',
      text: 'Disconnected',
      icon: '○'
    }
  };

  const config = statusConfig[status] || statusConfig.disconnected;

  return (
    <div className={`connection-status status-${config.color}`}>
      <span className="status-icon">{config.icon}</span>
      <span className="status-text">{config.text}</span>
    </div>
  );
};
