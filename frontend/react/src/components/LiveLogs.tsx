import React, { useState } from 'react';

interface LogEntry {
  timestamp: string;
  level: 'INFO' | 'WARNING' | 'ERROR';
  message: string;
  service: string;
}

const LiveLogs: React.FC = () => {
  const [logLevel, setLogLevel] = useState<string>('ALL');

  const logEntries: LogEntry[] = [
    {
      timestamp: '2025-10-25 20:12:00',
      level: 'INFO',
      message: 'Trip event producer initialized',
      service: 'trip-event-producer'
    },
    {
      timestamp: '2025-10-25 20:12:01',
      level: 'INFO',
      message: 'Processing 100 trips',
      service: 'trip-event-producer'
    },
    {
      timestamp: '2025-10-25 20:12:02',
      level: 'INFO',
      message: 'Generated 500 events',
      service: 'trip-event-producer'
    },
    {
      timestamp: '2025-10-25 20:12:03',
      level: 'INFO',
      message: 'Events sent to Kafka successfully',
      service: 'trip-event-producer'
    },
    {
      timestamp: '2025-10-25 20:12:04',
      level: 'WARNING',
      message: 'High memory usage detected',
      service: 'system'
    },
    {
      timestamp: '2025-10-25 20:12:05',
      level: 'ERROR',
      message: 'Failed to connect to Redis',
      service: 'trip-assembler'
    },
  ];

  const filteredLogs = logLevel === 'ALL'
    ? logEntries
    : logEntries.filter(log => log.level === logLevel);

  const getLevelColor = (level: string) => {
    switch (level) {
      case 'ERROR':
        return 'text-red-400 border-red-500';
      case 'WARNING':
        return 'text-yellow-400 border-yellow-500';
      case 'INFO':
        return 'text-blue-400 border-blue-500';
      default:
        return 'text-gray-400 border-gray-500';
    }
  };

  const getLevelIcon = (level: string) => {
    switch (level) {
      case 'ERROR':
        return '❌';
      case 'WARNING':
        return '⚠️';
      case 'INFO':
        return 'ℹ️';
      default:
        return '📝';
    }
  };

  return (
    <div className="p-6">
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-xl font-semibold text-white">Live Logs</h2>
        <div className="flex items-center space-x-4">
          <select
            value={logLevel}
            onChange={(e) => setLogLevel(e.target.value)}
            className="bg-gray-700 text-white px-3 py-2 rounded-lg border border-gray-600"
          >
            <option value="ALL">All Levels</option>
            <option value="ERROR">Errors Only</option>
            <option value="WARNING">Warnings Only</option>
            <option value="INFO">Info Only</option>
          </select>
          <button className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg text-sm">
            🔄 Refresh
          </button>
        </div>
      </div>

      <div className="bg-gray-900 rounded-lg p-4 max-h-96 overflow-y-auto">
        <div className="space-y-2">
          {filteredLogs.map((log, index) => (
            <div
              key={index}
              className={`p-3 rounded-lg border-l-4 bg-gray-800 ${getLevelColor(log.level)}`}
            >
              <div className="flex items-start space-x-3">
                <span className="text-lg">{getLevelIcon(log.level)}</span>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center space-x-2 mb-1">
                    <span className="text-sm font-mono text-gray-400">{log.timestamp}</span>
                    <span className={`text-xs font-medium px-2 py-1 rounded ${getLevelColor(log.level)}`}>
                      {log.level}
                    </span>
                    <span className="text-xs text-gray-500">{log.service}</span>
                  </div>
                  <p className="text-sm text-gray-300">{log.message}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="mt-4 text-center">
        <p className="text-sm text-gray-400">
          💡 This is a simplified log view. For full log aggregation, use the Prometheus/Grafana stack.
        </p>
      </div>
    </div>
  );
};

export default LiveLogs;
