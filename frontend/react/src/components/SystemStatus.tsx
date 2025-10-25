import React from 'react';
import { CheckCircleIcon, XCircleIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline';

const SystemStatus: React.FC = () => {
  const services = [
    { name: 'Metrics Server', status: 'online', endpoint: 'http://localhost:8000/metrics' },
    { name: 'Database', status: 'online', endpoint: 'postgresql://dev:dev@localhost:5432/chillflow' },
    { name: 'Kafka', status: 'unknown', endpoint: 'localhost:9092' },
    { name: 'Redis', status: 'unknown', endpoint: 'localhost:6379' },
  ];

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'online':
        return <CheckCircleIcon className="h-5 w-5 text-green-500" />;
      case 'offline':
        return <XCircleIcon className="h-5 w-5 text-red-500" />;
      default:
        return <ExclamationTriangleIcon className="h-5 w-5 text-yellow-500" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'online':
        return 'text-green-400';
      case 'offline':
        return 'text-red-400';
      default:
        return 'text-yellow-400';
    }
  };

  return (
    <div className="p-6">
      <h2 className="text-xl font-semibold text-white mb-6">System Status</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Service Status */}
        <div className="bg-gray-700 rounded-lg p-6">
          <h3 className="text-lg font-medium text-white mb-4">Service Status</h3>
          <div className="space-y-4">
            {services.map((service, index) => (
              <div key={index} className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  {getStatusIcon(service.status)}
                  <span className="text-gray-300">{service.name}</span>
                </div>
                <span className={`text-sm font-medium ${getStatusColor(service.status)}`}>
                  {service.status.toUpperCase()}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Quick Actions */}
        <div className="bg-gray-700 rounded-lg p-6">
          <h3 className="text-lg font-medium text-white mb-4">Quick Actions</h3>
          <div className="space-y-3">
            <button className="w-full bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg text-sm">
              🔄 Refresh All Data
            </button>
            <button className="w-full bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg text-sm">
              📊 Generate Test Data
            </button>
            <button className="w-full bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded-lg text-sm">
              🗄️ Check Database
            </button>
          </div>
        </div>
      </div>

      {/* System Information */}
      <div className="mt-6">
        <h3 className="text-lg font-medium text-white mb-4">System Information</h3>
        <div className="bg-gray-700 rounded-lg p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <p className="text-sm text-gray-400">Environment</p>
              <p className="text-white">Development</p>
            </div>
            <div>
              <p className="text-sm text-gray-400">Last Updated</p>
              <p className="text-white">{new Date().toLocaleString()}</p>
            </div>
            <div>
              <p className="text-sm text-gray-400">Version</p>
              <p className="text-white">1.0.0</p>
            </div>
            <div>
              <p className="text-sm text-gray-400">Status</p>
              <p className="text-green-400">Operational</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SystemStatus;
