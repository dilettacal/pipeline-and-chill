import React, { useState, useEffect } from 'react';
import {
  ChartBarIcon,
  DatabaseIcon,
  CpuChipIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  XCircleIcon
} from '@heroicons/react/24/outline';
import MetricsDashboard from './components/MetricsDashboard';
import DatabaseStats from './components/DatabaseStats';
import SystemStatus from './components/SystemStatus';
import LiveLogs from './components/LiveLogs';

interface SystemMetrics {
  totalTrips: number;
  recentTrips: number;
  totalRevenue: number;
  avgFare: number;
  eventsProcessed: number;
  processingLatency: number;
}

const App: React.FC = () => {
  const [activeTab, setActiveTab] = useState('metrics');
  const [metrics, setMetrics] = useState<SystemMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 300000); // Refresh every 5 minutes
    return () => clearInterval(interval);
  }, []);

  const fetchMetrics = async () => {
    try {
      // Fetch from metrics endpoint
      const metricsResponse = await fetch('http://localhost:8000/metrics');
      if (!metricsResponse.ok) throw new Error('Metrics endpoint not available');

      // Fetch from database API (you'd need to create this)
      const dbResponse = await fetch('/api/database/stats');
      if (!dbResponse.ok) throw new Error('Database API not available');

      const dbStats = await dbResponse.json();

      setMetrics({
        totalTrips: dbStats.totalTrips || 0,
        recentTrips: dbStats.recentTrips || 0,
        totalRevenue: dbStats.totalRevenue || 0,
        avgFare: dbStats.avgFare || 0,
        eventsProcessed: 0, // Parse from metrics response
        processingLatency: 0 // Parse from metrics response
      });

      setLoading(false);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch metrics');
      setLoading(false);
    }
  };

  const tabs = [
    { id: 'metrics', name: 'Metrics', icon: ChartBarIcon },
    { id: 'database', name: 'Database', icon: DatabaseIcon },
    { id: 'system', name: 'System', icon: CpuChipIcon },
    { id: 'logs', name: 'Logs', icon: ExclamationTriangleIcon },
  ];

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-gray-400">Loading ChillFlow Observatory...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <XCircleIcon className="h-16 w-16 text-red-500 mx-auto mb-4" />
          <h2 className="text-2xl font-bold text-white mb-2">Connection Error</h2>
          <p className="text-gray-400 mb-4">{error}</p>
          <button
            onClick={fetchMetrics}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-900">
      {/* Header */}
      <header className="bg-gray-800 border-b border-gray-700">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <h1 className="text-2xl font-bold text-white">🌊 ChillFlow Observatory</h1>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                <CheckCircleIcon className="h-5 w-5 text-green-500" />
                <span className="text-sm text-gray-300">Live</span>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Navigation */}
      <nav className="bg-gray-800 border-b border-gray-700">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex space-x-8">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`flex items-center space-x-2 py-4 px-1 border-b-2 font-medium text-sm ${
                    activeTab === tab.id
                      ? 'border-blue-500 text-blue-400'
                      : 'border-transparent text-gray-400 hover:text-gray-300 hover:border-gray-300'
                  }`}
                >
                  <Icon className="h-5 w-5" />
                  <span>{tab.name}</span>
                </button>
              );
            })}
          </div>
        </div>
      </nav>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto py-6 px-4 sm:px-6 lg:px-8">
        {/* Metrics Cards */}
        {metrics && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <div className="bg-gray-800 rounded-lg p-6">
              <div className="flex items-center">
                <div className="flex-shrink-0">
                  <DatabaseIcon className="h-8 w-8 text-blue-500" />
                </div>
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-400">Total Trips</p>
                  <p className="text-2xl font-semibold text-white">{metrics.totalTrips.toLocaleString()}</p>
                  <p className="text-sm text-green-400">+{metrics.recentTrips} recent</p>
                </div>
              </div>
            </div>

            <div className="bg-gray-800 rounded-lg p-6">
              <div className="flex items-center">
                <div className="flex-shrink-0">
                  <ChartBarIcon className="h-8 w-8 text-green-500" />
                </div>
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-400">Total Revenue</p>
                  <p className="text-2xl font-semibold text-white">${metrics.totalRevenue.toLocaleString()}</p>
                  <p className="text-sm text-gray-400">${metrics.avgFare.toFixed(2)} avg</p>
                </div>
              </div>
            </div>

            <div className="bg-gray-800 rounded-lg p-6">
              <div className="flex items-center">
                <div className="flex-shrink-0">
                  <CpuChipIcon className="h-8 w-8 text-purple-500" />
                </div>
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-400">Events Processed</p>
                  <p className="text-2xl font-semibold text-white">{metrics.eventsProcessed.toLocaleString()}</p>
                  <p className="text-sm text-blue-400">Live</p>
                </div>
              </div>
            </div>

            <div className="bg-gray-800 rounded-lg p-6">
              <div className="flex items-center">
                <div className="flex-shrink-0">
                  <ExclamationTriangleIcon className="h-8 w-8 text-yellow-500" />
                </div>
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-400">Processing Time</p>
                  <p className="text-2xl font-semibold text-white">{metrics.processingLatency.toFixed(2)}s</p>
                  <p className="text-sm text-gray-400">Total</p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Tab Content */}
        <div className="bg-gray-800 rounded-lg">
          {activeTab === 'metrics' && <MetricsDashboard />}
          {activeTab === 'database' && <DatabaseStats />}
          {activeTab === 'system' && <SystemStatus />}
          {activeTab === 'logs' && <LiveLogs />}
        </div>
      </main>
    </div>
  );
};

export default App;
