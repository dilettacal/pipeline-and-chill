import React, { useState, useEffect } from 'react';

interface DatabaseStats {
  totalTrips: number;
  recentTrips: number;
  totalRevenue: number;
  avgFare: number;
  tripsWithRevenue: number;
}

const DatabaseStats: React.FC = () => {
  const [stats, setStats] = useState<DatabaseStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 300000); // Refresh every 5 minutes
    return () => clearInterval(interval);
  }, []);

  const fetchStats = async () => {
    try {
      // In a real app, you'd have a backend API endpoint
      // For now, we'll simulate the data
      const mockStats: DatabaseStats = {
        totalTrips: 9761,
        recentTrips: 150,
        totalRevenue: 125430.50,
        avgFare: 12.85,
        tripsWithRevenue: 8500
      };

      setStats(mockStats);
    } catch (error) {
      console.error('Failed to fetch database stats:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-700 rounded w-1/4 mb-4"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="h-32 bg-gray-700 rounded"></div>
            <div className="h-32 bg-gray-700 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (!stats) {
    return (
      <div className="p-6">
        <div className="text-center">
          <p className="text-gray-400">Failed to load database statistics</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <h2 className="text-xl font-semibold text-white mb-6">Database Statistics</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {/* Total Trips */}
        <div className="bg-gray-700 rounded-lg p-6">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-8 h-8 bg-blue-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold">📊</span>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-400">Total Trips</p>
              <p className="text-2xl font-semibold text-white">{stats.totalTrips.toLocaleString()}</p>
            </div>
          </div>
        </div>

        {/* Recent Trips */}
        <div className="bg-gray-700 rounded-lg p-6">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-8 h-8 bg-green-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold">🕐</span>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-400">Recent (1h)</p>
              <p className="text-2xl font-semibold text-white">{stats.recentTrips.toLocaleString()}</p>
            </div>
          </div>
        </div>

        {/* Total Revenue */}
        <div className="bg-gray-700 rounded-lg p-6">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-8 h-8 bg-yellow-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold">💰</span>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-400">Total Revenue</p>
              <p className="text-2xl font-semibold text-white">${stats.totalRevenue.toLocaleString()}</p>
            </div>
          </div>
        </div>

        {/* Average Fare */}
        <div className="bg-gray-700 rounded-lg p-6">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-8 h-8 bg-purple-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold">📈</span>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-400">Average Fare</p>
              <p className="text-2xl font-semibold text-white">${stats.avgFare.toFixed(2)}</p>
            </div>
          </div>
        </div>

        {/* Trips with Revenue */}
        <div className="bg-gray-700 rounded-lg p-6">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-8 h-8 bg-red-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold">✅</span>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-400">With Revenue</p>
              <p className="text-2xl font-semibold text-white">{stats.tripsWithRevenue.toLocaleString()}</p>
            </div>
          </div>
        </div>

        {/* Revenue Percentage */}
        <div className="bg-gray-700 rounded-lg p-6">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <div className="w-8 h-8 bg-indigo-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold">%</span>
              </div>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-400">Revenue Coverage</p>
              <p className="text-2xl font-semibold text-white">
                {((stats.tripsWithRevenue / stats.totalTrips) * 100).toFixed(1)}%
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Data Table */}
      <div className="mt-8">
        <h3 className="text-lg font-medium text-white mb-4">Summary Table</h3>
        <div className="bg-gray-700 rounded-lg overflow-hidden">
          <table className="min-w-full divide-y divide-gray-600">
            <thead className="bg-gray-800">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                  Metric
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                  Value
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-300 uppercase tracking-wider">
                  Percentage
                </th>
              </tr>
            </thead>
            <tbody className="bg-gray-700 divide-y divide-gray-600">
              <tr>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-300">Total Trips</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-white font-mono">{stats.totalTrips.toLocaleString()}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-300">100%</td>
              </tr>
              <tr>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-300">Recent Trips (1h)</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-white font-mono">{stats.recentTrips.toLocaleString()}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-300">
                  {((stats.recentTrips / stats.totalTrips) * 100).toFixed(1)}%
                </td>
              </tr>
              <tr>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-300">Trips with Revenue</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-white font-mono">{stats.tripsWithRevenue.toLocaleString()}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-300">
                  {((stats.tripsWithRevenue / stats.totalTrips) * 100).toFixed(1)}%
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default DatabaseStats;
