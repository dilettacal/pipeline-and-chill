import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';

const MetricsDashboard: React.FC = () => {
  const [metrics, setMetrics] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 300000); // Refresh every 5 minutes
    return () => clearInterval(interval);
  }, []);

  const fetchMetrics = async () => {
    try {
      const response = await fetch('http://localhost:8000/metrics');
      if (response.ok) {
        const text = await response.text();
        // Parse Prometheus metrics (simplified)
        const parsedMetrics = parsePrometheusMetrics(text);
        setMetrics(parsedMetrics);
      }
    } catch (error) {
      console.error('Failed to fetch metrics:', error);
    } finally {
      setLoading(false);
    }
  };

  const parsePrometheusMetrics = (text: string) => {
    const lines = text.split('\n');
    const metrics: any[] = [];

    lines.forEach(line => {
      if (line.startsWith('#') || !line.trim()) return;

      const parts = line.split(' ');
      if (parts.length >= 2) {
        const name = parts[0];
        const value = parseFloat(parts[1]);

        if (!isNaN(value)) {
          metrics.push({
            name: name.replace(/_/g, ' ').toUpperCase(),
            value: value,
            timestamp: new Date().toISOString()
          });
        }
      }
    });

    return metrics;
  };

  if (loading) {
    return (
      <div className="p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-700 rounded w-1/4 mb-4"></div>
          <div className="h-64 bg-gray-700 rounded"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <h2 className="text-xl font-semibold text-white mb-6">Real-time Metrics</h2>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Metrics List */}
        <div className="bg-gray-700 rounded-lg p-4">
          <h3 className="text-lg font-medium text-white mb-4">Current Metrics</h3>
          <div className="space-y-3">
            {metrics.map((metric, index) => (
              <div key={index} className="flex justify-between items-center">
                <span className="text-gray-300">{metric.name}</span>
                <span className="text-white font-mono">{metric.value.toFixed(2)}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Chart Placeholder */}
        <div className="bg-gray-700 rounded-lg p-4">
          <h3 className="text-lg font-medium text-white mb-4">Metrics Over Time</h3>
          <div className="h-64 flex items-center justify-center">
            <p className="text-gray-400">Chart visualization coming soon...</p>
          </div>
        </div>
      </div>

      {/* Raw Metrics */}
      <div className="mt-6">
        <h3 className="text-lg font-medium text-white mb-4">Raw Metrics Data</h3>
        <div className="bg-gray-900 rounded-lg p-4">
          <pre className="text-sm text-gray-300 overflow-x-auto">
            {metrics.length > 0 ? JSON.stringify(metrics, null, 2) : 'No metrics data available'}
          </pre>
        </div>
      </div>
    </div>
  );
};

export default MetricsDashboard;
