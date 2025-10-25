# 🌊 ChillFlow Frontend

This directory contains the frontend components for the ChillFlow Observatory.

## 📁 Structure

```
frontend/
├── streamlit/          # Streamlit dashboard (Python)
├── react/             # React dashboard (JavaScript/TypeScript)
├── components/        # Shared components and utilities
└── README.md         # This file
```

## 🚀 Quick Start

### Option 1: Streamlit Dashboard (Recommended for AI/LLM integration)

```bash
# Install dependencies
uv pip install streamlit plotly requests openai anthropic

# Run the basic dashboard
streamlit run frontend/streamlit/dashboard.py

# Run the AI-powered dashboard
streamlit run frontend/streamlit/dashboard_ai.py
```

### Option 2: React Dashboard (For production use)

```bash
cd frontend/react
npm install
npm start
```

## 🎯 Features

### Streamlit Dashboard
- ✅ **Real-time metrics** from Prometheus
- ✅ **Database statistics** from PostgreSQL
- ✅ **AI-powered insights** with OpenAI/Anthropic
- ✅ **Interactive charts** with Plotly
- ✅ **Live logs** simulation
- ✅ **System status** monitoring

### React Dashboard (Coming Soon)
- 🔄 **Real-time updates** with WebSockets
- 📊 **Advanced visualizations** with D3.js
- 🎨 **Modern UI** with Tailwind CSS
- 📱 **Responsive design** for mobile
- 🔐 **Authentication** and user management

## 🤖 AI Integration

The Streamlit dashboard includes AI capabilities:

- **Natural Language Queries**: "Show me trips from Manhattan"
- **Performance Analysis**: AI insights on metrics
- **Anomaly Detection**: Identify potential issues
- **Optimization Suggestions**: Get recommendations

## 🔧 Configuration

### Environment Variables

```bash
# For AI features
export OPENAI_API_KEY="your-openai-key"
export ANTHROPIC_API_KEY="your-anthropic-key"

# For database connection
export DATABASE_URL="postgresql://dev:dev@localhost:5432/chillflow"
```

### Makefile Commands

```bash
# Start the Streamlit dashboard
make frontend streamlit

# Start the AI-powered dashboard
make frontend ai

# Start the React dashboard
make frontend react

# Install all dependencies
make frontend install
```

## 📊 Data Sources

The frontend connects to:

- **Metrics**: `http://localhost:8000/metrics` (Prometheus format)
- **Database**: `postgresql://dev:dev@localhost:5432/chillflow`
- **Kafka**: `localhost:9092` (for real-time events)
- **Redis**: `localhost:6379` (for caching)

## 🎨 Customization

### Streamlit Themes
- Dark theme (default)
- Light theme
- Custom CSS

### React Components
- Reusable dashboard components
- Custom chart components
- API client utilities

## 🚀 Deployment

### Local Development
```bash
make frontend dev
```

### Production
```bash
make frontend build
make frontend deploy
```

## 🤝 Contributing

1. Add new components to `frontend/components/`
2. Update documentation in `frontend/README.md`
3. Test with `make frontend test`
4. Deploy with `make frontend deploy`
