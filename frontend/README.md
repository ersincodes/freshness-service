# Freshness Service Frontend

A modern React + TypeScript chat application for the Freshness Service, providing a full-featured UI for interacting with the AI-powered search assistant.

## Features

- **Chat Interface**: Full chat experience with message history, markdown rendering, and streaming responses
- **Mode Badges**: Visual indicators showing retrieval mode (Online, Archive, Local)
- **Source Inspector**: View and inspect sources used to generate answers
- **Archive Browser**: Search and browse archived web pages
- **Settings & Health**: View system configuration and service health status

## Tech Stack

- **React 18** with TypeScript
- **Vite** for fast development and building
- **TailwindCSS** for styling
- **TanStack Query** for server state management
- **react-markdown** for rendering markdown content
- **Lucide React** for icons

## Getting Started

### Prerequisites

- Node.js 18+
- npm or pnpm
- Backend server running (FastAPI)

### Installation

```bash
# Install dependencies
npm install

# Start development server
npm run dev
```

The frontend will be available at `http://localhost:5173`.

### Environment Variables

Create a `.env` file in the frontend directory:

```env
VITE_API_BASE_URL=http://localhost:8000
```

## Project Structure

```
frontend/
├── src/
│   ├── components/
│   │   ├── archive/        # Archive browser components
│   │   ├── chat/           # Chat UI components
│   │   ├── layout/         # Layout components (sidebar)
│   │   ├── settings/       # Settings view components
│   │   └── ui/             # Base UI components
│   ├── lib/
│   │   ├── api.ts          # API client functions
│   │   ├── hooks.ts        # React Query hooks
│   │   ├── types.ts        # TypeScript type definitions
│   │   └── utils.ts        # Utility functions
│   ├── store/
│   │   └── chat-store.ts   # Chat state management
│   ├── App.tsx             # Main application component
│   ├── main.tsx            # Entry point
│   └── index.css           # Global styles
├── index.html
├── package.json
├── tailwind.config.js
├── postcss.config.js
├── tsconfig.json
└── vite.config.ts
```

## API Integration

The frontend communicates with the FastAPI backend through these endpoints:

- `POST /api/chat` - Send chat messages (non-streaming)
- `POST /api/chat/stream` - Stream chat responses via SSE
- `GET /api/archive/search` - Search archived pages
- `GET /api/archive/page/{url_hash}` - Get archived page details
- `GET /api/settings` - Get current settings
- `GET /api/health` - Check service health

## Development

```bash
# Run development server with hot reload
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview

# Type check
npm run type-check

# Lint
npm run lint
```

## License

MIT
