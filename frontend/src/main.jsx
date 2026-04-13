import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";

class ErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { hasError: false }; }
  static getDerivedStateFromError() { return { hasError: true }; }
  componentDidCatch(err, info) { console.error('App crash:', err, info); }
  render() {
    if (this.state.hasError) {
      return (
        <div style={{ minHeight: '100vh', background: '#f5f2ec', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: "'IBM Plex Mono', monospace" }}>
          <div style={{ textAlign: 'center', color: '#3a3a3a' }}>
            <div style={{ fontSize: 14, marginBottom: 8 }}>Something went wrong</div>
            <button onClick={() => window.location.reload()} style={{ padding: '6px 16px', border: '1px solid #3a3a3a', background: 'transparent', cursor: 'pointer', fontFamily: 'inherit', fontSize: 12 }}>Reload</button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>
);
