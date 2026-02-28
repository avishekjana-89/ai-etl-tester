import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import Dashboard from './pages/Dashboard';
import Connectors from './pages/Connectors';
import Upload from './pages/Upload';
import MappingReview from './pages/MappingReview';
import TestCases from './pages/TestCases';
import Results from './pages/Results';
import './index.css';

function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <aside className="sidebar">
          <div className="sidebar-logo">
            AI Powered ETL Tester
          </div>
          <nav className="sidebar-nav">
            <NavLink to="/" end>
              <span className="icon">📊</span> Dashboard
            </NavLink>
            <NavLink to="/connectors">
              <span className="icon">🔌</span> Connectors
            </NavLink>
            <NavLink to="/upload">
              <span className="icon">📄</span> Upload Mapping
            </NavLink>
            <NavLink to="/mappings">
              <span className="icon">🔗</span> Mappings
            </NavLink>
            <NavLink to="/testcases">
              <span className="icon">🧪</span> Test Cases
            </NavLink>
            <NavLink to="/results">
              <span className="icon">📋</span> Results
            </NavLink>
          </nav>
        </aside>
        <main className="main">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/connectors" element={<Connectors />} />
            <Route path="/upload" element={<Upload />} />
            <Route path="/mappings" element={<MappingReview />} />
            <Route path="/mappings/:id" element={<MappingReview />} />
            <Route path="/testcases" element={<TestCases />} />
            <Route path="/results" element={<Results />} />
            <Route path="/results/:id" element={<Results />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
