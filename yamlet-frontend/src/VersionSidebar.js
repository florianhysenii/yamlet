// src/VersionSidebar.js
import React, { useEffect, useState } from 'react';

const VersionSidebar = ({ pipelineName }) => {
  const [versions, setVersions] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchVersions = async () => {
      try {
        const response = await fetch(`/pipelines/${pipelineName}/versions`);
        if (!response.ok) {
          throw new Error("Failed to fetch versions");
        }
        const data = await response.json();
        setVersions(data.versions);
      } catch (err) {
        setError(err.toString());
      }
    };

    fetchVersions();
  }, [pipelineName]);

  return (
    <div style={{
      position: 'fixed',
      top: 0,
      right: 0,
      width: '250px',
      height: '100%',
      backgroundColor: '#fafafa',
      borderLeft: '1px solid #ccc',
      padding: '10px',
      overflowY: 'auto',
    }}>
      <h3>Pipeline Versions</h3>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      <ul style={{ listStyle: 'none', padding: 0 }}>
        {versions.map(v => (
          <li key={v.id} style={{ marginBottom: '10px', borderBottom: '1px solid #eee', paddingBottom: '5px' }}>
            <strong>Version:</strong> {v.version}<br/>
            <em>Created:</em> {v.created_at}
          </li>
        ))}
      </ul>
    </div>
  );
};

export default VersionSidebar;