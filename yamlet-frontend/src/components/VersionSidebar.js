import React, { useState, useEffect } from 'react';
import { Box, Typography } from '@mui/material';

function VersionSidebar({ pipelineName }) {
  const [versions, setVersions] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchVersions = async () => {
      try {
        const response = await fetch(`/pipelines/${pipelineName}/versions`);
        if (!response.ok) {
          throw new Error('Failed to fetch versions');
        }
        const data = await response.json();
        setVersions(data.versions || []);
      } catch (err) {
        setError(err.toString());
      }
    };

    fetchVersions();
  }, [pipelineName]);

  return (
    <Box
      sx={{
        position: 'fixed',
        right: 0,
        top: 64, // Below the AppBar
        width: '250px',
        height: 'calc(100% - 64px)',
        backgroundColor: '#fafafa',
        borderLeft: '1px solid #ccc',
        p: 2,
        overflowY: 'auto',
        zIndex: 1200,
      }}
    >
      <Typography variant="h6" gutterBottom>
        Pipeline Versions
      </Typography>
      {error ? (
        <Typography variant="body2" color="error">
          {error}
        </Typography>
      ) : versions.length > 0 ? (
        versions.map((v) => (
          <Box key={v.id} sx={{ mb: 2, borderBottom: '1px solid #ddd', pb: 1 }}>
            <Typography variant="subtitle2">Version: {v.version}</Typography>
            <Typography variant="caption">Created: {v.created_at}</Typography>
          </Box>
        ))
      ) : (
        <Typography variant="body2">No versions available.</Typography>
      )}
    </Box>
  );
}

export default VersionSidebar;
