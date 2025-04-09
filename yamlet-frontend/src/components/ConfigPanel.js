import React, { useState } from 'react';
import { Slide, Box, Typography, TextField, Button } from '@mui/material';

function ConfigPanel({ block, onSave, onClose }) {
  const [localConfig, setLocalConfig] = useState({ ...block.config });

  const renderFields = () => {
    // For source, show connection name and SQL query.
    if (block.type === 'source') {
      return (
        <>
          <TextField
            label="Connection Name"
            variant="outlined"
            size="small"
            fullWidth
            sx={{ mb: 2 }}
            value={localConfig.connectionName || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, connectionName: e.target.value })
            }
          />
          <TextField
            label="SQL Query"
            variant="outlined"
            size="small"
            fullWidth
            multiline
            rows={3}
            sx={{ mb: 2 }}
            value={localConfig.query || 'SELECT * FROM reports'}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, query: e.target.value })
            }
          />
        </>
      );
    }
    // For target, show connection name and table name.
    if (block.type === 'target') {
      return (
        <>
          <TextField
            label="Connection Name"
            variant="outlined"
            size="small"
            fullWidth
            sx={{ mb: 2 }}
            value={localConfig.connectionName || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, connectionName: e.target.value })
            }
          />
          <TextField
            label="Table Name"
            variant="outlined"
            size="small"
            fullWidth
            sx={{ mb: 2 }}
            value={localConfig.table || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, table: e.target.value })
            }
          />
        </>
      );
    }
    // For transformation blocks, show the SQL query.
    if (block.type === 'transformation') {
      return (
        <TextField
          label="SQL Query"
          variant="outlined"
          size="small"
          fullWidth
          multiline
          rows={6}
          sx={{ mb: 2 }}
          value={localConfig.query || ''}
          onChange={(e) =>
            setLocalConfig({ ...localConfig, query: e.target.value })
          }
        />
      );
    }
    return <Typography>No configuration available for this block type.</Typography>;
  };

  return (
    <Slide in={true} direction="up" mountOnEnter unmountOnExit>
      <Box
        sx={{
          position: 'fixed',
          bottom: 0,
          left: 0,
          right: 0,
          backgroundColor: '#fff',
          borderTop: '1px solid #ccc',
          p: 2,
          zIndex: 1300,
        }}
      >
        <Typography variant="h6" gutterBottom>
          Editing: {block.title}
        </Typography>
        {renderFields()}
        <Box sx={{ textAlign: 'right' }}>
          <Button variant="contained" onClick={() => onSave(block.id, localConfig)} sx={{ mr: 1 }}>
            Save
          </Button>
          <Button variant="outlined" onClick={onClose}>
            Cancel
          </Button>
        </Box>
      </Box>
    </Slide>
  );
}

export default ConfigPanel;
