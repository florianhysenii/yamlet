// src/App.js
import React, { useState, useRef, useEffect } from 'react';
import { DndProvider, useDrag, useDrop } from 'react-dnd';
import { HTML5Backend } from 'react-dnd-html5-backend';
import yaml from 'js-yaml';

// Material-UI components
import {
  AppBar,
  Toolbar,
  IconButton,
  Typography,
  Box,
  Paper,
  TextField,
  Button,
  Fade,
  Slide,
  createTheme,
  ThemeProvider,
  CssBaseline,
} from '@mui/material';

// MUI icons
import StorageIcon from '@mui/icons-material/Storage';
import BuildIcon from '@mui/icons-material/Build';
import CloudDoneIcon from '@mui/icons-material/CloudDone';
import ArrowForwardIosIcon from '@mui/icons-material/ArrowForwardIos';
import AccountCircleIcon from '@mui/icons-material/AccountCircle';

const ItemTypes = {
  BLOCK: 'block',
};

// ------------------- Custom Theme -------------------
const customTheme = createTheme({
  typography: {
    fontFamily: 'Poppins, Roboto, sans-serif',
  },
  palette: {
    primary: {
      main: '#2c3e50',
    },
    secondary: {
      main: '#2980b9',
    },
  },
});

// ------------------- VersionSidebar Component -------------------
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

// ------------------- PipelineBlock Component -------------------
function PipelineBlock({ block, index, moveBlock, onSelect }) {
  const ref = useRef(null);

  const [, drop] = useDrop({
    accept: ItemTypes.BLOCK,
    hover(item, monitor) {
      if (!ref.current) return;
      const dragIndex = item.index;
      const hoverIndex = index;
      if (dragIndex === hoverIndex) return;
      const hoverBoundingRect = ref.current.getBoundingClientRect();
      const hoverMiddleX =
        (hoverBoundingRect.right - hoverBoundingRect.left) / 2;
      const clientOffset = monitor.getClientOffset();
      const hoverClientX = clientOffset.x - hoverBoundingRect.left;
      if (dragIndex < hoverIndex && hoverClientX < hoverMiddleX) return;
      if (dragIndex > hoverIndex && hoverClientX > hoverMiddleX) return;
      moveBlock(dragIndex, hoverIndex);
      item.index = hoverIndex;
    },
  });

  const [{ isDragging }, drag] = useDrag({
    type: ItemTypes.BLOCK,
    item: { id: block.id, index },
    collect: (monitor) => ({
      isDragging: monitor.isDragging(),
    }),
  });

  const style = {
    width: 130,
    minHeight: 90,
    padding: 1,
    border: '2px solid',
    borderColor: block.selected ? '#2980b9' : '#ccc',
    borderRadius: 2,
    backgroundColor: '#fff',
    cursor: 'move',
    textAlign: 'center',
    opacity: isDragging ? 0.6 : 1,
    boxShadow: block.selected
      ? '0 0 10px rgba(41,128,185,0.4)'
      : '0 1px 3px rgba(0,0,0,0.2)',
    transition: 'transform 0.15s ease-out',
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
  };

  const handleClick = () => {
    if (ref.current) {
      ref.current.style.transform = 'scale(0.95)';
      setTimeout(() => {
        ref.current.style.transform = 'scale(1)';
        onSelect(block.id);
      }, 80);
    }
  };

  drag(drop(ref));

  const getIcon = () => {
    switch (block.type) {
      case 'source':
        return <StorageIcon sx={{ color: '#2ecc71', fontSize: 28 }} />;
      case 'transformation':
        return <BuildIcon sx={{ color: '#e67e22', fontSize: 28 }} />;
      case 'target':
        return <CloudDoneIcon sx={{ color: '#2980b9', fontSize: 28 }} />;
      default:
        return null;
    }
  };

  return (
    <Paper ref={ref} sx={style} onClick={() => onSelect(block.id)}>
      <Box sx={{ mb: 1 }}>{getIcon()}</Box>
      <Typography variant="subtitle2" sx={{ fontWeight: 'bold' }}>
        {block.title}
      </Typography>
      <Typography variant="body2" sx={{ color: '#666' }}>
        {block.description}
      </Typography>
    </Paper>
  );
}

// ------------------- ConfigPanel Component -------------------
function ConfigPanel({ block, onSave, onClose }) {
  const [localConfig, setLocalConfig] = useState({ ...block.config });

  const renderFields = () => {
    // For source blocks, show connection name and SQL query (instead of table)
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
    // For target blocks, show connection name and table name.
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

// ------------------- PipelineDesigner Component -------------------
function PipelineDesigner() {
  const [blocks, setBlocks] = useState([
    {
      id: 1,
      type: 'source',
      title: 'Source',
      description: 'SQL source connection',
      config: { connectionName: '', query: 'SELECT * FROM reports' },
      selected: false,
    },
    {
      id: 2,
      type: 'transformation',
      title: 'Transformation',
      description: 'SQL transformation',
      config: {
        query:
          "-- Write your SQL transformation here.\nCREATE OR REPLACE TEMP VIEW enriched AS\nSELECT id, UPPER(name) AS name, some_column,\nCASE WHEN some_column > 250 THEN 'High' ELSE 'Low' END AS score_category\nFROM temp_table;\nSELECT * FROM enriched;",
      },
      selected: false,
    },
    {
      id: 3,
      type: 'target',
      title: 'Target',
      description: 'Output connection',
      config: { connectionName: '', table: 'target_reports' },
      selected: false,
    },
  ]);

  const [generatedYaml, setGeneratedYaml] = useState('');
  const [submitMessage, setSubmitMessage] = useState('');

  // Determine the selected block (if any) for config editing.
  const selectedBlock = blocks.find((b) => b.selected) || null;

  const moveBlock = (fromIndex, toIndex) => {
    const updated = [...blocks];
    const [removed] = updated.splice(fromIndex, 1);
    updated.splice(toIndex, 0, removed);
    setBlocks(updated);
  };

  const handleSelectBlock = (blockId) => {
    setBlocks(blocks.map((b) => ({ ...b, selected: b.id === blockId })));
  };

  const handleSaveBlock = (blockId, newConfig) => {
    setBlocks(
      blocks.map((b) =>
        b.id === blockId ? { ...b, config: newConfig } : b
      )
    );
  };

  const handleClosePanel = () => {
    setBlocks(blocks.map((b) => ({ ...b, selected: false })));
  };

  // Generate pipeline YAML with your desired structure.
  const generatePipelineYaml = () => {
    const sourceBlock = blocks.find((b) => b.type === 'source') || {};
    const transformationBlock = blocks.find((b) => b.type === 'transformation') || {};
    const targetBlock = blocks.find((b) => b.type === 'target') || {};

    // Build source config with query (no table needed).
    const sourceConfig = {
      host: 'localhost',
      port: 3306,
      database: 'testdb',
      query: sourceBlock.config?.query || 'SELECT * FROM reports',
      cdc: {
        enabled: false,
        mode: 'timestamp',
        timestamp: { column: 'some_column' },
      },
    };

    const pipeline = {
      name: 'TestPipeline',
      source: {
        type: 'mysql',
        connection: sourceBlock.config?.connectionName || 'testdb',
        config: sourceConfig,
      },
      transformations: [
        {
          type: 'sql',
          config: {
            query: transformationBlock.config?.query || '-- no transformation',
          },
        },
      ],
      target: {
        type: 'jdbc',
        connection: targetBlock.config?.connectionName || 'testdb',
        config: {
          db_type: 'mysql',
          host: 'localhost',
          port: 3306,
          database: 'testdb',
          table: targetBlock.config?.table || 'target_reports',
          user: 'root',
          password: 'password',
          mode: 'append',
        },
      },
      scheduler: 'cron',
      schedule_cron: '0 2 * * *',
    };

    return yaml.dump(pipeline);
  };

  const handleGenerateYaml = () => {
    const yamlStr = generatePipelineYaml();
    setGeneratedYaml(yamlStr);
  };

  const handleSubmitPipeline = async () => {
    const yamlStr = generatePipelineYaml();
    const blob = new Blob([yamlStr], { type: 'text/yaml' });
    const formData = new FormData();
    formData.append('file', blob, 'pipeline.yaml');

    try {
      const response = await fetch('/pipelines/run', {
        method: 'POST',
        body: formData,
      });
      const data = await response.json();
      setSubmitMessage(`Success: ${JSON.stringify(data)}`);
    } catch (error) {
      setSubmitMessage(`Error: ${error.toString()}`);
    }
  };

  // New function: Save Pipeline Version (calls separate endpoint)
  const handleSaveVersion = async () => {
    const yamlStr = generatePipelineYaml();
    const blob = new Blob([yamlStr], { type: 'text/yaml' });
    const formData = new FormData();
    formData.append('file', blob, 'pipeline.yaml');

    try {
      // Note: Ensure your backend has an endpoint at /pipelines/save_version
      const response = await fetch('/pipelines/save_version', {
        method: 'POST',
        body: formData,
      });
      const data = await response.json();
      setSubmitMessage(`Version Saved: ${JSON.stringify(data)}`);
    } catch (error) {
      setSubmitMessage(`Error: ${error.toString()}`);
    }
  };

  return (
    <Box sx={{ display: 'flex', height: '100vh', fontFamily: 'Arial, sans-serif' }}>
      {/* Main content area */}
      <Box sx={{ flex: 1, p: 2, overflowY: 'auto' }}>
        <Typography variant="h5" sx={{ mb: 2, textAlign: 'center' }}>
          Pipeline Designer
        </Typography>

        {/* Centered horizontal pipeline */}
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            gap: 2,
            mb: 2,
            flexWrap: 'wrap',
          }}
        >
          {blocks.map((block, index) => (
            <React.Fragment key={block.id}>
              <PipelineBlock
                block={block}
                index={index}
                moveBlock={moveBlock}
                onSelect={handleSelectBlock}
              />
              {index < blocks.length - 1 && (
                <ArrowForwardIosIcon sx={{ fontSize: 30, color: '#7f8c8d' }} />
              )}
            </React.Fragment>
          ))}
        </Box>

        {/* Action buttons */}
        <Box sx={{ mt: 2, textAlign: 'center' }}>
          <Button variant="contained" color="primary" sx={{ mr: 1 }} onClick={handleGenerateYaml}>
            Generate YAML
          </Button>
          <Button variant="contained" color="secondary" sx={{ mr: 1 }} onClick={handleSubmitPipeline}>
            Run Pipeline
          </Button>
          <Button variant="outlined" onClick={handleSaveVersion}>
            Save Pipeline Version
          </Button>
        </Box>

        {/* YAML output */}
        {generatedYaml && (
          <Fade in={true}>
            <Box
              sx={{
                mt: 2,
                p: 2,
                backgroundColor: '#eee',
                borderRadius: 2,
                overflowX: 'auto',
              }}
            >
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Generated Pipeline YAML
              </Typography>
              <pre style={{ margin: 0 }}>{generatedYaml}</pre>
            </Box>
          </Fade>
        )}

        {/* Submission response */}
        {submitMessage && (
          <Box
            sx={{
              mt: 2,
              p: 2,
              border: '1px solid #ccc',
              borderRadius: 1,
              textAlign: 'center',
            }}
          >
            <Typography variant="subtitle1">Response:</Typography>
            <pre style={{ margin: 0 }}>{submitMessage}</pre>
          </Box>
        )}
      </Box>

      {/* Right sidebar for pipeline versions */}
      <VersionSidebar pipelineName="TestPipeline" />

      {/* Bottom config panel: shown when a block is selected */}
      {selectedBlock && (
        <ConfigPanel
          block={selectedBlock}
          onSave={handleSaveBlock}
          onClose={handleClosePanel}
        />
      )}
    </Box>
  );
}

export default function App() {
  return (
    <ThemeProvider theme={customTheme}>
      <CssBaseline />
      <DndProvider backend={HTML5Backend}>
        <PipelineDesigner />
      </DndProvider>
    </ThemeProvider>
  );
}
