// src/components/PipelineDesigner.js
import React, { useState } from 'react';
import { Box, Typography, Button, Fade, TextField } from '@mui/material';
import yaml from 'js-yaml';
import PipelineBlock from './PipelineBlock';
import ConfigPanel from './ConfigPanel';
import VersionSidebar from './VersionSidebar';
import ArrowForwardIosIcon from '@mui/icons-material/ArrowForwardIos';

function PipelineDesigner() {
  // Global pipeline name state (default "TestPipeline")
  const [pipelineName, setPipelineName] = useState("TestPipeline");

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

  // Determine the selected block for config editing.
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

  // Generate pipeline YAML using the global pipeline name.
  const generatePipelineYaml = () => {
    const sourceBlock = blocks.find((b) => b.type === 'source') || {};
    const transformationBlock = blocks.find((b) => b.type === 'transformation') || {};
    const targetBlock = blocks.find((b) => b.type === 'target') || {};

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
      name: pipelineName,
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

  const handleSaveVersion = async () => {
    const yamlStr = generatePipelineYaml();
    const blob = new Blob([yamlStr], { type: 'text/yaml' });
    const formData = new FormData();
    formData.append('file', blob, 'pipeline.yaml');

    try {
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
        {/* Pipeline Name Input */}
        <Box sx={{ textAlign: 'center', mb: 2 }}>
          <TextField
            label="Pipeline Name"
            variant="outlined"
            size="small"
            value={pipelineName}
            onChange={(e) => setPipelineName(e.target.value)}
          />
        </Box>
        
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
      <VersionSidebar pipelineName={pipelineName} />

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

export default PipelineDesigner;
