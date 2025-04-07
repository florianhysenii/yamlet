// src/App.js
import React, { useState, useRef } from 'react';
import { DndProvider, useDrag, useDrop } from 'react-dnd';
import { HTML5Backend } from 'react-dnd-html5-backend';
import yaml from 'js-yaml';

// Define the drag item type.
const ItemTypes = {
  BLOCK: 'block',
};

// Basic inline styles.
const blockStyle = {
  border: '1px solid #ccc',
  borderRadius: '4px',
  padding: '16px',
  margin: '8px 0',
  backgroundColor: '#fff',
  cursor: 'move',
};

const modalOverlayStyle = {
  position: 'fixed',
  top: 0,
  left: 0,
  width: '100%',
  height: '100%',
  backgroundColor: 'rgba(0,0,0,0.5)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
};

const modalContentStyle = {
  backgroundColor: '#fff',
  padding: '20px',
  borderRadius: '4px',
  width: '400px',
  maxWidth: '90%',
};

// ----- PipelineBlock Component -----
function PipelineBlock({ block, index, moveBlock, onEdit }) {
  const ref = useRef(null);

  const [, drop] = useDrop({
    accept: ItemTypes.BLOCK,
    hover(item, monitor) {
      if (!ref.current) return;
      const dragIndex = item.index;
      const hoverIndex = index;
      if (dragIndex === hoverIndex) return;
      const hoverBoundingRect = ref.current.getBoundingClientRect();
      const hoverMiddleY = (hoverBoundingRect.bottom - hoverBoundingRect.top) / 2;
      const clientOffset = monitor.getClientOffset();
      const hoverClientY = clientOffset.y - hoverBoundingRect.top;
      if (dragIndex < hoverIndex && hoverClientY < hoverMiddleY) return;
      if (dragIndex > hoverIndex && hoverClientY > hoverMiddleY) return;
      moveBlock(dragIndex, hoverIndex);
      item.index = hoverIndex;
    },
  });

  const [{ isDragging }, drag] = useDrag({
    type: ItemTypes.BLOCK,
    item: { id: block.id, index, type: ItemTypes.BLOCK },
    collect: (monitor) => ({
      isDragging: monitor.isDragging(),
    }),
  });

  drag(drop(ref));

  return (
    <div
      ref={ref}
      style={{ ...blockStyle, opacity: isDragging ? 0.5 : 1 }}
      onClick={() => onEdit(block.id)}
    >
      <h3>{block.title}</h3>
      <p>{block.description}</p>
    </div>
  );
}

// ----- EditModal Component -----
function EditModal({ block, onClose, onSave }) {
  const [localConfig, setLocalConfig] = useState({ ...block.config });

  const renderFields = () => {
    if (block.type === 'source') {
      return (
        <>
          <label>Connection Name:</label>
          <input
            type="text"
            value={localConfig.connectionName || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, connectionName: e.target.value })
            }
            style={{ width: '100%', marginBottom: '10px' }}
          />
          <label>Table Name:</label>
          <input
            type="text"
            value={localConfig.table || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, table: e.target.value })
            }
            style={{ width: '100%' }}
          />
        </>
      );
    } else if (block.type === 'transformation') {
      return (
        <>
          <label>SQL Query:</label>
          <textarea
            rows="8"
            cols="40"
            value={localConfig.query || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, query: e.target.value })
            }
            style={{ width: '100%' }}
          />
        </>
      );
    } else if (block.type === 'target') {
      return (
        <>
          <label>Connection Name:</label>
          <input
            type="text"
            value={localConfig.connectionName || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, connectionName: e.target.value })
            }
            style={{ width: '100%', marginBottom: '10px' }}
          />
          <label>Table Name:</label>
          <input
            type="text"
            value={localConfig.table || ''}
            onChange={(e) =>
              setLocalConfig({ ...localConfig, table: e.target.value })
            }
            style={{ width: '100%' }}
          />
        </>
      );
    } else {
      return <p>No configuration available for this block type.</p>;
    }
  };

  return (
    <div style={modalOverlayStyle}>
      <div style={modalContentStyle}>
        <h2>Edit {block.title}</h2>
        {renderFields()}
        <div style={{ marginTop: '20px', textAlign: 'right' }}>
          <button onClick={() => onSave(block.id, localConfig)} style={{ marginRight: '10px' }}>
            Save
          </button>
          <button onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  );
}

// ----- PipelineDesigner Component -----
function PipelineDesigner() {
  const [blocks, setBlocks] = useState([
    {
      id: 1,
      type: 'source',
      title: 'Source',
      description: 'Configure your SQL source connection.',
      config: { connectionName: '', table: '' },
    },
    {
      id: 2,
      type: 'transformation',
      title: 'Transformation',
      description: 'Define your SQL transformation.',
      config: {
        query:
          "-- Write your SQL transformation here.\nCREATE OR REPLACE TEMP VIEW enriched AS\nSELECT id, UPPER(name) AS name, some_column,\nCASE WHEN some_column > 250 THEN 'High' ELSE 'Low' END AS score_category\nFROM temp_table;\nSELECT * FROM enriched;",
      },
    },
    {
      id: 3,
      type: 'target',
      title: 'Target',
      description: 'Configure your target connection.',
      config: { connectionName: '', table: '' },
    },
  ]);
  const [editingBlockId, setEditingBlockId] = useState(null);
  const [generatedYaml, setGeneratedYaml] = useState('');
  const [submitMessage, setSubmitMessage] = useState('');

  const moveBlock = (fromIndex, toIndex) => {
    const updated = [...blocks];
    const [removed] = updated.splice(fromIndex, 1);
    updated.splice(toIndex, 0, removed);
    setBlocks(updated);
  };

  const handleEdit = (blockId) => {
    setEditingBlockId(blockId);
  };

  const handleCloseModal = () => {
    setEditingBlockId(null);
  };

  const handleSaveBlock = (blockId, newConfig) => {
    const updated = blocks.map((b) =>
      b.id === blockId ? { ...b, config: newConfig } : b
    );
    setBlocks(updated);
    setEditingBlockId(null);
  };

  // Generate pipeline YAML with structured sections.
  const generatePipelineYaml = () => {
    const sourceBlock = blocks.find((b) => b.type === 'source') || {};
    const transformationBlock = blocks.find((b) => b.type === 'transformation') || {};
    const targetBlock = blocks.find((b) => b.type === 'target') || {};

    const pipeline = {
      name: "TestPipeline",
      source: {
        type: "mysql",
        connection: sourceBlock.config.connectionName || "testdb",
        config: {
          table: sourceBlock.config.table || "reports",
          cdc: {
            enabled: false,
            mode: "timestamp",
            timestamp: { column: "some_column" },
          },
        },
      },
      transformations: [
        {
          type: "sql",
          config: {
            query: transformationBlock.config.query || "-- SQL transformation placeholder",
          },
        },
      ],
      target: {
        type: "jdbc",
        connection: targetBlock.config.connectionName || "testdb",
        config: {
          db_type: "mysql",
          table: targetBlock.config.table || "target_reports",
          mode: "append",
        },
      },
      scheduler: "cron",
      schedule_cron: "0 2 * * *",
    };

    return yaml.dump(pipeline);
  };

  const handleGenerateYaml = () => {
    const yamlStr = generatePipelineYaml();
    setGeneratedYaml(yamlStr);
  };

  // Submit the generated YAML to the backend.
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

  return (
    <div style={{ maxWidth: '700px', margin: '0 auto', padding: '20px', fontFamily: 'Arial, sans-serif' }}>
      <h2>Pipeline Designer (Drag & Drop)</h2>
      <p>Drag and drop blocks to reorder. Click a block to edit its configuration.</p>
      {blocks.map((block, index) => (
        <PipelineBlock
          key={block.id}
          block={block}
          index={index}
          moveBlock={moveBlock}
          onEdit={handleEdit}
        />
      ))}
      <div style={{ marginTop: '20px' }}>
        <button onClick={handleGenerateYaml} style={{ marginRight: '10px' }}>
          Generate Pipeline YAML
        </button>
        <button onClick={handleSubmitPipeline}>Run Pipeline</button>
      </div>
      {generatedYaml && (
        <div style={{ marginTop: '20px', backgroundColor: '#f0f0f0', padding: '10px' }}>
          <h4>Generated Pipeline YAML</h4>
          <pre>{generatedYaml}</pre>
        </div>
      )}
      {submitMessage && (
        <div style={{ marginTop: '20px', padding: '10px', border: '1px solid #ccc' }}>
          <strong>Response:</strong>
          <pre>{submitMessage}</pre>
        </div>
      )}
      {editingBlockId && (
        <EditModal
          block={blocks.find((b) => b.id === editingBlockId)}
          onClose={handleCloseModal}
          onSave={handleSaveBlock}
        />
      )}
    </div>
  );
}

function App() {
  return (
    <DndProvider backend={HTML5Backend}>
      <PipelineDesigner />
    </DndProvider>
  );
}

export default App;
