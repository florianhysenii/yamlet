import React, { useRef } from 'react';
import { useDrag, useDrop } from 'react-dnd';
import Paper from '@mui/material/Paper';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import StorageIcon from '@mui/icons-material/Storage';
import BuildIcon from '@mui/icons-material/Build';
import CloudDoneIcon from '@mui/icons-material/CloudDone';

const ItemTypes = {
  BLOCK: 'block',
};

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

// eslint-disable-next-line no-unused-vars
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

export default PipelineBlock;
