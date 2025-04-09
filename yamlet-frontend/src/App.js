// src/App.js
import React from 'react';
import { DndProvider } from 'react-dnd';
import { HTML5Backend } from 'react-dnd-html5-backend';
import { createTheme, ThemeProvider, CssBaseline } from '@mui/material';
import PipelineDesigner from './components/PipelineDesigner';

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
