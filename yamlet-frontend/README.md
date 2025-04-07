# Yamlet Frontend

**Yamlet Frontend** is a React-based user interface for visually building YAML-based data pipelines using a drag-and-drop experience. It serializes the configuration into a YAML file and sends it to the Yamlet FastAPI backend for execution using Apache PySpark.

## 🖼️ Features

- Drag-and-drop pipeline builder
- Converts UI blocks into YAML
- Sends pipeline definition to the backend for execution

## 📦 Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-username/yamlet-frontend.git
cd yamlet-frontend
```

### 2. Install dependencies
```
npm install

```
### 3. Start the development server

```
npm start
```

 The frontend will run on http://localhost:3000 and automatically proxy API requests to http://localhost:8000 (FastAPI backend).

# 🔁 Flow
User builds the pipeline using the drag-and-drop UI.

The app generates the corresponding YAML configuration using js-yaml.

The YAML is POSTed to a backend endpoint (/run/data-pipeline) to trigger the data pipeline execution.

# 🧩 Notes
Ensure yamlet (backend) is running at http://localhost:8000.

Backend expects a POST request with the YAML content to deserialize and run the pipeline.

