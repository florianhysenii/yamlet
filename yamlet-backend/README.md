# Yamlet - FastAPI Backend

**Yamlet** is a FastAPI-based backend service for managing YAML-based data pipelines. It works together with the `yamlet-frontend`, and uses Apache PySpark as the data processing engine.

## 🚀 Features
- Load and manage YAML pipeline definitions
- Interface with Apache PySpark for execution
- Designed to be used with the Yamlet frontend

## 🛠️ Setup

### 1. Clone the repository

```bash
git clone https://github.com/glimmerpulse/yamlet.git
cd yamlet
```
### 2. Create a virtual environment
```
python3 -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```
### 3. Install requirements
```
pip install -r requirements.txt

```
### 4.4. Run the FastAPI server
```
uvicorn yamlet.api:app --reload
```


### to run migrations alembic init alembic.
# 🧩 Notes
Make sure Java and PySpark are installed and configured on your system.

This backend is meant to work with the Yamlet frontend.

### Made with ❤️ for streamlined data pipeline development.

