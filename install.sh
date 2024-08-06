#!/bin/bash

# Check if Python is installed
if ! command -v python3 &> /dev/null
then
    echo "Python3 is not installed. Please install it before continuing."
    exit 1
fi

# Check if Node.js is installed
if ! command -v node &> /dev/null
then
    echo "Node.js is not installed. Please install it before continuing."
    exit 1
fi

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install Python dependencies
pip install -r requirements.txt

# Create or update React app
if [ ! -d "react_app" ]; then
    npx create-react-app react_app
fi

cd react_app

# Ensure package.json exists and is valid
if [ ! -s "package.json" ]; then
    echo '{
  "name": "react_app",
  "version": "0.1.0",
  "private": true,
  "dependencies": {
    "react": "^17.0.2",
    "react-dom": "^17.0.2",
    "react-scripts": "4.0.3",
    "d3": "^7.0.0"
  },
  "scripts": {
    "start": "react-scripts start",
    "build": "react-scripts build",
    "test": "react-scripts test",
    "eject": "react-scripts eject"
  },
  "eslintConfig": {
    "extends": [
      "react-app",
      "react-app/jest"
    ]
  },
  "browserslist": {
    "production": [
      ">0.2%",
      "not dead",
      "not op_mini all"
    ],
    "development": [
      "last 1 chrome version",
      "last 1 firefox version",
      "last 1 safari version"
    ]
  }
}' > package.json
fi

# Install dependencies
npm install

# Return to root directory
cd ..

echo "Setup completed. Here's how to run the project:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Run the Python pipeline: python data_pipeline/main_pipeline.py"
echo "3. Run the React application: cd react_app && npm start"