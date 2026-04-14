#!/bin/bash
# Install system dependencies
apt-get update
apt-get install -y build-essential python3-dev

# Install pandas from pre-built wheel
pip install --only-binary :all: pandas numpy
pip install -r requirements.txt
