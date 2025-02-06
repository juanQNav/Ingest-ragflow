#  prot-ragflow

This repository contains code for integrating and using various components, including the DSpace API and a knowledge base. The structure is organized as follows:

```
src/
├── prot-ragflow/
    ├── dspace_api/
    │   ├── __init__.py
    │   ├── README.md
    │   ├── authentication.py
    │   ├── collections.py
    │   ├── files.py
    ├── raggg/
    │   ├── __init__.py
    │   ├── README.md
    │   ├── files.py
    │   ├── parsing.py
```

## raggg
The raggg module provides functions to upload PDFs to the knowledge base.

## dspace_api
The dspace_api module interacts with the DSpace REST API endpoints.

## scripts
This directory contains the scripts used for testing and experimentation.

## Installation
To use this project, install it as a package by running the following command:

```bash
pip install -e .
``` 

