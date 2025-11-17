from setuptools import find_packages, setup

setup(
    name="ingest_ragflow",
    version="0.1.0",
    description="This repository contains code for integrating "
    "and using various components, including the "
    "DSpace API and a knowledge base in Ragflow.",
    author="juanQNav et al.",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests[socks]",
        "tqdm",
        "pandas",
        "ragflow-sdk==0.22.0",
    ],
    python_requires=">=3.10",
    extras_require={
        "dev": [
            "ruff>=0.13.2",
            "flake8>=7.1.1,<7.2",
            "coverage>=7.10,<7.11",
            "pytest>=8.4.1",
        ],
    },
)
