from setuptools import find_packages, setup

setup(
    name="ingest_ragflow",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests[socks]",
        "tqdm",
        "pandas",
        "ragflow-sdk",
    ],
    python_requires=">=3.10",
)
