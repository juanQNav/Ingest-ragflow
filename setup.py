from setuptools import setup, find_packages

setup(
    name="prot_raggflow",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "tqdm",
        # "aiohttp",
    ],
    python_requires=">=3.7",
)