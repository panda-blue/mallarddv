from setuptools import setup, find_packages

setup(
    name="mallarddv",
    version="1.1.0",
    packages=find_packages(),
    install_requires=[
        "duckdb",
    ],
    description="A lightweight Python implementation of a Data Vault data warehouse pattern using DuckDB.",
    author="panda-blue,michaelsilvestre",
    author_email="contact@example.com",
    url="https://github.com/panda-blue/mallarddv",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)