from setuptools import setup, find_packages

setup(
    name="mallarddv",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "duckdb",
    ],
    description="A lightweight Python implementation of a Data Vault data warehouse pattern using DuckDB.",
    author="MallardDV Contributors",
    author_email="contact@example.com",
    url="https://github.com/michaelsilvestre/mallarddv",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)