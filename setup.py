from os import path
from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.readlines()

setup(
    name="custom-dremio-connector",
    version="0.2.0",
    author="TIKI",
    python_requires=">=3.9",
    install_requires=requirements,
    packages=find_packages(include=["connector", "connector.*"]),
)
