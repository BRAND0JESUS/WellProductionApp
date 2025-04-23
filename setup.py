import setuptools
import os
from setuptools import find_packages, setup

# Read requirements
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="well_production_app",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Professional Well Production Visualization Application",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/well_production_app",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: Other/Proprietary License",
        "Operating System :: Microsoft :: Windows",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "well_production_app=main_app:main",
        ],
    },
)