#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="katsdpmetawriter",
    description="Karoo Array Telescope Meta Data Writer",
    author="Simon ratcliffe",
    packages=find_packages(),
    scripts=[
        "scripts/meta_writer.py"
        ],
    setup_requires=['katversion'],
    install_requires=[
        'aiokatcp',
        'boto',
        'katsdptelstate',
        'katsdpservices'
    ],
    use_katversion=True
)
