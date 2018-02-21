#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="katsdpmetawriter",
    description="Karoo Array Telescope Meta Data Writer",
    author="Simon ratcliffe",
    packages=find_packages(),
    scripts=[
        "scripts/meta_writer.py"
        ],
    dependency_links=[
        'git+ssh://git@github.com/ska-sa/katsdptelstate#egg=katsdptelstate',
        'git+ssh://git@github.com/ska-sa/katversion#egg=katversion'
    ],
    setup_requires=['katversion'],
    install_requires=[
        'aiokatcp',
        'boto',
        'h5py',
        'numpy',
        'katsdptelstate',
        'katsdpservices',
        'hiredis',
        'netifaces',
        'futures'
    ],
    use_katversion=True
)
