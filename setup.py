# Copyright 2016 Ricequant All Rights Reserved
from setuptools import setup, find_packages


def readfile(filename):
    with open(filename, mode="rt") as f:
        return f.read()


setup(
    name='fdhandle',
    version='0.1.0',
    url="http://www.ricequant.com",
    author="Ricequant",
    author_email="public@ricequant.com",

    packages=find_packages(exclude=[]),
    package_data={'fdhandle': ['fdhandle.yaml', 'sql/*.sql']},
    include_package_data=True,

    install_requires=readfile("requirements.txt"),

    zip_safe=False,

    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    description="1. recalculate fundamental data from Genius to avoid " \
                "future data;"
                "2. schedule to handle Genius' update data."
)
