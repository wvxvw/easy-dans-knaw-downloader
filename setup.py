#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name='easy-dans-knaw-downloader',
    version='0.0.0',
    description='Download data from easy.dans.knaw.nl',
    author='olegsivokon@gmail.com',
    url='https://github.com/wvxvw/easy-dans-knaw-downloader',
    license='MIT',
    packages=['easy_dans_knaw_downloader'],
    install_requires=[
        'selenium==3.141.0',
    ],
    scripts=[
        'bin/dowload',
    ],
)
