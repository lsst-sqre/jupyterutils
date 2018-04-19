"""
Setup Module for LSST Jupyter Utilities
"""
import codecs
import io
import os
import setuptools


def get_version(file, name='__version__'):
    """Get the version of the package from the given file by
    executing it and extracting the given `name`.
    """
    path = os.path.realpath(file)
    version_ns = {}
    with io.open(path, encoding="utf8") as f:
        exec(f.read(), {}, version_ns)
    return version_ns[name]


def local_read(filename):
    """Convenience function for includes"""
    full_filename = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        filename)
    return codecs.open(full_filename, 'r', 'utf-8').read()


NAME = 'jupyterutils'
DESCRIPTION = 'LSST utilites for Jupyter environment'
LONG_DESCRIPTION = local_read("README.md")
VERSION = get_version('%s/_version.py' % NAME)
AUTHOR = 'Adam Thornton'
AUTHOR_EMAIL = 'athornton@lsst.org'
URL = 'https://github.com/sqre-lsst/jupyterutils'
LICENSE = 'MIT'

setuptools.setup(
    name=NAME,
    version=get_version("%s/_version.py" % NAME),
    long_description=LONG_DESCRIPTION,
    packages=setuptools.find_packages(),
    url=URL,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license=LICENSE,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'License :: OSI Approved :: MIT License',
    ],
    keywords='lsst',
    install_requires=[
        'requests>=2.0.0,<3.0.0',
        'kubernetes>=4.0.0,<6.0.0',
        'bokeh>=0.12.15'
    ],
    entry_points={
        'console_scripts': [
            'prepuller = jupyterutils.prepuller:standalone',
            'scanrepo = jupyterutils.scanrepo:standalone'
        ]
    }
)
