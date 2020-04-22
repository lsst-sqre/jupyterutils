'''Setup module for LSST JupyterHub Utilities.
'''
import codecs
import io
import os
import setuptools


def get_version(file, name='__version__'):
    '''Get the version of the package from the given file by
    executing it and extracting the given `name`.
    '''
    path = os.path.realpath(file)
    version_ns = {}
    with io.open(path, encoding="utf8") as f:
        exec(f.read(), {}, version_ns)
    return version_ns[name]


def local_read(filename):
    '''Convenience function for includes.
    '''
    full_filename = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        filename)
    return codecs.open(full_filename, 'r', 'utf-8').read()


NAME = 'jupyterhubutils'
DESCRIPTION = 'LSST utilites for Jupyter Hub server environment'
LONG_DESCRIPTION = local_read("README.md")
VERSION = get_version('%s/_version.py' % NAME)
AUTHOR = 'Adam Thornton'
AUTHOR_EMAIL = 'athornton@lsst.org'
URL = 'https://github.com/sqre-lsst/jupyterhubutils'
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
        'kubernetes>=10.0.0,<11.0.0',
        'semver>=2.0.0,<3.0.0',
        'oauthenticator>=0.9.0,<1.0.0',
        'jupyter-client>=5.0.0,<7.0.0',
        'jupyterhub-jwtauthenticator>=0.1.0,<1.0.0',
        'jupyterhub-kubespawner>=0.10.0,<1.0.00',
        'jinja2>=2.0.0,<3.0.0',
        'pytz>=2019.3',
        'eliot',
        'eliot-tree',
        'argo-workflows>=3.0.0,<3.2.0',
        'argo-workflows-sdk'
    ],
    entry_points={
        'console_scripts': [
            'prepuller = jupyterhubutils.scanrepo:prepullerstandalone',
            'reaper = jupyterhubutils.scanrepo:reaperstandalone',
            'scanrepo = jupyterhubutils.scanrepo:standalone'
        ],
        'jupyterhub.authenticators': [
            ('LSSTGitHubOAuthenticator = ' +
             'jupyterhubutils.authenticator.LSSTGitHubOAuthenticator'),
            ('LSSTCILogonOAuthenticator = ' +
             'jupyterhubutils.authenticator.LSSTCILogonOAuthenticator'),
            ('LSSTJWTAuthenticator = ' +
             'jupyterhubutils.authenticator.LSSTJWTAuthenticator'),
        ],
        'jupyterhub.spawners': [
            'LSSTSpawner = jupyterhubutils.spawners.LSSTSpawner',
        ],
    }
)
