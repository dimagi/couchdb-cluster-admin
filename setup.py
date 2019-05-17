from __future__ import absolute_import
from __future__ import unicode_literals
from setuptools import find_packages, setup

# For compatibility with pip-compile,
# all other imports must be at the function level


def get_version(filename):
    import os
    from io import open
    import re
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, encoding="utf-8") as handle:
        content = handle.read()
    return re.search(r'__version__ = "([^"]+)"', content).group(1)


def read_md(filename):
    import os
    from io import open
    path = os.path.join(os.path.dirname(__file__), filename)
    try:
        from pypandoc import convert
        return convert(path, 'rst')
    except ImportError:
        print("warning: pypandoc not found, could not convert Markdown to RST")
        with open(path, encoding='utf-8') as handle:
            return handle.read()


def read_requirements(filename):
    import os
    from io import open
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, encoding="utf-8") as handle:
        content = handle.readlines()
    return [
        line.strip() for line in content if not line.startswith('#')
    ]


setup(
    name='couchdb-cluster-admin',
    version=get_version('couchdb_cluster_admin/__init__.py'),
    description='Utility for managing multi-node couchdb 2.x clusters',
    long_description=read_md('README.md'),
    long_description_content_type="text/markdown",
    maintainer='Dimagi',
    maintainer_email='dev@dimagi.com',
    url='https://github.com/dimagi/couchdb-cluster-admin',
    license='BSD License',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=(
        'argparse>=1.4',
        'gevent',
        'jsonobject>=0.8.0',
        'PyYAML',
        'requests',
        'dimagi-memoized'
    ),
    tests_require=read_requirements('test-requirements.txt'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
