from setuptools import setup, find_packages

# Get the long description from the relevant file
long_description = "This package implements document level deduplication with MinHashLSH and a variant, LSHBloom."

# Get the code version
__version__ = "1.0.0"

setup(
    name='tpc_dedup',
    version=__version__,
    description='Document Deduplication Algorithms for processing very large text datasets',
    long_description=long_description,
    url='https://github.com/TPC-AI/data-general-text-code-web/',
    project_urls={
        'Source': 'https://github.com/TPC-AI/data-general-text-code-web/',
    },
    author='Arham Khan',
    author_email='arham@uchicago.edu',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='database datamining deduplication document text natural language parsing',
    packages=find_packages(include=['deduplication*']),
    install_requires=[
        'numpy>=1.11',
        'scipy>=1.0.0',
        'redis>=2.10.0',
        'datasketch @ git+https://github.com/123epsilon/datasketch.git@060a32b4b4a2272d77480dd633a1bf770678ba49',
        'pybloomfiltermmap3==0.5.7',
        'tqdm>=4.60.0',
    ]
)