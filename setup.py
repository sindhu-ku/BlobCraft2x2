from setuptools import setup, find_packages

setup(
    name='BlobCraft2x2',
    version='1.0.0',
    description='Query DUNE 2x2 databases (slow controls, light readout system) and dump into JSON blobs ',
    author='Sindhujha Kumaran',
    author_email='s.kumaran@uci.edu',
    packages = find_packages(),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'SC_query = BlobCraft2x2.SC.SC_query:main',
            'LRS_query = BlobCraft2x2.LRS.LRS_query:main',
            'Mx2_query = BlobCraft2x2.Mx2.Mx2_query:main'
        ],
    },

    install_requires=[
        'sqlalchemy',
        'psycopg2-binary',
        'influxdb',
        'PyYAML',
        'pandas',
        'numpy',
        'python-dateutil'
    ],
    python_requires='>=3.9',
)
