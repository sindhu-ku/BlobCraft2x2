from setuptools import setup, find_packages

setup(
    name='Query2x2',
    version='1.0.0',
    description='Query tool for DUNE 2x2 databases (slow controls, light readout system)',
    author='Sindhujha Kumaran',
    author_email='s.kumaran@uci.edu',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'SC_query = Query2x2.SC.SC_query:main',
            'LRS_query = Query2x2.LRS.LRS_query:main'
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
