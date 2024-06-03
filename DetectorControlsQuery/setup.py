from setuptools import setup, find_packages

setup(
    name='DetectorControlsQuery',
    version='1.0.0',
    description='Query tool for DUNE 2x2 detector controls',
    author='Sindhujha Kumaran',
    author_email='s.kumaran@uci.edu',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'query = SC_query:main',
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
