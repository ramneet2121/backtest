from setuptools import setup

setup(
    name='python_backtest_utils',
    version='1.0.0',
    author='Ramneet Saini',
    author_email='saini.ramneet21@gmail.com',
    description='A custom Python library for backtest',
    packages=['numpy', 'pandas', 'datetime', 'psycopg2'],
    install_requires=[
        'numpy',
        'pandas',
        'datetime',
        'psycopg2'
    ]
)
