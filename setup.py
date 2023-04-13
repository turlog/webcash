from setuptools import setup, find_packages

setup(
    name='webcash',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    version='0.1',
    python_requires='>=3.8',
    install_requires=[
        'tabulate',
        'click',
        'furl',
        'ruamel.yaml',
        'piecash',
        'lxml',
    ],
    entry_points={
        'console_scripts': [
            'statements=webcash.utils.statements:cli'
        ]
    }
)