from setuptools import setup, find_packages

#* run setup command: pip install -e .

setup(
    name = "europy_db_controllers",
    version = "1.0",
    description = "",
    author = "Stephan Raab & Lucas Ullon",
    setup_requirements = ["setuptools_scm"],
    packages = find_packages(),
    include_package_data= True,
    # include packages to be installed as openpyxl etc. need to be tested first
    install_requires = ['Flask <= 2.3.1',
                        'holidays <= 0.44',
                        'openpyxl <= 3.1.2',
                        'psycopg2-binary <= 2.9.9',
                        'SQLAlchemy <= 2.0.29',
                        'SQLAlchemy-Utils <= 0.41.2',
                        'Flask-SQLAlchemy <= 3.1.1',
                        'Flask-Caching <= 2.1.0'],
    classifiers= [
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)