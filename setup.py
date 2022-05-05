from setuptools import find_namespace_packages, setup


setup(
    author="vmttn",
    name="data-inclusion-scripts",
    url="https://github.com/betagouv/data-inclusion-scripts",
    version="0.0.1",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "click==8.0.3",
        "pandas==1.4.2",
        "great-expectations==0.15.1",
        "requests==2.27.1",
    ],
    extra_requires={"test": ["pytest==7.1.1"]},
    entry_points={"console_scripts": ["data-inclusion=data_inclusion.cli.cli:cli"]},
)
