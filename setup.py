from setuptools import setup, find_packages

setup(
    name="func_preprocessing",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "func_preprocessing=func_preprocessing.cli:main",
        ]
    },
)