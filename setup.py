from setuptools import setup, find_packages

exec(open("func_preprocessing/_version.py").read())

setup(
    name="func_preprocessing",
    version=__version__,  # noqa: F821
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "func_preprocessing=func_preprocessing.cli:main",
        ]
    },
)
