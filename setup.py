from setuptools import setup, find_packages

exec(open("func_preprocess/_version.py").read())

setup(
    name="func_preprocess",
    version=__version__,  # noqa: F821
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "func_preprocess=func_preprocess.cli:main",
        ]
    },
    install_requires=["setuptools>=65.5.1"],
)
