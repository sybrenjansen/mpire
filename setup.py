from setuptools import setup


def read_description():
    with open('README.md') as file:
        return file.read()

setup(
    name="mpire",
    version='0.6.1',
    author="Sybren Jansen (Target Holding)",
    author_email="sybren.jansen@target-holding.nl",
    description="A Python package for multiprocessing, but faster than multiprocessing",
    long_description=read_description(),
    packages=["mpire"],
    install_requires=["tqdm"],
    test_suite='tests'
)
