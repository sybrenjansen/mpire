from setuptools import setup

with open('README.md') as file:
    long_description = file.read()

setup(
    name="mpire",
    version='0.6.0',
    author="Sybren Jansen (Target Holding)",
    author_email="sybren.jansen@target-holding.nl",
    description="A Python package for multiprocessing, but faster than multiprocessing",
    long_description=long_description,
    packages=["mpire"],
    install_requires=["tqdm"],
    test_suite='tests'
)
