import platform
import sys
from setuptools import find_packages, setup

# For Python < 3.7 we need dataclasses. On Windows, we need pywin32 for CPU pinning
additional_dependencies = []
if sys.version_info[0] == 3 and sys.version_info[1] < 7:
    additional_dependencies.append("dataclasses")
if platform.system() == "Windows":
    additional_dependencies.append("pywin32==225")


def read_description():
    with open("README.rst") as file:
        return file.read()


if __name__ == "__main__":
    setup(
        name="mpire",
        version="2.3.0",
        author="Slimmer AI",
        description="A Python package for easy multiprocessing, but faster than multiprocessing",
        long_description=read_description(),
        url="https://github.com/Slimmer-AI/mpire",
        license="MIT",
        packages=find_packages(),
        scripts=["bin/mpire-dashboard"],
        install_requires=["tqdm"] + additional_dependencies,
        include_package_data=True,
        extras_require={
            "dashboard": ["flask"],
            "dill": ["multiprocess"],
            "docs": ["sphinx==3.2.1",
                     "sphinx-rtd-theme==0.5.0",
                     "sphinx-autodoc-typehints==1.11.0",
                     "sphinxcontrib-images==0.9.2",
                     "sphinx-versions==1.0.1"],
            "testing": ["multiprocess", "numpy"] + additional_dependencies
        },
        test_suite="tests",
        tests_require=["multiprocess", "numpy"],
        classifiers=[
            # Development status
            "Development Status :: 5 - Production/Stable",

            # Supported Python versions
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",

            # License
            "License :: OSI Approved :: MIT License",

            # Topic
            "Topic :: Software Development",
            "Topic :: Software Development :: Libraries",
            "Topic :: Software Development :: Libraries :: Python Modules"
        ]
    )
