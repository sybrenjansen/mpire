from setuptools import find_packages, setup


def read_description():
    with open('README.rst') as file:
        return file.read()


if __name__ == '__main__':
    setup(
        name="mpire",
        version='1.2.1',
        author="Slimmer AI",
        description="A Python package for easy multiprocessing, but faster than multiprocessing",
        long_description=read_description(),
        url='https://github.com/Slimmer-AI/mpire',
        license='MIT',
        packages=find_packages(),
        scripts=['bin/mpire-dashboard'],
        install_requires=["numpy", "tqdm"],
        include_package_data=True,
        extras_require={
            'dashboard': ['flask'],
            'docs': ['sphinx==3.2.1',
                     'sphinx-rtd-theme==0.5.0',
                     'sphinx-autodoc-typehints==1.11.0',
                     'sphinxcontrib-images==0.9.2',
                     'sphinx-versions==1.0.1'],
            'dill': ['multiprocess'],
        },
        test_suite='tests',
        classifiers=[
            # Development status
            'Development Status :: 5 - Production/Stable',

            # Supported Python versions
            'Programming Language :: Python :: 3',

            # License
            'License :: OSI Approved :: MIT License',

            # Topic
            'Topic :: Software Development :: Libraries :: Python Modules'
        ]
    )
