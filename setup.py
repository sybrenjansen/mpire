from setuptools import find_packages, setup


def read_description():
    with open('README.rst') as file:
        return file.read()


if __name__ == '__main__':
    setup(
        name='mpire',
        version='2.10.1',
        author='Sybren Jansen',
        description='A Python package for easy multiprocessing, but faster than multiprocessing',
        long_description=read_description(),
        url='https://github.com/sybrenjansen/mpire',
        license='MIT',
        packages=find_packages(exclude=['*tests*']),
        scripts=['bin/mpire-dashboard'],
        install_requires=['importlib_resources; python_version<"3.9"',
                          'pywin32>=301; platform_system=="Windows"',
                          'pygments>=2.0',
                          'tqdm>=4.27'],
        include_package_data=True,
        extras_require={
            'dashboard': ['flask'],
            'dill': ['multiprocess; python_version<"3.11"',
                     'multiprocess>=0.70.15; python_version>="3.11"'],
            'docs': ['docutils==0.17.1',
                     'sphinx==3.2.1',
                     'sphinx-rtd-theme==0.5.0',
                     'sphinx-autodoc-typehints==1.11.0',
                     'sphinxcontrib-images==0.9.2',
                     'sphinx-versions==1.0.1'],
            'testing': ['ipywidgets',
                        'multiprocess; python_version<"3.11"',
                        'multiprocess>=0.70.15; python_version>="3.11"',
                        'numpy',
                        'pywin32>=301; platform_system=="Windows"',
                        'rich'],
        },
        test_suite='tests',
        tests_require=['multiprocess', 'numpy'],
        classifiers=[
            # Development status
            'Development Status :: 5 - Production/Stable',

            # Supported Python versions
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11',
            'Programming Language :: Python :: 3.12',

            # License
            'License :: OSI Approved :: MIT License',

            # Topic
            'Topic :: Software Development',
            'Topic :: Software Development :: Libraries',
            'Topic :: Software Development :: Libraries :: Python Modules'
        ]
    )
