from setuptools import find_packages, setup


def read_description():
    with open('README.rst') as file:
        return file.read()


if __name__ == '__main__':
    # For Python < 3.7 we need dataclasses. On Windows, we need pywin32 for CPU pinning
    setup(
        name='mpire',
        version='2.4.0',
        author='Slimmer AI',
        description='A Python package for easy multiprocessing, but faster than multiprocessing',
        long_description=read_description(),
        url='https://github.com/Slimmer-AI/mpire',
        license='MIT',
        packages=find_packages(),
        scripts=['bin/mpire-dashboard'],
        install_requires=['dataclasses; python_version<"3.7"',
                          'pywin32==225; platform_system=="Windows"',
                          'tqdm'],
        include_package_data=True,
        extras_require={
            'dashboard': ['flask'],
            'dill': ['multiprocess'],
            'docs': ['docutils==0.17.1',
                     'sphinx==3.2.1',
                     'sphinx-rtd-theme==0.5.0',
                     'sphinx-autodoc-typehints==1.11.0',
                     'sphinxcontrib-images==0.9.2',
                     'sphinx-versions==1.0.1'],
            'testing': ['dataclasses; python_version<"3.7"',
                        'multiprocess', 
                        'numpy',
                        'pywin32==225; platform_system=="Windows"']
        },
        test_suite='tests',
        tests_require=['multiprocess', 'numpy'],
        classifiers=[
            # Development status
            'Development Status :: 5 - Production/Stable',

            # Supported Python versions
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',

            # License
            'License :: OSI Approved :: MIT License',

            # Topic
            'Topic :: Software Development',
            'Topic :: Software Development :: Libraries',
            'Topic :: Software Development :: Libraries :: Python Modules'
        ]
    )
