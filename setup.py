from setuptools import find_packages, setup


def read_description():
    with open('README.md') as file:
        return file.read()


if __name__ == '__main__':
    setup(
        name="mpire",
        version='1.1.2',
        author="Sybren Jansen (Slimmer AI)",
        author_email="sybren.jansen@slimmer.ai",
        description="A Python package for easy multiprocessing, but faster than multiprocessing",
        long_description=read_description(),
        packages=find_packages(),
        scripts=['bin/mpire-dashboard'],
        install_requires=["numpy", "tqdm"],
        include_package_data=True,
        extras_require={
            'dashboard': ['flask'],
            'docs': ['sphinx==1.8.5',
                     'sphinx-rtd-theme==0.4.3',
                     'sphinx-autodoc-typehints==1.6.0', 
                     'sphinxcontrib-images==0.8.0'],
            'dill': ['multiprocess'],
        },
        test_suite='tests'
    )
