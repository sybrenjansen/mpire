from setuptools import setup


def read_description():
    with open('README.md') as file:
        return file.read()


if __name__ == '__main__':
    setup(
        name="mpire",
        version='0.9.0',
        author="Sybren Jansen (Target Holding)",
        author_email="sybren.jansen@target-holding.nl",
        description="A Python package for multiprocessing, but faster than multiprocessing",
        long_description=read_description(),
        packages=["mpire"],
        install_requires=["numpy", "tqdm"],
        extras_require={
            'docs': ['sphinx', 'sphinx-rtd-theme'],
            'dill': ['multiprocess']
        },
        test_suite='tests'
    )
