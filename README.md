![Build](https://github.com/Slimmer-AI/mpire/workflows/Python%20package/badge.svg?branch=master)

MPIRE (MultiProcessing: Insanely Rapid Execution)
-------------------------------------------------

A Python package for multiprocessing, but faster than multiprocessing. It combines the convenient map like functions
of multiprocessing.Pool with the benefits of using copy-on-write shared objects of multiprocessing.Process.

Features
--------

- Multiprocessing with ``map``/``map_unordered``/``imap``/``imap_unordered`` functions
- Easy use of copy-on-write shared objects with a pool of workers
- Each worker can have its own state (e.g., to load a memory-intensive model only once for each worker without the
  need of sending it through a queue)
- Automatic task chunking for all available map functions to speed up processing of small task queues (including numpy
  arrays)
- Adjustable maximum number of active tasks to avoid memory problems
- Automatic restarting of workers after a specified number of tasks to reduce memory footprint
- Nested pool of workers are allowed when setting the ``daemon`` option
- Child processes can be pinned to specific or a range of CPUs on Linux systems
- Multiple process start methods available, including: ``fork`` (default), ``forkserver``, ``spawn``, and ``threading``
- Progress bar support using [tqdm](https://tqdm.github.io/)
- Progress dashboard support
- (Optional) [dill](https://pypi.org/project/dill/) support


Installation
------------

Through pip (PyPi):

```
pip install mpire
```

From source:

```
python setup.py install
```

Documentation
-------------

If you want to build the documentation, please install the documentation dependencies by executing:

```
pip install mpire[docs]
```

or 

```
pip install .[docs]
```

Documentation can then be build by executing:

```
python setup.py build_docs
```

Documentation can also be build from the ``docs`` folder directly. In that case MPIRE should be installed and available
in your current working environment. Then execute:

```
make html
```

in the ``docs`` folder.
