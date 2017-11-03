MPIRE (MultiProcessing: Insanely Rapid Execution)
-------------------------------------------------

A Python package for multiprocessing, but faster than multiprocessing. It combines the convenient map like functions
of multiprocessing.Pool with the benefits of using copy-on-write shared objects of multiprocessing.Process.

Features
--------

- Multiprocessing with map/map_unordered/imap/imap_unordered functions
- Easy use of copy-on-write shared objects with a pool of workers
- Automatic task chunking for all available map functions to speed up processing of small task queues
- Functions are only pickled once for each worker
- Adjustable maximum number of active tasks to avoid memory problems
- Automatic restarting of workers after a specified number of tasks to reduce memory footprint
- Nested pool of workers are allowed when setting the ``daemon`` option
- Child processes can be pinned to specific CPUs on Linux systems
- Progress bar support

Installation
------------

Through pip (localshop):

```
pip install mpire -i http://localshop.priv.tgho.nl/repo/tgho --trusted-host localshop.priv.tgho.nl
```

From source:

```
python setup.py install
```

Documentation
-------------

If you want to build the documentation, please install the following Python packages:

 * Sphinx
 * sphinx-rtd-theme

Documentation can be build by executing:

```
python setup.py build_docs
```

Documentation can also be build from the ``docs`` folder directly. In that case MPIRE should be installed and available
in your current working environment. Then execute:

```
make html
```

in the ``docs`` folder. For a pre-build version, please refer to [RTFM](https://rtfm.tgho.nl/mpire).
