MPIRE (MultiProcessing: Insanely Rapid Execution)
-------------------------------------------------

A Python package for multiprocessing, but faster than multiprocessing. It combines the convenient map like functions
of multiprocessing.Pool with the benefits of using copy-on-write shared objects of multiprocessing.Process.

Features
--------

- The default map/imap/imap_unordered functions, including map_unordered
- Automatic task chunking for all available map functions to speed up processing
- Functions are only pickled once when workers are started
- Adjustable maximum number of active tasks
- Support for shared objects (which are only passed once to each worker and are copy-on-write)

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

in the ``docs`` folder. For a pre-build version, please refer to
[CI/Jenkins](http://ci:8080/job/Attic/job/mpire/Documentation/).
