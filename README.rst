MPIRE (MultiProcessing Is Really Easy)
======================================

|Build status| |Docs status|

.. |Build status| image:: https://github.com/Slimmer-AI/mpire/workflows/Build/badge.svg?branch=master
.. |Docs status| image:: https://github.com/Slimmer-AI/mpire/workflows/Docs/badge.svg?branch=master

``MPIRE`` is a Python package for multiprocessing, but faster and more user-friendly than the default multiprocessing
package. It combines the convenient map like functions of ``multiprocessing.Pool`` with the benefits of using
copy-on-write shared objects of ``multiprocessing.Process``.

Full documentation is available at https://slimmer-ai.github.io/mpire/.

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
- Child processes can be pinned to specific or a range of CPUs
- Multiple process start methods available, including: ``fork`` (default), ``forkserver``, ``spawn``, and ``threading``
- Progress bar support using tqdm_
- Progress dashboard support
- (Optional) dill_ support

.. _tqdm: https://tqdm.github.io/
.. _dill: https://pypi.org/project/dill/


Installation
------------

Through pip (PyPi):

.. code-block:: bash

    pip install mpire

From source:

.. code-block:: bash

    python setup.py install


Documentation
-------------

If you want to build the documentation, please install the documentation dependencies by executing:

.. code-block:: bash

    pip install mpire[docs]

or 

.. code-block:: bash

    pip install .[docs]


Documentation can then be build by executing:

.. code-block:: bash

    python setup.py build_docs

Documentation can also be build from the ``docs`` folder directly. In that case ``MPIRE`` should be installed and
available in your current working environment. Then execute:

.. code-block:: bash

    make html

in the ``docs`` folder.
