Installation
============

:ref:`MPIRE <secret>` builds are currently distributed through Target Holding's (internal) localshop_.

.. _localshop: https://localshop.tgho.nl/repo/tgho

MPIRE can be installed through pip:

.. code-block:: bash

    pip install mpire

.. note::

    MPIRE is only available for Python 3.x.

Dependencies
------------

- Python 3.x

Python packages (installed automatically when installing MPIRE):

- numpy
- tqdm_

.. _tqdm: https://pypi.python.org/pypi/tqdm

.. _dilldep:

DILL
~~~~

Optionally, you can choose to use dill_ instead of regular pickle which allows you to serialize some more exotic types,
like lambdas. Install the appropriate dependencies to enable this:

.. code-block:: bash

    pip install mpire[dill]

.. _dill: https://pypi.org/project/dill/
