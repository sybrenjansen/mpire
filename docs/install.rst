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

.. _dashboarddep:

Dashboard
~~~~~~~~~

Optionally, you can install the dependencies for the MPIRE dashboard, which depends on Flask_. The dashboard allows you
to see progress information from a browser. This is convenient when running scripts in a notebook or screen, or want to
share the progress information with others. Install the appropriate dependencies to enable this:

.. code-block:: bash

    pip install mpire[dashboard]

.. _Flask: https://flask.palletsprojects.com/en/1.1.x/
