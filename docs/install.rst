Installation
============

:ref:`MPIRE <secret>` builds are distributed through PyPi_.

.. _PyPi: https://pypi.org/

MPIRE can be installed through pip:

.. code-block:: bash

    pip install mpire

.. note::

    MPIRE is only available for Python >= 3.6.

Dependencies
------------

- Python >= 3.6

Python packages (installed automatically when installing MPIRE):

- tqdm
- pywin32 (Windows only)

.. _dilldep:

Dill
~~~~

For some functions or tasks it can be useful to not rely on pickle, but on some more powerful serialization backend,
like dill_. ``dill`` isn't installed by default as it has a BSD license, while MPIRE has an MIT license. If you want
to use it, the license of MPIRE will change to a BSD license as well, as required by the original BSD license. See the
`BSD license of multiprocess`_ for more information.

You can enable ``dill`` by executing:

.. code-block:: bash

    pip install mpire[dill]

This will install multiprocess_, which uses ``dill`` under the hood. You can enable the use of ``dill`` by setting
``use_dill=True`` in the :obj:`mpire.WorkerPool` constructor.

.. _dill: https://pypi.org/project/dill/
.. _multiprocess: https://github.com/uqfoundation/multiprocess
.. _BSD license of multiprocess: https://github.com/uqfoundation/multiprocess/blob/master/LICENSE

.. _dashboarddep:

Dashboard
~~~~~~~~~

Optionally, you can install the dependencies for the MPIRE dashboard, which depends on Flask_. Similarly as with
``dill``, ``Flask`` has a BSD-license. Installing these dependencies will change the license of MPIRE to BSD as well.
See the `BSD license of Flask`_ for more information.

The dashboard allows you to see progress information from a browser. This is convenient when running scripts in a
notebook or screen, or want to share the progress information with others. Install the appropriate dependencies to
enable this:

.. code-block:: bash

    pip install mpire[dashboard]

.. _Flask: https://flask.palletsprojects.com/en/1.1.x/
.. _BSD license of Flask: https://github.com/pallets/flask/blob/main/LICENSE.rst
