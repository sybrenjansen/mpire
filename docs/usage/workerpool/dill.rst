.. _use_dill:

Dill
====

.. contents:: Contents
    :depth: 2
    :local:

For some functions or tasks it can be useful to not rely on pickle, but on some more powerful serialization backends
like dill_. ``dill`` isn't installed by default. See :ref:`dilldep` for more information on installing the dependencies.

One specific example where ``dill`` shines is when using start method ``spawn`` (the default on Windows) in combination
with iPython or Jupyter notebooks. ``dill`` enables parallelizing more exotic objects like lambdas and functions defined
in iPython and Jupyter notebooks. For all benefits of ``dill``, please refer to the `dill documentation`_.

Once the dependencies have been installed, you can enable it using the ``use_dill`` flag:

.. code-block:: python

    with WorkerPool(n_jobs=4, use_dill=True) as pool:
        ...

.. note::

    When using ``dill`` it can potentially slow down processing. This is the cost of having a more reliable and
    powerful serialization backend.

.. _dill: https://pypi.org/project/dill/
.. _dill documentation: https://github.com/uqfoundation/dill
