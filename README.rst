=====
MPIRE
=====

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

.. code::

    pip install mpire -i http://localshop.priv.tgho.nl/repo/tgho --trusted-host localshop.priv.tgho.nl

From source:

.. code::

    python setup.py install

Usage
-----

.. code:: python

    import math
    from mpire import WorkerPool
    from multiprocessing import Array
    
    def square(x):
        return x * x
        
    def square_with_index(shared_objects, idx, x):
        result_container = shared_objects
        result_container[idx] = x * x
        
    def square_root_modulo(shared_objects, idx, x):
        square_container, root_container = shared_objects
        square_container[idx] = x * x
        root_container[idx] = math.sqrt(x)
        return x % 2
    
    def example():
        with WorkerPool(n_jobs=4) as pool:
            # We need to specify the length of the iterable such that the chunker can automatically determine the chunk 
            # size (not needed when the container implements __len__).
            results = pool.map(square, ((x,) for x in range(1000000)), iterable_len=1000000, max_tasks_active=None,
                               worker_lifespan=None)
            
            # Use a shared array to store the results. Note that we need to use map here (not imap) as map ensures the 
            # jobs are completed after the statement. If we did not change the shared objects and the function to 
            # execute, there would be no point in restarting the workers. But, as we do both, we do have to restart 
            # them.
            result_container = Array('f', 1000000, lock=False)
            pool.set_shared_objects(result_container, has_return_value_with_shared_objects=False)
            pool.map_unordered(square_with_index, ((idx, x) for idx, x in enumerate(range(1000000))), 
                               iterable_len=1000000, restart_workers=True)
            
            # Both square and root the value, and return modulo 2
            square_container = Array('f', 1000000, lock=False)
            root_container = Array('f', 1000000, lock=False)
            pool.set_shared_objects([square_container, root_container], has_return_value_with_shared_objects=True)
            modulo_results = pool.map(square_root_modulo, [(idx, x) for idx, x in enumerate(range(1000000))], 
                                      max_tasks_active=2, restart_workers=True, worker_lifespan=1)
    
    if __name__ == '__main__':
        example()
