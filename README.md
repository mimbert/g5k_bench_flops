g5k_bench_flops
===============

automate flops benchmarking of grid5000 clusters

installation:
-------------

- dependencies:

  - execo http://execo.gforge.inria.fr/doc/

  - g5k_cluster_engine
    https://github.com/lpouillo/execo-g5k-tools/tree/master/engines/g5k_cluster_engine

    (put g5k_cluster_engine.py on the python search path, or copy
    g5k_cluster_engine.py in g5k_bench_flops directory)

- clone the repo

- add to the root dir the following files. They are not part of this
  repo because these are distribution packages and because of possible
  licensing issues (not checked):

  - atlas3.10.0.tar.bz2 (download from
    http://sourceforge.net/projects/math-atlas/files/)

  - hpl-2.1.tar.gz (download from
    http://www.netlib.org/benchmark/hpl/)

  - openmpi-1.6.3.tar.bz2 (download from
    http://www.open-mpi.org/software/ompi/v1.6/)

running
-------

In brief:

- adapt packages dict if needed (if using different versions of atlas,
  hpl, openmpi) in common.py

- run g5k_prepare_bench_flops to precompile each packages on all
  clusters (the precompiled packages will be downloaded in directory
  preparation/)

- run g5k_bench_flops

- wait...

Details:

- Run g5k_bench_flops and g5k_prepare_bench_flops with --help to see
  list of options

Example:

For example. Let's bench the newly installed cluster paranoia in
Rennes, not yet in production (as of April 3, 2014):

On a grid5000 frontend or node:

   $ ./g5k_prepare_bench_flops paranoia -ML -o "-q testing" -T
    
   $ ./g5k_bench_flops paranoia -ML -o "-q testing" -T


analyzing
---------

When all cases have been run, use the following script to obtain results:

   $  ./parse_bench_results <run_dir>

and fill the Reference API.
