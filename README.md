g5k_bench_flops
===============

automate flops benchmarking of grid5000 clusters

installation:
-------------

- dependencies: execo http://execo.gforge.inria.fr/doc/

- clone the repo

- add to the root dir the following files, I prefer not putting them on the repo because these are distribution packages and because of possible licensing issues (i did not check):

  - atlas3.10.0.tar.bz2 (download from http://sourceforge.net/projects/math-atlas/files/)

  - hpl-2.1.tar.gz (download from http://www.netlib.org/benchmark/hpl/)

  - openmpi-1.6.3.tar.bz2 (download from http://www.open-mpi.org/software/ompi/v1.6/)

running
-------

- adapt packages dict if needed (if using different versions of atlas, hpl, openmpi) in common.py

- run g5k_prepare_bench_flops to precompile each packages on all clusters (the precompiled packages will be downloaded in directory preparation/)

- run g5k_bench_flops

- wait...
