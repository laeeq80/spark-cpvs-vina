# Docking CPP Implementation #

The folder contains docking implementation in c++.

## Why CPP implementation

While the rest of the project is in scala which uses OECHEM java libraries, the OECHEM java library has memory leakage issue due to which we had to use OECHEM C++ library.

## Implementation

We build a simple c++ docking and then pipped the c++ executable file to our scala application using spark's pipe method. 

## Practicalities

Please set DOCKING_CPP environment variable in the PATH to the dockingstd binary available at spark-vs/docking-cpp/bin/
