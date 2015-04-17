# Course-Project-Database-recovery
This project implements the ARIES algorithm for database recovery. The framework is built by the instructor and our work is in StudentComponent/LogMgr.cpp.
The StorageEngine simulates a database by processing instructions read from test cases under testcases/ and dispatching them to LogMgr which is responsible for the details of the execution. 
The LogMgr consists of the implementations of two parts: normal operation and recovery after crash of the database. The normal operation includes function abort, checkpoint, pageflushed, write, commit, all of which follows the Write-ahead Logging (WAL) paradigm. The recovery is done by calling the recovery function which follows the analyse-redo-undo pattern.
