# PyMirrorFileSystemsByHash

This app was reinitialized in December 2021. 
A lot of scripts became outdated and a new strategy for the
  mirroring operations was chosen.
Basically the new approach brought some kind of simplification.
Three may be listed below:
1) a node self-reference field (child to parent) was discontinued;;
2) the "logical" primary key became name and parentpath;
3) the approach to finding repeats (based on sha1) was simplified to a db-SELECT. 

The next challenge is to add some sort of GUI to help use the app without the command line.
