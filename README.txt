This project branched off from INSTINCT HPC. instinct_dt version is designed to be easily portable between linux and windows, and does not
containerize methods. Rather, the whole processes can ideally be containerized. 

This version allows for faster development/testing on windows, and will be immediately useful on AFSC workstations. 

The philosophy of atomic methods will still be enforced, rather with containers, by command line calls. Scipts will be passed parameters 
and relative paths, and will save to the task target to prevent side effects. 

Python, R, and future dependencies will not be managed during development, but should be accounted for in any containerization. 
