Parameter files are organized into two top levels: Global, and Job. 

Global contains necessary locations and definitions for the run. Wrapper indicates whether the job output can be skipped (used in a scenario where
populating the cache is the goal over interacting with an output). 

Job contains parameters for a job execution. They are organized in hierarchies: the parameters in the second level of job will be 
available to the running processes, so in order to make parameters available at certain times in the pipeline, place a 'params_drop'
in your pipeline definition (see /lib/users/contrib/afsc_mml_acoustics/pipelines for examples). 

The parameters file and pipeline design are closely associated. As such, you should make sure that specialized pipelines that don't 
fit typical patterns are given their own parameters file. 

Pipelines assign parameters to processes using character matching from this file to the process names. If you want a parameters section
to correspond to a common prefix instead of a certain process name, you can use the * after the common prefix. (See TrainModel_RF* in 
the parameter file and compare to the processes with this prefix in /lib/users/contrib/afsc_mml_acoustics/processes.py)