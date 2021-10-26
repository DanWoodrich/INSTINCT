INSTINCT pipelines are can be defined here in nestedtext https://nestedtext.org/en/stable/basic_use.html, 
or in ../definitions.py as a python dictionary. 

The pipelines in this folder have pluggable components, which is why they are defined here in this manner. Pipelines can also be 
hardcoded.

A pipeline can be executed as a job. A job will take precedence over a pipeline of the same name.   

There are several features that you can implement outside of the basic function of connecting pipelines and processes. These are: 

params_drop: This allows you to 'drop' down to a specified sublevel (including up to the top level of parameters, 'job') in the 
parameters file. This enables you to keep the parameters file small and minimally redundant. 

pipe_link: lets you connect to a different pipeline. Useful with larger pipelines and when building complexity onto existing pipelines
 
loop_on: is used for parameter sweeps. Currently, only one parameter sweep is allowed per level (cannot sweep on two parameters simultaneously). 
This is generally used (by Daniel Woodrich as of 10/26/21) for spliting up pipelines based on data source. Note that swept parameter is not automatically stored, 
this will need to be implemented in your process or method if you plan on doing a downstream analysis. 