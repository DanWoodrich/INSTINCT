# INSTINCT #

Welcome to the open alpha version of INSTINCT! 

INSTINCT is a system for pipeline development for workflows related to detection of signals in acoustic data. INSTINCT pipelines consist of modular components, and can be extended beyond detection to include annotation
and analysis workflows. 

## Usage ##

INSTINCT is run within the command line. Currently, it is only setup for Windows, Linux support is planned. 

Before running INSTINCT, you'll need to have an idea of:
* What workflow you'd like to implement
* How you want to structure your pipeline and project
* The location and metadata of your sound files 
* The location and metadata of any labels you will need

To configure needed dependencies, follow /Instructions - Config.txt . Note that INSTINCT has no base functionality except when used in conjunction with a user submodule (ie, DanWoodrich/instinct_afsc_mml_acoustics) 

There currently is not a manual or training videos detail on how to get started. For those that are eager to use the system, please reach out to me at daniel.woodrich@noaa.gov so I can help get you started. The examples in 
the afsc_mml_acoustics folder in lib/user/contrib are a good example of how our user project is structured. 

```bash
instinct pull_contrib [contrib location] 
```

will populate your user folder (which INSTINCT reads) with contrib files that can help for initially standing up your project. 

Collaboration is best implemented with a seperate repository that the present repository will link to as a submodule. 
If you are interested in linking a submodule in contrib from your user repository, please let me know. 

The typical command is as follows: 
```bash
INSTINCT [pipeline or job name] [project name] [parameter file name]
```

Now also supports
INSTINCT [pipeline or job name] [gcs uri to .nt file]

## About ##

INSTINCT is a command-line application, built to leverage the pipeline framework of [Luigi](https://github.com/spotify/luigi) to make acoustic signal detection workflows
language agnostic, modular, repeatable, and extensible. INSTINCT is currently only in use by the Acoustics group at the Alaska Fisheries Science Center Marine Mammal Lab, but is built for collaboration. 
The core of INSTINCT is built in python, however, INSTINCT is not at this time a standalone module: it refers to locations within the lib/user directory, where it expects to read files specific to your project. 
Collaboration is built in by supporting links to external repositories as submodules in /lib/etc/user/contrib.  

## Philosophy ##

INSTINCT was designed to be a low dependency, extremely customizable, and language agnostic "one stop shop" for acoustic detector design, evaluation, and deployment. To that end, it has performed very well for us. However, to get started as a new INSTINCT user requires a good understanding of it's design philosophy. 

Some of these themes include: 

* The pipeline hierarchy: jobs -> pipelines -> processes -> methods -> parameters
	* Jobs: are used for execution. They: 
		* Return one or more output files. 
		* Can be composed of one or more pipelines.
		* Can deploy a single pipeline as a job
	* Pipelines: data processing routine with a single output. They: 
		* Can be designed compositionally or hard coded by the user. 
		* Can be composed of processes and pipelines. 
		* Always ends in a process. 
	* Processes: data processing step which is defined by standard data processing step, inputs and outputs.   
	* Methods: a particular implementation of a process. They are:
		* Called by subprocess 
		* Implemented in language of choice
	* Parameters: further control function of methods. 
* The interaction of pipelines and parameters:
	* in order to keep the size of the parameter file small, parameter nesting is used to control which parameters are available to methods during pipeline execution. 
this is navigated by how you organize the parameter file, and the use of the 'params_drop' argument in pipeline definitions 
	* processes default to reading the parameters section of their same name, in the top level of the dictionary. params_drop drops the
dictionary to a nested level, and overrites redundant names in the top level. In this way, different parameters can be used for the same
process/methods within a single job. 

## Further reading ##

[A recent demo on basic features of INSTINCT and the instinct_afsc_mml_acoustics user submodule](https://drive.google.com/drive/folders/1TNhrO5JhrNRcO0zN5lN2ZPLXBP386kSi?usp=sharing)

[More information about the INSTINCT basic det/class algorithm, and some of the project's history](https://repository.library.noaa.gov/view/noaa/27222)
* The original det/class algorithm is an extension of the method presented in Ross & Allen 2014

[Slides](https://docs.google.com/presentation/d/1-bcTugqm44vruxQw3cWvLT0hR5GFPFUK/edit?usp=sharing&ouid=109971476781094853963&rtpof=true&sd=true) and [video](https://www.youtube.com/watch?v=U9wet-1_3yg&list=PLY6u3ZR91o26z_PgCRd762lmJ9FXN4YTv&index=14&t=1195s) from a recent presentation at the NOAA 3rd AI workshop




