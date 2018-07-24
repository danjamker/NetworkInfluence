[![DOI](https://zenodo.org/badge/128926599.svg)](https://zenodo.org/badge/latestdoi/128926599)

# Infleunce Probabilities

## Synopsis

This repo includes all the code needed to replicat the model proposed in XX and XX. 

This is a distributed applications which runs ontop of a Sparl(Yarn) cluster. 

The aim of the application is to model the process at which nodes come to addopt an innovation from what their nabours have addopted. This models this through learning a global threshold which when breached means a user will adopt an innovation. 

## Requirments

- HBase
- Spark

## Run code

This application is broken down into a number of diffrent Spark Jobs, all of which are chained. First the network is loaded into HBase, metrics are then computed from the diffusions and then infleunce values computed. 

The steps are all lists in Runner.java, it is in this file where you specify the location of the HBase instance along with defining the Spark Context.

- table_name: name of the hnase table to use
- network_file: location of directory which contains the network deffinition
- input_file: the location of the directory which contains all the diffusions
- output_file: the location to save the output of the application

### Input

The following section will outline the two files which are needed to set up and run a diffusion

## Network

This should be a csv file in the format of:
<sourse_node, target_node, weight, Edge_creating_time>

## Diffusions 

Each diffusion should have a seperate file all within the same folder. With the name of the file being the name of the diffusion.

Each file should be in the format of:

<diffusion_id, node_id, time_stamp>


### Processing output 

The output of the model is a CSV file which contains all the infleunce probabilities for each node, the innovation the influence is assosiated too and whether the node used the innovation or not. 

<id , nodeID, infleunce_value, innovation, performent, time, global max>

These include values can then be used in ROC analysis to learn a global threshold at which the innovation is addopted by a node given a et amount of expoure. 

The note book to compute these is not included in this repo. 

### Contributers

Daniel Kershaw - Lancaster Universtiy

### Refrences

D. Kershaw, M. Rowe, A. Noulas, et al., \Birds of a feather talk together: user in
uence on
language adoption", in Hawaii International Conference on System Sciences, Hawaii International
Conference on System Sciences, Jan. 2017, isbn: 9780998133102. doi: 10.24251/HICSS.2017.225.
[Online]. Available: http://hdl.handle.net/10125/41379

Goyal, Amit, Francesco Bonchi, and Laks VS Lakshmanan. "Learning influence probabilities in social networks." Proceedings of the third ACM international conference on Web search and data mining. ACM, 2010.