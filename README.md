# CS441 - Homework 2 - Aditya Sawant
#
##### Description: Create a map/reduce program for parallel processing of the [publicly available DBLP dataset](https://dblp.uni-trier.de) that contains entries for various publications at many different venues (e.g., conferences and journals).

## Overview
As part of this assignment, a total of 4 map-reduce jobs have been created.

- Job **1** calculates the authorship score for each author.

- Job **2** calculates the number of publications in a particular year.

- Job **3** calculates the maximum, median and average number of co-authors for each author.

- Job **4** calculates the number of publication types(or venues) in each year.

## Instructions 
**SBT** is needed to build a jar for hadoop.   
Once cloned from the repository, open the terminal or command prompt, cd to the directory where the project is cloned and then run the following command:  
```sbt clean compile assembly```    

Run the following command in the directory where the jar has been generated/stored:
``` hadoop jar  Aditya_Sawant_MapReduce_HW_2-assembly-0.1.jar <job number> <hadoop input directory> <hadoop output directory>```

**P.S:** There must be an xml file present in the input directory which follows the dblp.dtd specification.
The output for the given job will be created and stored in the directory mentioned in the args.

The output for each job is in a csv format.

**For AWS EMR** visit the following channel: 
[AWS EMR Execution Steps](https://youtu.be/iJdvD5S2hwE) 
