# HyOD
## Introduction

HyOD is a solution for Set-based Order Dependency (OD) discovery.
Given an instance of a relational schema, HyOD efficiently computes the set of all minimal OD.

## Requirements

* Java 17 or later
* Maven 4.0.0 or later

## Usage

### A Quick Example
After building the project with maven, it is already runnable with a simple example.
This example running HyOD takes as input the dataset [WP-20K-7.csv](https://github.com/jgszxlyh/HyOD/blob/HyOD/Data/exp1/WP-20K-7.csv)
with initial sample size 100,selected tuple pairs 10, computes its ODs,
and outputs relevant information including the number of ODs,memory cost,sample size after compute,running time and so on.

You Can use  ```HyOD.jar``` like: ```java -jar HyOD.jar WP-20K-7.csv 100 10```

### Configures

Most parameters and configures related to our experiment are in [HyOD.java](https://github.com/jgszxlyh/HyOD/blob/HyOD/src/main/java/HyOD/HyOD.java).
Some of the most pertinent ones are listed here, and please refer to the code and comments for further detail.

* ```fp```: file path of input dataset
* ```samplesize```: initial sample size, within \[0,\|r\|\]
* ```tuple num```: the number of tuples added to sample by valided OD, at least 1
* ```thread num```: the number of thread in threadpool,only [HyODM.java](https://github.com/jgszxlyh/HyOD/blob/HyOD/src/main/java/HyOD/HyODM.java) use it

## Comparative Experiments

HyOD are compared to other two Set-based Order Dependencyr methods, [FastOD](https://doi.org/10.1109/ICDE.2018.00176)
and [DistOD](https://doi.org/10.1007/s00778-021-00683-4).
The source code of DistOD can be found [here](https://github.com/CodeLionX/distod).
And we use FastOD.jar in DistOD.

## License

HyOD is released under the [Apache 2.0 license](https://github.com/RangerShaw/FastADC/blob/master/LICENSE).
