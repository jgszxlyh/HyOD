# HyOD
## Introduction

HyOD is a solution for Set-based Order Dependency (OD) discovery.
Given an instance of a relational schema, HyOD efficiently computes the set of all minimal valid set-based ODs.

## Requirements

* Java 8 or later
* Maven 4.0.0 or later

## Usage

### A Quick Example
In this example, HyOD takes as input the dataset [WP-20K-7.csv](https://github.com/jgszxlyh/HyOD/blob/main/Data/exp1/WP-20K-7.csv) with initial sample size 100, and 10 tuple pairs for each invalid OD. It finds ODs, and outputs relevant information including the number of ODs, memory cost, sample size after computation, running time and so on.

You Can use  ```HyOD.jar``` like: ```java -jar HyOD.jar WP-20K-7.csv 100 10```

### Configures

Most parameters and configures related to our experiment are in [HyOD.java](https://github.com/jgszxlyh/HyOD/blob/main/src/main/java/HyOD/HyOD.java).
Some of the most pertinent ones are listed here, and please refer to the code and comments for further detail.

* ```fp```: file path of input dataset
* ```samplesize```: initial sample size, within \[0,\|r\|\]
* ```tuple num```:  the number of violating tuple pairs selected for each invalid OD, at least 1
* ```thread num```: the number of threads in thread pool, only used by [HyODM.java](https://github.com/jgszxlyh/HyOD/blob/main/src/main/java/HyOD/HyODM.java)

## Comparative Experiments

HyOD is compared to the  set-based order dependency method [FastOD](https://link.springer.com/article/10.1007/s00778-018-0510-0).
The source code of FastOD provided by the authors can be found [here](https://git.io/fastodbid).

## License

HyOD is released under the [Apache 2.0 license](https://github.com/jgszxlyh/HyOD/blob/main/LICENSE).
