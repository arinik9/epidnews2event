# epidnews2event
Epidemiological event extraction from news

* Nejat Arinik [nejat.arinik@inrae.fr](mailto:nejat.arinik@inrae.fr)
* Roberto Interdonato [roberto.interdonato@cirad.fr](mailto:roberto.interdonato@cirad.fr)
* Mathieu Roche [mathieu.roche@cirad.fr](mailto:mathieu.roche@cirad.fr)
* Maguelonne Teisseire [maguelonne.teisseire@inrae.fr](mailto:maguelonne.teisseire@inrae.fr)


## Description

This set of `Python` scripts is designed for extracting epidemiological events from news in the context of Epidemic Intelligence. In our work, we define an epidemiological event as the detection of the virus at a specific date and time and in a specific location. The code is mainly designed to handle three different Epidemic Intelligence systems: [PADI-web](https://padi-web.cirad.fr), [ProMED](https://promedmail.org/) and [EMPRES-i](https://empres-i.apps.fao.org).

Moreover, in this work, the spatial and temporal scales are two important parameters. It is also worth emphasizing that each spatial entity is geocoded with [GeoNames](https://www.geonames.org/).


## Data

For reproducibility purpose, we provide some samples from the data collected by PADI-web, ProMED and EMPRES-i in the `in` folder. Hence, it is possible to run the source code with these samples. To get the complete data, please email me. This complete dataset consists of the Avian Influenza events occurred between 2019 and 2021.


## Organization

* Folder `in`:

  * Folder `empres-i`: Raw unnormalized event data from Empres-i.
  * Folder `padiweb`: Raw event data from PADI-web.
  * Folder `promed`: Raw unnormalized event data from ProMED.

* Folder `out`: contains the files produced by our scripts

* Folder `src`: 

  * Folder `event`: this folder contains event-related classes, such as Diseae, Location, Temporality and Host.
  * Folder `event_clustering`: the folder contains the implementations for event clustering.
  * Folder `event_fusion`: he folder contains the implementations for event fusion.
  * Folder `event_retrieval`: this folder is specific for PADI-web and it contains the scripts performing the event retrieval task, once the preprocesing step has been performed.
  * Folder `geocoding`: this folder contains the scripts performing the geocoding task. This task consists in assigning geographic coordinates to spatial entities.
  * Folder `media_sources`: this folder contains the scripts performing the processing of media source information.
  * Folder `preprocessing`: this folder contains the scripts performing the preprocessing step. It essentially corresponding to normalizing event related entities. Regarding ProMED and Empres-i, this step amounts to extract normalized events.


## Installation

* Install Python (tested with Python 3.8.12)

* Install Python dependencies using the following command:

  ```
  pip install -r requirements.txt
  ```
* Download this project from this repository: https://github.com/arinik9/epidnews2event

* We have already put a sample dataset in the `in` folder. 
  


## How to run ?

* Go to the folder `src`.

* Run the file `main.py`.


