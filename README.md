# epidnews2event
Epidemiological event extraction from news. This Github repository is dedicated to our work, which has been published in IEEE Access [Arınık'23].


* Nejat Arinik [nejat.arinik@inrae.fr](mailto:nejat.arinik@inrae.fr)
* Roberto Interdonato [roberto.interdonato@cirad.fr](mailto:roberto.interdonato@cirad.fr)
* Mathieu Roche [mathieu.roche@cirad.fr](mailto:mathieu.roche@cirad.fr)
* Maguelonne Teisseire [maguelonne.teisseire@inrae.fr](mailto:maguelonne.teisseire@inrae.fr)


## Description

This set of `Python` scripts is designed for extracting epidemiological events from news in the context of Epidemic Intelligence. In our work, we define an epidemiological event as the detection of the virus at a specific date and time and in a specific location. The code is mainly designed to handle three different Epidemic Intelligence systems: [PADI-web](https://padi-web.cirad.fr), [ProMED](https://promedmail.org/) and [EMPRES-i](https://empres-i.apps.fao.org).

Moreover, in this work, the spatial and temporal scales are two important parameters. It is also worth emphasizing that each spatial entity is geocoded with [GeoNames](https://www.geonames.org/).


## Data

For reproducibility purpose, we provide some samples from the data collected by PADI-web, ProMED and EMPRES-i in the `in` folder. Hence, it is possible to run the source code with these samples. The complete datasets can be found on [Dataverse](https://entrepot.recherche.data.gouv.fr/dataset.xhtml?persistentId=doi:10.57745/Y3XROX) (`raw_event_data.zip`). These complete datasets consist of the Avian Influenza events occurred between 2019 and 2021.



## Organization

* Folder `in`:

  * Folder `empres-i`: Raw unnormalized event data from Empres-i.
  * Folder `padiweb`: Raw event data from PADI-web.
  * Folder `promed`: Raw unnormalized event data from ProMED.

* Folder `in-bahdja`: this folder is supposed to contain input files for evaluating the event extraction task. It is used by the file `src/main_eval_bahdja.py`. The input files can be found on [Dataverse](https://entrepot.recherche.data.gouv.fr/dataset.xhtml?persistentId=doi:10.57745/Y3XROX) (`eval_event_data.zip`) and they are prepared by Bahdja Boudoua.

* Folder `in-geocoding`: this folder is supposed to contain input files for evaluating the geocoding task. It is used by the file `src/main_eval_geocoding.py`. The input files can be found on [Dataverse](https://entrepot.recherche.data.gouv.fr/dataset.xhtml?persistentId=doi:10.57745/Y3XROX) (`eval_event_data.zip`). These files constitute some of the files of [this Dataverse repository](https://dataverse.cirad.fr/dataset.xhtml?persistentId=doi:10.18167/DVN1/KH7YTO): 

* Folder `out`: contains the files produced by our scripts

* Folder `out-bahdja`: contains the files produced by our scripts

* Folder `out-geocoding`: contains the files produced by our scripts

* Folder `src`: 

  * Folder `event`: this folder contains event-related classes, such as Diseae, Location, Temporality and Host.
  * Folder `event_clustering`: the folder contains the implementations for event clustering.
  * Folder `event_fusion`: he folder contains the implementations for event fusion.
  * Folder `event_retrieval`: this folder is specific for PADI-web and it contains the scripts performing the event retrieval task, once the preprocesing step has been performed.
  * Folder `geocoding`: this folder contains the scripts performing the geocoding task. This task consists in assigning geographic coordinates to spatial entities.
  * Folder `media_sources`: this folder contains the scripts performing the processing of media source information.
  * Folder `preprocessing`: this folder contains the scripts performing the preprocessing step. It essentially corresponding to normalizing event related entities. Regarding ProMED and Empres-i, this step amounts to extract normalized events.
  * Folder `evaluate`: this folder contains the scripts performing the evaluation of the geocoding and event extraction tasks.
  * Folder `prepare_input`: this folder contains the scripts creating additional input files from `in-geocoding/articlesweb.csv` into the folder `in-geocoding`.


## Installation

* Install [Maven](https://maven.apache.org/).

* Install Python (tested with Python 3.8.12)

* Install Python dependencies using the following command:

  ```
  pip install -r requirements.txt

  ```

* Run the java dependencies for the package SUTime (check [this website](https://pypi.org/project/sutime/) for more information):

  ```
  mvn dependency:copy-dependencies -DoutputDirectory=./jars -f $(python3 -c 'import importlib; import pathlib; print(pathlib.Path(importlib.util.find_spec("sutime").origin).parent / "pom.xml")')

  ```

* Download this project from this repository: https://github.com/arinik9/epidnews2event

* We have already put sample datasets in the `in/events` folder. For the complete data, you need to retrieve the data from [Dataverse](https://entrepot.recherche.data.gouv.fr/dataset.xhtml?persistentId=doi:10.57745/Y3XROX). Download, unzip the file `raw_event_data.zip` and place the folders under `raw_event_data/in` into the `in` folder.
  
* Update the variable `MAIN_FOLDER` in the file `src/main.py` for your main directory absolute path (e.g. `/home/USER/epidnews2event`).

* Finally, add the main folder absolute path into `PYTHONPATH`:

  ```
  export PYTHONPATH="$PWD"

  ```

* (Optional) GeoNames applies an hourly limit quota for API queries. In order to take advantage the GeoNames' API as much as possible, we are using multiple GeoNames accounts. The description of these accounts are found in the file `src/consts.py` (with the variables `GEONAMES_API_USERNAME<NO>`, where `<NO>` is an integer value). If necessary, you can also increase the number of these accounts. In which case, you need to update the files under `src/geocode`.




## How to run the main program ?

* Go to the main folder (e.g. `/home/USER/epidnews2event`).

* Run the file `src/main.py`.




## How to run the evaluation scripts ?

* Go to the main folder (e.g. `/home/USER/epidnews2event`).

* Run the file `src/main_eval_bahdja.py` for the event extraction task.

* Run the file `src/main_eval_geocoding.py` for the geocoding task.


## References

* **[Arınık'23]** N. Arınık, R. Interdonato, M. Roche and M. Teisseire, [*An Evaluation Framework for Comparing Epidemic Intelligence Systems*](https://www.doi.org/10.1109/ACCESS.2023.3262462). in IEEE Access, vol. 11, pp. 31880-31901, 2023, doi: 10.1109/ACCESS.2023.3262462.

