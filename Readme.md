## Welcome to the **SUPREMESTORES** DWH Project!

This is the Data Warehouse that I've been working on during the EPAM Data Analytics engineering lab. This is the public version of that project, extended with multiple additional features.

Schematics & documentation can be found in `/Docs` folder.

Scripts to load the data, schemas & other metadata are situated in the `/Scripts` folder

The source data (url) and all the transformations are listed in the `/Data Sources/Sources/SourceProcessing.ipynb` notebook. The transformations were aimed to enrich the source, both column and row-wise. It is also important to mention that the source was split into two, but more on that in the notebook. Since source files are too large to be uploaded on git - just download the source `.csv` from kaggle and run it through all the cells in the aforementioned notebook (theres also a link to that dataset!), if you are interested in running the dwh locally on your machine.

The source increment data is also uploaded, as well as the notebook (`/Data Sources/Sources/IncrementCreation.ipynb`) that lists all the procedures that were used to create the increment (from the transformed source data, obviously).

List of Known Issues:

- Fact Table Loading procedure creates duplicated rows (due to joins)

- Bash script for cron job (scheduled batch load) is not attached

**to be fixed in version 1.2**

