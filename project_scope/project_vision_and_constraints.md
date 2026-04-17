# Desired Product

I am looking to create a database of various product recalls that are published by U.S. government agencies. v1 ingests from five sources:

1. The Food and Drug Administration (FDA)
2. The Consumer Product Safety Comission (CPSC)
3. The Department of Agriculture (USDA)
4. The National Highway Traffic Safety Administration (NHTSA)
5. The U.S. Coast Guard (USCG)

Two agencies originally considered are not in v1: the Environmental Protection Agency (EPA) is deferred pending more information about whether a usable enforcement-action feed exists, and the Federal Aviation Administration (FAA) was cut as out of scope. See [ADR 0001](../documentation/decisions/0001-sources-in-scope.md) for the full scope decision and reasoning.

Recall data is reported by each of these organizations in different settings and formats, with variable data update schedules. Because of these data constraints, this project will need to do the following: 

* Fetch new and updated recall data from each organization's source at a sensible and managable cadence.
* Validate the expected format and content of the recall data.
* Clean and augement recall data to fit project-defined data model
* Once verified, new and edited data will be added/amended to the database or data warehouse. 
* This database/data warehouse will need to support both a website/app that will provide users with both an explantation of the project data, what it measures, and how it can be used as well as data visualization dashboards and reports of recall data in addition to a keyword search functionality for specific recall(s).
* This database/data warehouse will also need an API for users (and maybe the web app itself? )to leverage for automated data retrival.

This database will need to be seeded first with historical recalls data and then the data engineering pipeline will need to be constructed to automatically add/update with ongoing recalls data in real time. 

In addition to this data pipeline, database/data warehouse, API, and website/app I would like to create thorough data governance, metadata, and data documentation for this project. 

# Tooling
* Ideally I would like as much of the code for this project as possible to be in the python framework.
* I would prefer as much tooling to be open-source software as possible.

# Constraints
* This is a personal project I am doing as a way to exhibit my software developer and data engineering skills. As such, I have a bias for as many tools as necessary to be free or low-cost options. 
* I would like this to run end-to-end automated and take advantage of as much data engineering and cloud tools that make sense but also need to keep costs at or near zero for all portions of the stack and for both the backend data pipeline, data storage, as well as the API and front end app.




