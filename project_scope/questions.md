# Architecture Questions
* What is the data model for recalls data from each source? OLTP? OLAP? Relational? NoSQL? Object-oriented? Hierarchial? Network?
  * What is the unit of analysis? 
  * What are the features that are present in all sources?
  * What are the features present in some but not others?
  * What, if anything, needs to be reconciled between recalls from different sources? How would that be done?
* Do I need a Landing Zone and/or Quarantine Zone?
* What tooling and architecture would I need to consider for each of the following layers:
  * Ingestion
  * Landing Zone
  * Processing
  * Serving
  * Consumption
* Does an ETL, ELT, or some other pipeline work best for this project?
* What are the pros and cons of utilizing a Data Warehouse, RDBMS, or NoSQL database for serving the data? I am assuming blob storage isn't necessary based on my data size and format. Is that a good assumption? How am I storing data?
* Should there be different repositories for the data pipeline, website/app, API and any other work bundles associated with this project?
* Where will project code be hosted/executed?
* Should project code be containerized? If so, how and where?
* Should my project update data in with batched or streaming methodology/layer?
* Lambda (separate batch and speed data pipelines) vs. Kappa (single stream processing pipeline) architecture?
* Should my transaction models be ACID, BASE, or something else?
* Should my query types be OLTP (Online Transaction Processing) or OLAP (Online Analytical Processing)?

# Tooling Questions
* Should I look into [dcpy](https://github.com/dcherian/dcpy) python module? Used by folks at the [NYC Department of City Planning](https://github.com/NYCPlanning/data-engineering) did a [demo](https://github.com/NYCPlanning/open_data_ingest_demo) on it. More information in my `One Tool for All Your Public (or Private) Data Extraction Needs`.
* What are free/low-cost and preferably open-source tools I can use to both draw/diagram my data model/database structure and my directed acyclic graphs (DAGs) that will help me document and understand this project better?
* What am I using for:
  * Linting?
  * Type checking?
  * Tests?
  * Data validation model?
* What are API fixtures and how can I leverage them to provide more information about how to engineer around each API that I am interacting with?

# Data Source Questions
* What is the publishing cadence of each data source?
* Are edits/updates to ongoing recalls provided in the same data source? If so, how are they denoted? (e.g. are updates "in place" or would they change the location of a recall in the source? What are the data fields that denote when recalls are posted and when any updates are made?)
* How often are new recalls and/or recall updates being add to the source data? How many typically in a given time period window? How many historical records for each source? 
