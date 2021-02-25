# ALMA EDM Document Migration

ALMA has engaged a contractor to migrate documents and metadata from the existing EDM databases to the new Alfresco system. The main input for this subcontractor is a spreadsheet populated with all the document IDs and metadata to be transferred.

### See **Instructions.docx** for the step-by-step procedure.

The spreadsheet example provided by ALMA is **Migration Matrix-JAO-24 Dec 2020_AA (CA).xlsx**. Morgan McLeod [mmcleod@nrao.edu](mailto:mmcleod@nrao.edu) on behalf of the ALMA FE IET has developed some scripts to partially automate populating the template. He will execute these scripts on your behalf during the migration period. This page shall serve as the interface between document owners this service.

##### Example:

See the contents of the **EXAMPLE** folder

##### Source code and other details:

**ALMA EDM Database Schema for Migration.pdf** gives the database structure of the existing ALMA EDM database and the structure of the output template. *It is for reference only. You do not need to understand it to use this service.*

**Source code** for these scripts Is available on [GitHub](https://github.com/morganmcleod/ALMA-EDM-Oracle).

It depends on a few Python libraries: 
See [requirements.txt](https://github.com/morganmcleod/ALMA-EDM-Oracle/blob/master/requirements.txt).

$ pip install -r requirements.txt
