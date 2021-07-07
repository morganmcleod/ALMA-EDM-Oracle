import os
os.chdir('L:\python\ALMA-EDM-Oracle')
from EDM.EDM import EDM

print("starting")     
edm = EDM()

def coverage():
    global edm
    edm.readDocumentsXLSX("Workspace/BACKEND-Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/FELO-Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/FRONTEND_MTM_Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/Meetings-Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/CORR-Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/Photonics-Documents2.xlsx")
    edm.readDocumentsXLSX("Workspace/SCIENCE-Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/Documents_Brito.xlsx")
    edm.writeDocumentsXLSX("Workspace/coverage-2021-07-07.xlsx", setForumOwner = False, maxDepth = 5)
    
def migrationToDo():
    global edm
    edm.readDocumentsXLSX("Workspace/FRONTEND_MTM_Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/Meetings-Documents.xlsx")
    edm.readDocumentsXLSX("Workspace/CORR-Documents.xlsx")
    edm.writeMigrationXLSX("Workspace/FE_CORR_Meet-Migration.xlsx")
