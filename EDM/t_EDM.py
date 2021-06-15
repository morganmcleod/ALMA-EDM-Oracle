import os
os.chdir('L:\python\ALMA-EDM-Oracle')
from EDM.EDM import EDM

print("starting")     
edm = EDM()

edm.readDocumentsXLSX("Workspace/BACKEND-Documents.xlsx")
edm.readDocumentsXLSX("Workspace/FELO-Documents.xlsx")
edm.readDocumentsXLSX("Workspace/FRONTEND_MTM_Documents.xlsx")
# edm.readDocumentsXLSX("Workspace/Photonics-Documents.xlsx")
edm.readDocumentsXLSX("Workspace/SCIENCE-Documents.xlsx")
edm.writeDocumentsXLSX("Workspace/coverage-2021-06-11.xlsx", setForumOwner = False, maxDepth = 5)