# class EDM
#
# Automates selecting and formatting the contents of the ALMA EDM database
# into the specified format for migration to the new Alfresco EDM system.
#
# Usage:
#
# See user documentation in 'distributed to users/instructions.docx'. 
#
# Rename EDMDatabase_template.ini to EDMDatabase.ini
#   Fill in your ALMA Oracle server credentials.
#   You may ignore the MySQL section which was used to keep local copies of some tables during development.
#
# >>> from EDM.EDM import EDM
# >>> edm = EDM()
# loadAllForums...
# It starts by loading the tree of all EDM forums.
#
# >>> edm.writeForumsXLSX("EDMForums.xlsx")
# Will produce a spreadsheet of all the forums.
# Users mark which forums they wish to explore via the OWNER column.
#
# >>> edm.readForumsXLSX("User-EDMForums.xlsx")
# Read back in the user's selected forums.
#
# >>> edm.writeDocumentsXLSX("User-Documents.xlsx")
# Write out a brief listing of all the document trees in the selected forums.
# Users mark which documents they wish to migrate via the OWNER column.
#
# >>> edm.readDocumentsXLSX("User-Docuemtns.xlsx")
# Read back in the user's selected documents.
#
# >>> edm.writeMigrationXLSX("User-Migration.xlsx")
# Write out the selected documents in the required format for migration
#
# file share for interacting with users:
# https://astrocloud.nrao.edu/s/jDD4EXFpof4STRR
#
# Morgan McLeod <mmcleod@nrao.edu> February 2021

import os.path
import tablib
import configparser
import copy
import re
from EDM.EDMTree import EDMTree
from ALMAFE.basic.ParseTimeStamp import ParseTimeStamp
from ALMAFE.database.DriverMySQL import DriverMySQL as driverMySQL
from ALMAFE.database.DriverOracle import DriverOracle as driverOracle
from EDM.MLStripper import strip_tags

# NestedParser for matching braces from https://stackoverflow.com/a/14715850
class ParserNode(list):
    def __init__(self, parent=None):
        self.parent = parent

class NestedParser(object):
    def __init__(self, left='\(', right='\)'):
        self.scanner = re.Scanner([
            (left, self.left),
            (right, self.right),
            (r"\s+", None),
            (".+?(?=(%s|%s|$))" % (right, left), self.other),
        ])
        self.result = ParserNode()
        self.current = self.result

    def parse(self, content):
        self.scanner.scan(content)
        return self.result

    def left(self, scanner, token):
        new = ParserNode(self.current)
        self.current.append(new)
        self.current = new

    def right(self, scanner, token):
        self.current = self.current.parent

    def other(self, scanner, token):
        self.current.append(token.strip())

def splitOnBracketsOrSpace(inputStr:str):
    '''
    Splits inputStr into a list of strings based on EDMs {bracket} rules:
    * Within brackets, do not split on space
    * Outside brackets, do split on space
    * Brackets may be nested, in which case return nested lists.
    :param inputStr: str
    :return list[str] with nested lists possible.    
    '''
    output = []
    if inputStr:    
        p = NestedParser(left = '\{', right = '\}')
        try:
            items = p.parse(inputStr)
            for item in items:
                if type(item) is str:
                    for word in item.split():
                        output.append(word)
                elif len(item) == 1:
                    output.append(item[0])
                elif len(item) == 0:
                    output.append('')
                else:
                    output.append(item)
        except:
            raise
    return output

class EDM():
    '''
    Class for retrieving and transforming document metadata from the ALMA EDM SITESCAPE database.
    '''
    
    TIMESTAMP_FORMAT = '%Y/%m/%d-%H:%M:%S'  # used for timestamps in SITESCAPE
    ILLEGAL_CHARACTERS_RE = '[\000-\010]|[\013-\014]|[\016-\037]+'  # chars which cannot be exported by Tablib/OpenPyXL

    def __init__(self):
        '''
        Constructor
        '''
        # load database configuration strings:
        self.__loadConfiguration()
        # the main ALMA EDM database:
        self.driverOracle = driverOracle(self.OracleConfig)
        # can also use a local MySQL server for some tables - for test/debug:    
        self.driverMySQL = driverMySQL(self.MySQLConfig)        
        self.reset()
        # start by loading the forums tree:
        self.loadAllForums()

    def reset(self):
        '''
        Reset to just-constructed state:
        '''
        self.forums = EDMTree()
        self.parseTimeStamp = ParseTimeStamp()

    def __loadConfiguration(self):
        '''
        load our configuration file
        '''
        self.OracleConfig = None
        self.MySQLConfig = None
        config = configparser.ConfigParser()
        config.read("EDMDatabase.ini")
        self.OracleConfig = {
            'host' : config['Oracle']['host'],                  # orait-12c-db.sco.alma.cl
            'user' : config['Oracle']['user'],
            'passwd' : config['Oracle']['passwd'],
            'service_name' : config['Oracle']['service_name'],  # ssf
            'schema' : config['Oracle']['schema'],              # SITESCAPE
            'port' : config['Oracle'].get('port', 1521)         # 1521
            }
        self.MySQLConfig = {
            'enable' : config['MySQL']['enable'],   # global switch to enable/disable using the local MySQL server
            'host' : config['MySQL']['host'],
            'user' : config['MySQL']['user'],
            'passwd' : config['MySQL']['passwd'],
            'database' : config['MySQL']['database'],
            'port' : config['MySQL'].get('port', 3306),         # 3306
            'local_tables' : config['MySQL']['local_tables'].split()  # list of tables available locally
            }
    
    def loadAllForums(self):
        '''
        Load the forums tree
        '''
        self.reset()
        print("loadAllForums...")
        # Most of the meta-data about the forums tree structure is in the ALMA__PROPS table.
        # The actual forum name to table prefix mapping is in TABLE_MAP. 
        q = '''SELECT
                    a.FORUMNAME,
                    a.PROPVAL AS CLASS,
                    b.PROPVAL AS TITLE,
                    c.PROPVAL AS PARENT,
                    d.PROPVAL AS TOPDOCID,
                    e.TABLEPREFIX
                FROM
                    ALMA__PROPS a
                        LEFT OUTER JOIN ALMA__PROPS c ON a.FORUMNAME=c.FORUMNAME AND LOWER(c.PROPID)='parentsummit'
                        LEFT OUTER JOIN ALMA__PROPS d ON a.FORUMNAME=d.FORUMNAME AND LOWER(d.PROPID)='topdocid'
                        LEFT OUTER JOIN TABLE_MAP e ON a.FORUMNAME=e.FORUMNAME,
                    ALMA__PROPS b
                WHERE
                    a.FORUMNAME=b.FORUMNAME AND
                    LOWER(a.PROPID)='class' AND
                    LOWER(b.PROPID)='title' AND
                    (LOWER(a.PROPVAL)='summit' OR LOWER(a.PROPVAL)='docshare')
                ORDER BY
                    b.PROPVAL
            '''
        if int(self.MySQLConfig['enable']) and \
                'ALMA__PROPS' in self.MySQLConfig['local_tables'] and \
                'TABLE_MAP' in self.MySQLConfig['local_tables']:

            # during development/debug, cache some tables locally.  Configure in EDMDatabase.ini
            self.driverMySQL.execute(q)
            rows = self.driverMySQL.fetchall()
        else:
            self.driverOracle.execute(q)
            self.driverOracle.fetchall()
        
        # column indexes in query result:
        FORUMNAME = 0
        CLASS = 1
        TITLE = 2
        PARENT = 3
        TOPDOCID = 4
        TABLEPREFIX = 5

        for row in rows:
            # all rows have a title:
            attrs = {'TITLE' : row[TITLE].strip() if row[TITLE] else ''}

            # 'docshare' rows have an associated document tree: 
            if row[CLASS] == 'docshare':
                attrs['TOPDOCID'] = row[TOPDOCID]
                attrs['TABLEPREFIX'] = row[TABLEPREFIX]
            
            # add to the forums tree:    
            self.forums.insert(row[FORUMNAME], attrs, row[PARENT])
        
        # create the parent-child indexes and depth counter:
        self.forums.index()
    
    def writeForumsXLSX(self, outFile):
        '''
        Write an excel file having all the Forums structure and OWNERs marked, if any.
        :param outFile: output Excel file name
        '''
        sheet = tablib.Dataset(title="EDM Forums")
        sheet.headers = ['OWNER', 'DOCS', 'FORUMNAME', 'TITLE', 'EDM URL']
        # depth-first traversal of the Forums tree:
        for forum in self.forums.depthFirst():
            title = forum['attrs'].get('TITLE', '')
            # guard against falsy title:
            if not title:
                title = ''
            depth = forum.get('depth', 0)
            indent = '  ' * depth
            sheet.append([
                forum['attrs'].get('OWNER', ''),
                'Y' if forum['attrs'].get('TABLEPREFIX', False) else '',
                forum['name'],
                indent + title.replace('\n', '\s'),
                'http://edm.alma.cl/forums/alma/dispatch.cgi/{}/'.format(forum['name'])
            ])
        output = sheet.export('xlsx')
        with open(outFile, 'wb') as f:
            f.write(output)  
    
    def readForumsXLSX(self, inFile, loadDocshares = True):
        '''
        Read an existing Excel file having OWNERs marked, and update my forums tree.
        :param inFile: Excel file having the Forums structure and OWNERs marked
               must have same columns and order as is generated by writeForumsXLSX.
        :param loadDocshares: if True, proceed to load the selected docshares.
        '''
        # column indexes in sheet:
        OWNER = 0
        FORUMNAME = 2
        
        with open(inFile, 'rb') as file:
            sheet = tablib.import_set(file, format = 'xlsx', headers = True)
        for row in sheet:
            if row[OWNER]:
                forum = self.forums.find(row[FORUMNAME])
                if forum:
                    forum['attrs']['OWNER'] = row[OWNER]
                else:
                    print('Forum not found: ' + row[FORUMNAME])
        if loadDocshares:
            self.loadDocshares()
    
    def clearDocshares(self, clearForumOwners = True):
        '''
        Delete any previously loaded docshares and optionally forum owners
        '''
        for forum in self.forums.insertionOrder():
            try:
                del forum['attrs']['docshare']
            except:
                pass
            if clearForumOwners:
                try:
                    del forum['attrs']['OWNER']
                except:
                    pass
                
    def loadDocshares(self):
        '''
        Load the document trees for all the Forums currently marked with an OWNER 
        '''
        # open database connection:
        self.driverOracle.connect()

        # when we enter a subtree that has OWNER marked, save it here:
        forumOwner = None
        # and save the forum name with the OWNER marked here:
        treeTop = None

        # hook function to call after visiting all children of a subtree:
        def doneHook(name):
            nonlocal forumOwner, treeTop
            # when we have finished a subtree having OWNER marked, clear forumOwner:
            if name == treeTop:
                forumOwner = None
        
        # iterate depth-first until OWNER seen
        for forum in self.forums.depthFirst(doneHook = doneHook):
            owner = forum['attrs'].get('OWNER', False)
            # unless we are already in a subtree having OWNER, set forumOwner and treeTop:
            if owner and not forumOwner:
                forumOwner = owner
                treeTop = forum['name']
                        
            # load the doc share, if present and we are in a subtree with a forumOwner:
            if forumOwner:
                # all docshares have TABLEPREFIX:
                if forum['attrs'].get('TABLEPREFIX', False):
                    # if there's already a docshare node, warn that this was previously loaded:
                    if forum['attrs'].get('docshare', False):
                        print("Reloading " + forum['name'])
                    else:
                        print('loadDocshare forum: ' + forum['name'])                        
                    self.__loadDocshare(forum)
                
        # close database connection:
        self.driverOracle.disconnect()
        
    def __loadDocshare(self, forum):
        '''
        Load the documents tree under the specified forum
        :param forum: str FORUMNAME
        '''
        
        # column indexes in query result:
        DOCID           = 0
        DOCCONTENT      = 1
        DOCUMENTTYPE    = 2
        TOPPARENTID     = 3
        PARENTFOLDER    = 4
        PARENTID        = 5
        DOCNUMBER       = 6
        CREATEDBY       = 7
        CREATEDON       = 8
        MODIFIEDBY      = 9
        MODIFIEDON      = 10
        KEYWORDS        = 11 # CATEGORY
        TITLE           = 12
        UPLOADFILEINFO  = 13
        ISWORKFLOW      = 14 # WORKFLOWWFP
        WORKFLOWSTATE   = 15
        LOGO            = 16
        TE_VALUES       = 17
        DE_VALUES       = 18
        WORKFLOWDATA    = 19
        ABSTRACT        = 20
        
        tablePrefix = forum['attrs']['TABLEPREFIX']
        table_D = "{}_D".format(tablePrefix).upper()
        table_K = "{}_K".format(tablePrefix).upper()
        
        q = '''SELECT d.DOCID,d.DOCCONTENT,d.DOCUMENTTYPE,d.TOPPARENTID,d.PARENTFOLDER,d.PARENTID,d.DOCNUMBER,
            d.CREATEDBY,d.CREATEDON,d.MODIFIEDBY,d.MODIFIEDON,d.CATEGORY,d.TITLE,d.UPLOADFILEINFO,d.WORKFLOWWFP,d.WORKFLOWSTATE,
            k1.KVPVAL AS LOGO, k2.KVPVAL AS TE_VALUES, k3.KVPVAL AS DE_VALUES, k4.KVPVAL AS WORKFLOWDATA, k5.KVPVAL AS ABSTRACT
            FROM {} d LEFT OUTER JOIN {} k1
            ON d.DOCID = k1.DOCID AND k1.KVPID = 'logo'
            LEFT OUTER JOIN {} k2
            ON d.DOCID = k2.DOCID AND k2.KVPID = 'te_values'
            LEFT OUTER JOIN {} k3
            ON d.DOCID = k3.DOCID AND k3.KVPID = 'de_values'
            LEFT OUTER JOIN {} k4
            ON d.DOCID = k4.DOCID AND k4.KVPID = 'workflowData'
            LEFT OUTER JOIN {} k5
            ON d.DOCID = k5.DOCID AND k5.KVPID = 'abstractText'
            ORDER BY d.DOCID'''.format(table_D, table_K, table_K, table_K, table_K, table_K)
        
        # make the docshare tree for this forum:
        forum['attrs']['docshare'] = EDMTree()
        try:
            if int(self.MySQLConfig['enable']) and \
                    table_D in self.MySQLConfig['local_tables'] and \
                    table_K in self.MySQLConfig['local_tables']:

                # during development/debug, cache some tables locally.  Configure in EDMDatabase
                self.driverMySQL.execute(q)
                rows = self.driverMySQL.fetchall()
            else:                
                self.driverOracle.execute(q)
                rows = self.driverOracle.fetchall() 
            
            # store last document ID seen to prevent duplicates (from table_K JOIN for abstractText):
            lastDocID = None

            for row in rows:                
                title = row[TITLE].strip() if row[TITLE] else ''
                # EDM does have folders with title labeled 'None' to handle internal page structuring:
                if title.lower() == 'none':
                    title = ''
                # Change any characters not compatible with Tablib/OpenPyXL export to space:
                if title:
                    title = re.sub(self.ILLEGAL_CHARACTERS_RE, ' ', title)
                
                abstract = row[ABSTRACT].strip() if row[ABSTRACT] else ''
                # Change any characters not compatible with Tablib/OpenPyXL export to space:
                if abstract:
                    abstract = re.sub(self.ILLEGAL_CHARACTERS_RE, ' ', abstract)
                
                attrs = {'DOCCONTENT' : row[DOCCONTENT], 
                         'DOCUMENTTYPE' : row[DOCUMENTTYPE],
                         'TOPPARENTID' : row[TOPPARENTID],
                         'PARENTFOLDER' : row[PARENTFOLDER],
                         'PARENTID' : row[PARENTID],
                         'DOCNUMBER' : row[DOCNUMBER],
                         'CREATEDBY' : row[CREATEDBY],
                         'CREATEDON' : row[CREATEDON],
                         'MODIFIEDBY' : row[MODIFIEDBY],
                         'MODIFIEDON' : row[MODIFIEDON],
                         'KEYWORDS' : row[KEYWORDS],
                         'TITLE' : title,
                         'UPLOADFILEINFO' : row[UPLOADFILEINFO],
                         'ISWORKFLOW' : row[ISWORKFLOW],
                         'WORKFLOWSTATE' : row[WORKFLOWSTATE],
                         'LOGO' : row[LOGO], 
                         'WORKFLOWDATA' : row[WORKFLOWDATA],
                         'ABSTRACT' : abstract
                        }
                
                # mark 'folderFrame' parents so they can later adopt the child DOCID:
                if row[DOCCONTENT] and row[DOCCONTENT].startswith('application/x-wgw-id'):
                    attrs['FOLDERFRAME'] = row[DOCCONTENT].split()[1]

                # unpack TE_VALUES into AUTHORS, EDITORS, ALMA_DOC_NUMBER_TE, STATUS
                te_values = row[TE_VALUES]
                # merge dicts: values in second dict override values in first for matching key:
                attrs = {**attrs, **self.parseTE_Values(te_values)}
                
                # unpack DE_VALUES into AUTHORS, ALMA_DOC_NUMBER_DE, FILE_NAME_DE
                de_values = row[DE_VALUES]
                # values from DE_VALUES will override values from TE_VALUES:
                attrs = {**attrs, **self.parseDE_Values(de_values)}

                # add to documents tree but skip to prevent dups if DOCID same as lastDocId:
                if not lastDocID == row[DOCID]:
                    forum['attrs']['docshare'].insert(row[DOCID], attrs, row[PARENTID])                
                lastDocID = row[DOCID]

            # update all FOLDERFRAME nodes to move child sub-trees into place: 
            for doc in forum['attrs']['docshare'].insertionOrder():
                childName = doc['attrs'].get('FOLDERFRAME', None)
                if childName:
                    # find the referenced child:
                    child = forum['attrs']['docshare'].find(childName)
                    # update the child's PARENTID:
                    child['attrs']['PARENTID'] = doc['name']
                    # blank the child's title so that it will be skipped on output:
                    child['attrs']['TITLE'] = ''
                    # make the change the parent-child relationship in the docshare tree:
                    forum['attrs']['docshare'].adopt(child, doc['name'])

            # index the parent-child relationships and depth counter in the docshare tree:
            forum['attrs']['docshare'].index()

        except:
            print(f"Load failed!")
            raise
     
    def writeDocumentsXLSX(self, outFile, setForumOwner = True):        
        '''
        Export docshares having an OWNER to a tabbed spreadsheet
        :param outFile: target filename for .xlsx file
        :param setForumOwner: if True, set owner on top-level docshare folders 
        '''
        outputBook = tablib.Databook()
        
        # when we enter a subtree that has OWNER marked, save it here:
        forumOwner = None
        # and save the forum name with the OWNER marked here:
        treeTop = None
        
        # hook function to call after visiting all children of a subtree:
        def doneHook(name):
            nonlocal forumOwner, treeTop
            # when we have finished a subtree having OWNER marked, clear forumOwner:
            if name == treeTop:
                forumOwner = None
                
        # iterate depth first until OWNER seen
        for forum in self.forums.depthFirst(doneHook = doneHook):
            owner = forum['attrs'].get('OWNER', False)
            # unless we are already in a subtree having OWNER, set forumOwner and treeTop:
            if owner and not forumOwner:
                forumOwner = owner
                treeTop = forum['name']
                
            # write a page to the outputBook for each docshare:
            if forum['attrs'].get('docshare', False):
                self.__writeDocshare(outputBook, forum, forumOwner if setForumOwner else None)

        # export the outputBook:
        if outputBook.sheets():
            output = outputBook.export('xlsx')
            with open(outFile, 'wb') as f:
                f.write(output)
        else:
            print("Nothing selected to write for " + outFile)  
                 
    def __writeDocshare(self, outputBook, forum, forumOwner = None):
        '''
        Write a single docshare to the given outputBook
        :param outputBook: tablib.Databook representing a tabbed spreadsheet
        :param owner: forum owner to be applied to the top-level docshare folders
        :param forum: Node from self.forums tree having docshare to write
        '''
        sheet = tablib.Dataset(title=forum['name'])
        sheet.headers = ['OWNER', 'DOCID', 'DOCNUMBER', 'TITLE', 'EDM_URL', 'FILE_NAME', 'UPLOADFILEINFO']

        # depth-first traversal of the Documents tree:        
        for doc in forum['attrs']['docshare'].depthFirst():
            # skip replies and empty structure:
            if doc['attrs'].get('DOCUMENTTYPE', '') != 'reply':
                docTitle = doc['attrs'].get('TITLE', '')
                uploadFile = doc['attrs'].get('UPLOADFILEINFO', '')
                fileName = doc['attrs'].get('FILE_NAME_DE', '')
                # guard against falsy value:
                if not docTitle:
                    docTitle = ''
                if not uploadFile:
                    uploadFile = ''
                if not fileName:
                    fileName = ''
                
                # skip rows with no useful content:
                if docTitle or uploadFile or fileName:
                           
                    # Handle FolderFrame items:
                    ffTarget = doc['attrs'].get('FOLDERFRAME', None)
                    if ffTarget:
                        url = 'http://edm.alma.cl/forums/alma/dispatch.cgi/{}/folderFrame/{}/'.format(forum['name'], ffTarget)
                    else:
                        url = 'http://edm.alma.cl/forums/alma/dispatch.cgi/{}/docProfile/{}'.format(forum['name'], doc['name'])
                    
                    # rules for setting owner:
                    depth = doc.get('depth', 0)
                    if forumOwner:
                        # if forumOwner was provided, we will set it at top-level docshare folders only:            
                        owner = forumOwner if depth == 0 and ffTarget is None else None
                    else:
                        # if forumOwner was not provided, get it from the document node:
                        owner = doc['attrs'].get('OWNER', '')
    
                    indent = '  ' * depth
                    sheet.append([
                        owner,
                        doc['name'],
                        doc['attrs'].get('DOCNUMBER', ''), 
                        indent + docTitle.strip().replace('\n', '\s'),
                        url,
                        fileName,
                        uploadFile.strip()
                    ])
                    
        outputBook.add_sheet(sheet)

    def readDocumentsXLSX(self, inFile):
        '''
        Read a tabbed spreadsheet of docshares, recording OWNERS selected by the submittor.
        :param inFile: .xlsx spreadsheet of same format as exported by writeSelectedDocumentsXLSX()
        '''
        # sheet column indexes:
        OWNER = 0
        DOCID = 1
        
        # load the spreadheet:
        with open(inFile, 'rb') as file:
            data = tablib.import_book(file, format = 'xlsx', headers = True)
        # loop on tabs:
        for sheet in data.sheets():
            # tab title is the FORUMNAME.  Find it in forums tree:
            forum = self.forums.find(sheet.title)
            if forum:
                # cache the last OWNER we see while loading to assign to the forum
                owner = None
                # load the forum's documents tree:
                if forum['attrs'].get('docshare', False):
                    print("Reloading " + forum['name'])
                else:
                    print('loadDocshare forum: ' + forum['name'])                        
                self.__loadDocshare(forum)
                # loop over rows in the sheet:
                for row in sheet:
                    if row[OWNER]:
                        # and assign OWNER:
                        owner = row[OWNER]
                        document = forum['attrs']['docshare'].find(row[DOCID])
                        if document:
                            document['attrs']['OWNER'] = owner
                        else:
                            print('Document not found: ' + row[DOCID])
                # set ownership on the forum as well:
                if owner:
                    forum['attrs']['OWNER'] = owner
            else:
                print('Forum not found: ' + sheet.title)

    OUTPUT_COLUMNS = [
        'FORUM ID ++',          #0 FORUMNAME
        'FORUM NAME ++',        #1 forum TITLE
        'Doc ID ++',            #2 DOCID
        'Document Title (++)',  #3 TITLE
        'Subject (++)',         #4 ABSTRACT 
        'Authors (++)',         #5 TE_VALUES authoreso -> AUTHORS or from DE_VALUES 
        'Keywords (++)',        #6 KEYWORDS
        'Editors',              #7 TE_VALUES groupeso -> EDITORS
        'ALMA DOC Number (++)', #8 TE_VALUES number -> ALMA_DOC_NUMBER_TE or DE_VALUES -> ALMA_DOC_NUMBER_DE
        'File Name (++)',       #9 FILE_NAME_UL or FILE_NAME_DE
        'Document Type (++)',   #10 from ALMA_DOC_NUMBER -> DOC_TYPE
        'Owner Name (++)',      #11 same as Authors[0]
        'Version (++)',         #12 from ALMA_DOC_NUMBER -> DOC_VERSION
        'Created (++)',         #13 CREATEDON 
        'Modified (++)',        #14 MODIFIEDON
        'Modified By',          #15 MODIFIEDBY
        'Reviewed By (++)',     #16 From WorkFlow or Same as AUTHORS if not CCB Flag
        'Approved By (++)',     #17 From WorkFlow or ''
        'Released By (++)',     #18 From WorkFlow or ''
        'CCB Flag (++)',        #19 ISWORKFLOW not NULL
        'Security Mode (++)',   #20 ''
        'Document Status (++)', #21 TE_VALUES status -> DOC_STATUS_TE or WORKFLOWSTATE -> DOC_STATUS_WF
                                #   [uncontrolled, draft, Under Revision, approved, released, superseded, obsolete, withdrawn]
        'Issuance Agency ++',   #22 from LOGO -> ISS_AGENCY [ESO, NAOJ, NRAO, JAO, Not ALMA DOC] 
        'Doc abstract',         #23 ABSTRACT
        'File Type',            #24 from FILE_NAME -> FILE_TYPE 
                                #   [Adobe PDF, AUTOCAD DWG, MS Word, MS PowerPoint, MS Excel, Txt, MS Project, MS Visio]
        'Posted by',            #25 from LOGO -> POSTED_BY
        'Date Posted'           #26 from UPLOADFILEINFO -> UPLOAD_DATETIME
    ] 

    def writeMigrationXLSX(self, outFile):
        sheet = tablib.Dataset(title="Migration")
        sheet.headers = self.OUTPUT_COLUMNS

        # when we enter a subtree that has OWNER marked, save it here:
        docOwner = None
        # and save the forum name with the OWNER marked here:
        treeTop = None

        # hook function to call after visiting all children of a subtree:
        def doneHook(name):
            nonlocal docOwner, treeTop
            # when we have finished a subtree having OWNER marked, clear forumOwner:
            if name == treeTop:
                docOwner = None
        
        # iterate forums to find OWNERs with docshares:
        for forum in [f for f in self.forums.insertionOrder() 
                      if f['attrs'].get('OWNER', False) 
                      and f['attrs'].get('docshare', False)]:

            # for debugging big docshares - stop before scanning the whole thing:
            if docOwner == 'STOP!':
                break
            
            print('writeMigration forum: ' + forum['name'])

            # iterate documents:
            for doc in forum['attrs']['docshare'].depthFirst(doneHook = doneHook):
                owner = doc['attrs'].get('OWNER', False)
                # unless we are already in a subtree having OWNER, set docOwner and treeTop:
                if owner and not docOwner:
                    docOwner = owner
                    treeTop = doc['name']
                
                # for debugging big docshares - stop before scanning the whole thing:
                if docOwner == 'STOP!':
                    break
                
                # write documents to the outfile
                if docOwner and (doc['attrs'].get('UPLOADFILEINFO') or doc['attrs'].get('FILE_NAME_DE')):
                    docTitle = doc['attrs'].get('TITLE', '')
                    # guard against TITLE had falsy value:
                    if not docTitle:
                        docTitle = ''
                    
                    # from ALMA_DOC_NUMBER unpack DOC_TYPE, DOC_VERSION
                    almaDocNum = doc['attrs'].get('ALMA_DOC_NUMBER_TE', None)
                    if not almaDocNum:
                        almaDocNum = doc['attrs'].get('ALMA_DOC_NUMBER_DE', None)
                    doc['attrs'] = {**doc['attrs'], **self.parseAlmaDocNum(almaDocNum)}
                    
                    # from WORKFLOWDATA, WORKFLOWSTATE unpack DOC_STATUS_WF, REVIEWED_BY, APPROVED_BY, RELEASED_BY
                    workflowData = doc['attrs'].get('WORKFLOWDATA', None)
                    workflowState = doc['attrs'].get('WORKFLOWSTATE', None)
                    doc['attrs'] = {**doc['attrs'], **self.parseWorkflow(workflowData, workflowState)}
                    
                    # unpack UPLOADFILEINFO into FILE_NAME_UL, UPLOAD_BY, UPLOAD_DATETIME
                    uploads = doc['attrs'].get('UPLOADFILEINFO', None)
                    doc['attrs'] = {**doc['attrs'], **self.parseUploadFileInfo(uploads)}
                    
                    # unpack FILENAME_DE or FILE_NAME_UL into FILE_TYPE
                    fileName = doc['attrs'].get('FILE_NAME_DE', None)
                    if not fileName:
                        fileName = doc['attrs'].get('FILE_NAME_UL', None)
                    doc['attrs']['FILE_NAME'] = fileName
                    doc['attrs'] = {**doc['attrs'], **self.parseFilename(fileName)}
                    
                    # unpack LOGO into POSTED_BY, ISS_AGENCY
                    logo = doc['attrs'].get('LOGO', None)
                    doc['attrs'] = {**doc['attrs'], **self.parseLogo(logo)}
                    
                    # fix timestamp formats:
                    createdOn = doc['attrs'].get('CREATEDON', None)
                    modifiedOn = doc['attrs'].get('MODIFIEDON', None)
                    uploadOn = doc['attrs'].get('UPLOAD_DATETIME', None)
                    createdOn, modifiedOn, uploadOn = self.parseTimeStamps(createdOn, modifiedOn, uploadOn)
                    doc['attrs']['CREATEDON'] = createdOn
                    doc['attrs']['MODIFIEDON'] = modifiedOn
                    doc['attrs']['UPLOAD_DATETIME'] = uploadOn
                    
                    # clean up abstract;
                    abstract = doc['attrs'].get('ABSTRACT', '')
                    if abstract:
                        abstract = strip_tags(abstract).replace('\n', ' ').replace('\r', ' ').strip()
                    
                    # find first author:
                    authors = doc['attrs']['AUTHORS']
                    author0 = ''
                    if authors:
                        author0 = authors.split()[0]
                    
                    # is workflow document?
                    isWorkflow = doc['attrs'].get('ISWORKFLOW', False)
                    sheet.append([
                        forum['name'],
                        forum['attrs'].get('TITLE', ''),
                        doc['name'],
                        docTitle.strip().replace('\n', ' '),
                        abstract,
                        authors,
                        ' '.join(splitOnBracketsOrSpace(doc['attrs'].get('KEYWORDS', ''))),
                        doc['attrs']['EDITORS'],
                        almaDocNum if almaDocNum else '',
                        doc['attrs']['FILE_NAME'],
                        doc['attrs']['DOC_TYPE'],
                        author0,
                        doc['attrs']['DOC_VERSION'],
                        doc['attrs']['CREATEDON'],
                        doc['attrs']['MODIFIEDON'],
                        doc['attrs'].get('MODIFIEDBY', ''),
                        doc['attrs'].get('REVIEWED_BY', '') if isWorkflow else doc['attrs']['AUTHORS'],
                        doc['attrs'].get('APPROVED_BY', '') if isWorkflow else '',
                        doc['attrs'].get('RELEASED_BY', '') if isWorkflow else '',
                        '1' if isWorkflow else '0',
                        '',                                 # Security Mode
                        doc['attrs'].get('DOC_STATUS_WF', '') if isWorkflow else doc['attrs']['DOC_STATUS_TE'],
                        doc['attrs']['ISS_AGENCY'],
                        abstract,
                        doc['attrs']['FILE_TYPE'],
                        doc['attrs']['POSTED_BY'],
                        doc['attrs']['UPLOAD_DATETIME']
                    ])
        # export the outputBook:
        output = sheet.export('xlsx')
        with open(outFile, 'wb') as f:
            f.write(output)  
         
    
    def parsePairs(self, values:list, lookup:dict):
        '''
        Treats values as a list of key, value, key, value... and tests whether the keys are in lookup.
        If matched, add lookup.value : key to the output dictionary. 
        :param values: list of str treated as key, value, key, value...
        :param lookup: dict{str : str}
        :return dict{lookup value : key} 
        '''
        vals = copy.copy(values)
        attrsOut = {}
        while vals:
            try:
                key = vals.pop(0)
                value = vals.pop(0)
            except:
                pass
            else:
                field = lookup.get(key, False)
                if field:
                    attrsOut[field] = value
        return attrsOut
    
    def parseTE_Values(self, te_values:str):
        '''
        Parse the contents of a te_values record associated with a document.
        :param te_values: str
        :return dict of {str : str} of matching items found in te_vaues
        '''
        attrsOut = {'AUTHORS' : '',
                    'EDITORS' : '',
                    'ALMA_DOC_NUMBER_TE' : '',
                    'DOC_STATUS_TE' : ''
                   }
        if te_values:
            lookup = {'authoreso' : 'AUTHORS',
                      'groupeso' : 'EDITORS',
                      'number' : 'ALMA_DOC_NUMBER_TE',
                      'status' : 'DOC_STATUS_TE'
                      }
            te_values = splitOnBracketsOrSpace(te_values)
            attrsOut = {**attrsOut, **self.parsePairs(te_values, lookup)}
        return attrsOut
                    
    def parseDE_Values(self, de_values:str):
        '''
        Parse the contents of a de_values record associated with a Workflow-controlled document.
        :param te_values: str
        :return dict of {str : str} of matching items found in de_values
        '''
        attrsOut = {'ALMA_DOC_NUMBER_DE' : '',
                    'FILE_NAME_DE' : '',
                    'AUTHORS' : ''
                    } 
        if de_values:
            lookup = {'de_ele8671' : 'ALMA_DOC_NUMBER_DE',
                      'de_ele10279' : 'FILE_NAME_DE',
                      'de_ele12796' : 'AUTHORS'
                      }
            de_values = splitOnBracketsOrSpace(de_values)
            attrsOut = {**attrsOut, **self.parsePairs(de_values, lookup)}
        return attrsOut
    
    def parseWorkflow(self, workflowData:str, workflowState:str):
        '''
        Parse the contents of the workflowState field and  workflowData record associated with a document.
        :param te_values: str
        :return dict of {str : str} of matching items found in the inputs.
        '''
        attrsOut = {'DOC_STATUS_WF' : '',
                    'REVIEWED_BY' : '',
                    'APPROVED_BY' : '',
                    'RELEASED_BY' : ''                       
                    }
        
        if workflowState:
            attrsOut['DOC_STATUS_WF'] = workflowState 
        
        if workflowData:
            lookup = {'r.in Technical Review' : 'REVIEWED_BY',
                      'r.Approved Document' : 'APPROVED_BY' 
                      }
            items = splitOnBracketsOrSpace(workflowData)
            attrsOut = {**attrsOut, **self.parsePairs(items, lookup)}
            releasedBy = ''
            while items:
                try:
                    key = items.pop(0)
                    value = items.pop(0)
                except:
                    pass
                else:
                    try:
                        if re.match('^a\.To', key):
                            if releasedBy:
                                releasedBy += ' '
                            try:
                                # splitOnBracketsOrSpace returned {{sentence} user}?
                                releasedBy += str(value[1])
                            except(IndexError):
                                # returned just a string?
                                releasedBy += value
                    except:
                        raise
            
            if releasedBy:
                attrsOut['RELEASED_BY'] = releasedBy

        return attrsOut

    def parseAlmaDocNum(self, almaDocNum:str):
        '''
        Parse the ALMA document number to find DOC_TYPE and DOC_VERSION
        :param almaDocNum: str
        :return dict{str : str} of items found.
        '''
        attrsOut = {'DOC_TYPE' : '',
                    'DOC_VERSION' : '',
                    }
        if almaDocNum:
            almaDoc = almaDocNum.translate(str.maketrans('-.', '||')).split('|')
            #ALMA-56.03.00.00-70.35.30.00-A-ICD
            #BEND-55.05.03.02-012-A-CRE
            try:
                attrsOut['DOC_TYPE'] = almaDoc[-1]
                attrsOut['DOC_VERSION'] = almaDoc[-2]
            except:
                pass
                
        return attrsOut
                    
    def parseUploadFileInfo(self, uploadFileInfo:str):
        '''
        Parse the contents of the UPLOADFILEINFO field associated with a document.
        :param uploadFileInfo: str
        :return dict{str : str} of items found.
        '''
        attrsOut = {'FILE_NAME_UL' : '',
                    'UPLOAD_BY' : '',
                    'UPLOAD_DATETIME' : ''
                    }
        if uploadFileInfo:
            upload = splitOnBracketsOrSpace(uploadFileInfo)
            if len(upload) >= 1:
                filename = upload[0]
                attrsOut['FILE_NAME_UL'] = filename
            if len(upload) >= 3:
                attrsOut['UPLOAD_BY'] = upload[2]
            if len(upload) >= 4:
                attrsOut['UPLOAD_DATETIME'] = upload[3]
        return attrsOut

    def parseFilename(self, filename:str):
        '''
        Parse a filename to determine document type.
        :param filename: str
        :return dict{'FILE_TYPE' : str}
        '''
        attrsOut = {'FILE_TYPE' : ''}
        if filename:
            ext = os.path.splitext(filename)
            ext = ext[1].strip('.').lower() if len(ext) >= 2 else ''
                
            if ext in ['pdf']:
                attrsOut['FILE_TYPE'] = 'Adobe PDF'
            elif ext in ['dwg']:
                attrsOut['FILE_TYPE'] = 'AUTOCAD DWG'
            elif ext in ['doc', 'docx', 'docm']:
                attrsOut['FILE_TYPE'] = 'MS Word'
            elif ext in ['ppt', 'pptx', 'pptm']:
                attrsOut['FILE_TYPE'] = 'MS PowerPoint'
            elif ext in ['xls', 'xlsx', 'xlsm', 'xlst']:
                attrsOut['FILE_TYPE'] = 'MS Excel'
            elif ext in ['mpp', 'mpt']:
                attrsOut['FILE_TYPE'] = 'MS Project'
            elif ext in ['vsd', 'vsdx', 'vsdm']:
                attrsOut['FILE_TYPE'] = 'MS Visio'
            elif ext in ['txt', 'csv', 'ini']:
                attrsOut['FILE_TYPE'] = 'Txt'
            else:
                attrsOut['FILE_TYPE'] = ext.upper()
        return attrsOut


    def parseLogo(self, logo:str):
        '''
        Parse the 'logo' record associated with a document to determine who posted it.
        :param logo: str
        :return dict{str : str} of info found
        '''
        attrsOut = {'POSTED_BY' : '',
                    'ISS_AGENCY' : ''
                    }
        if logo:
            # split on delimiters '{}'
            logo = splitOnBracketsOrSpace(logo)
            
            # convert name to firstname.lastname:
            if len(logo) >= 1:
                postedBy = '.'.join(logo[0].split())
                attrsOut['POSTED_BY'] = postedBy

            # parse email to determine issuing agency:
            if len(logo) >= 6:
                email = logo[5].split('@')
                if len(email) >= 2:
                    domain = email[1].lower()
                    
                    if re.search('nrao|nrc[\w\-\.]*.ca', domain):
                        # 'nrao.edu', 'aoc.nrao.edu', 'nrao.cl', 'nrc-cnrc.gc.ca', 'nrc.gc.ca', 'nrc.ca'
                        attrsOut['ISS_AGENCY'] = 'NRAO'
                    
                    elif postedBy in ['ral'] or \
                        re.search('(eso.org|\.es|\.fr|\.ac.uk|\.nl|\.it|\.se)$', domain):
                        # 'eso.org', 'iram.fr', 'oan.es', 'rl.ac.uk', 'sron.rug.nl', 'sron.nl', 'jb.man.ac.uk', 'obs.u-bordeaux1.fr', 'astro.rug.nl', 'inaf.it', 'iasfbo.inaf.it', 'stfc.ac.uk', 'chalmers.se'
                        attrsOut['ISS_AGENCY'] = 'ESO'
                    
                    elif re.search('\.ac\.jp$', domain):
                        # 'nao.ac.jp', 'nro.nao.ac.jp'
                        attrsOut['ISS_AGENCY'] = 'NAOJ'
                    
                    elif domain in ['asiaa.sinica.edu.tw']:
                        attrsOut['ISS_AGENCY'] = 'NAOJ'
                    
                    elif domain in ['alma.cl']:
                        attrsOut['ISS_AGENCY'] = 'JAO'
                    
                    else:
                        attrsOut['ISS_AGENCY'] = 'Not ALMA DOC<' + domain +'>'
        return attrsOut
        
    def parseTimeStamps(self, createdOn, modifiedOn, uploadOn):
        '''
        Convert string timestamps to datetime.  Do nothing if they are already datetime.
        :param createdOn: str or datetime
        :param modifiedOn: str or datetime
        :param uploadOn: str or datetime
        :return (datetime, datetime, datetime) in order createdOn, modifiedOn, uploadOn
        '''
        if createdOn and type(createdOn) is str:
            createdOn = self.parseTimeStamp.parseTimeStampWithFormatString(createdOn, self.TIMESTAMP_FORMAT)
        if modifiedOn and type(modifiedOn) is str:
            modifiedOn = self.parseTimeStamp.parseTimeStampWithFormatString(modifiedOn, self.TIMESTAMP_FORMAT)
        if uploadOn and type(uploadOn) is str:
            uploadOn = self.parseTimeStamp.parseTimeStampWithFormatString(uploadOn, self.TIMESTAMP_FORMAT)
        return (createdOn, modifiedOn, uploadOn)
