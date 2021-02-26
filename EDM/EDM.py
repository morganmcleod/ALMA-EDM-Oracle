import os.path
import tablib
import configparser
import re
from EDM.EDMTree import EDMTree
from ALMAFE.basic.ParseTimeStamp import ParseTimeStamp
from ALMAFE.database.DriverMySQL import DriverMySQL as driverSQL
from ALMAFE.database.DriverOracle import DriverOracle as driverOracle
from EDM.MLStripper import strip_tags

# file share:
# https://astrocloud.nrao.edu/s/jDD4EXFpof4STRR

class EDM():
    '''
    Class for retrieving and transforming document metadata from the ALMA EDM SITESCAPE database.
    '''
    
    TIMESTAMP_FORMAT = '%Y/%m/%d-%H:%M:%S'  # used for timestamps in SITESCAPE
    ILLEGAL_CHARACTERS_RE = '[\000-\010]|[\013-\014]|[\016-\037]+'

    def __init__(self):
        self.__loadConfiguration()
        self.driverOracle = driverOracle(self.OracleConfig)    
        self.driverSQL = driverSQL(self.MySQLConfig)
        self.reset()
        self.loadAllForums()

    def reset(self):
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
            'host' : config['Oracle']['host'],
            'user' : config['Oracle']['user'],
            'passwd' : config['Oracle']['passwd'],
            'service_name' : config['Oracle']['service_name'],
            'schema' : config['Oracle']['schema']
            }
        self.MySQLConfig = {
            'enable' : config['MySQL']['enable'],
            'host' : config['MySQL']['host'],
            'user' : config['MySQL']['user'],
            'passwd' : config['MySQL']['passwd'],
            'database' : config['MySQL']['database'],
            'local_tables' : config['MySQL']['local_tables'].split()
            }
    
    def loadAllForums(self):
        self.reset()
        print("loadAllForums...")
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

            # during development/debug, cache some tables locally.  Configure in EDMDatabase
            self.driverSQL.execute(q)
            rows = self.driverSQL.fetchall()
        else:
            self.driverOracle.execute(q)
            self.driverOracle.fetchall()
        
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
                # add to the forums tree; attach to 'root' if not found:
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
        :param loadDocshares: if True, proceed to load the selected docshares.
        '''
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
            

    def clearDocshares(self):
        '''
        Delete any previously loaded docshares
        '''
        for forum in self.forums.insertionOrder():
            try:
                del forum['attrs']['docshare']
            except:
                pass
            
    def loadDocshares(self):
        '''
        Load the document trees for all the Forums currently marked with an OWNER 
        '''
        self.driverOracle.connect()

        forumOwner = None
        treeTop = None

        # hook function to call after visiting all children of a subtree:
        def doneHook(name):
            nonlocal forumOwner, treeTop
            if name == treeTop:
                forumOwner = None
        
        # iterate depth first until OWNER seen
        for forum in self.forums.depthFirst(doneHook = doneHook):
            owner = forum['attrs'].get('OWNER', False)
            # set forumOwner
            if owner and not forumOwner:
                forumOwner = owner
                treeTop = forum['name']
                        
            # load the doc share, if present:
            if forumOwner:
                if forum['attrs'].get('TABLEPREFIX', False):
                    if forum['attrs'].get('docshare', False):
                        print("revisiting " + forum['name'])
                    self.__loadDocshare(forum)
                
        self.driverOracle.disconnect()
        
    def __loadDocshare(self, forum):
        '''
        Load the documents tree under the specified forum
        :param forum: str FORUMNAME
        '''
        print('loadDocuments forum: ' + forum['name'])
        
        DOCID = 0
        DOCCONTENT = 1
        DOCUMENTTYPE = 2
        TOPPARENTID = 3
        PARENTFOLDER = 4
        PARENTID = 5
        DOCNUMBER = 6
        CREATEDBY = 7
        CREATEDON = 8
        MODIFIEDBY = 9
        MODIFIEDON = 10
        KEYWORDS = 11    # CATEGORY in TABLE_D
        TITLE = 12
        UPLOADFILEINFO = 13
        ISWORKFLOW = 14
        LOGO = 15
        TE_VALUES = 16
        DE_VALUES = 17
        WORKFLOWDATA = 18
        ABSTRACT = 19
        
        tablePrefix = forum['attrs']['TABLEPREFIX']
        table_D = "{}_D".format(tablePrefix).upper()
        table_K = "{}_K".format(tablePrefix).upper()
        
        q = '''SELECT d.DOCID,d.DOCCONTENT,d.DOCUMENTTYPE,d.TOPPARENTID,d.PARENTFOLDER,d.PARENTID,d.DOCNUMBER,
            d.CREATEDBY,d.CREATEDON,d.MODIFIEDBY,d.MODIFIEDON,d.CATEGORY,d.TITLE,d.UPLOADFILEINFO,d.WORKFLOWWFP,
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
                self.driverSQL.execute(q)
                rows = self.driverSQL.fetchall()
            else:                
                self.driverOracle.execute(q)
                rows = self.driverOracle.fetchall() 
            
            lastDocID = None
            for row in rows:
                
                title = row[TITLE].strip() if row[TITLE] else ''
                if title.lower() == 'none':
                    title = ''
                if title:
                    try:
                        title = re.sub(self.ILLEGAL_CHARACTERS_RE, ' ', title)
                    except:
                        raise
                
                abstract = row[ABSTRACT].strip() if row[ABSTRACT] else ''
                if abstract:
                    try:
                        abstract = re.sub(self.ILLEGAL_CHARACTERS_RE, ' ', abstract)
                    except:
                        raise
                
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
                         'LOGO' : row[LOGO], 
                         'WORKFLOWDATA' : row[WORKFLOWDATA],
                         'ABSTRACT' : abstract
                        }
                # mark 'folderFrame' parents:
                if row[DOCCONTENT] and row[DOCCONTENT].startswith('application/x-wgw-id'):
                    attrs['FOLDERFRAME'] = row[DOCCONTENT].split()[1]

                # unpack TE_VALUES into AUTHORS, EDITORS, ALMA_DOC_NUMBER, STATUS
                te_values = row[TE_VALUES]
                attrs = {**attrs, **self.parseTE_Values(te_values)}
                
                # unpack DE_VALUES into AUTHORS, ALMA_DOC_NUMBER, FILENAME
                de_values = row[DE_VALUES]
                attrs = {**attrs, **self.parseDE_Values(de_values)}

                # add to documents tree, prevent duplicates on 1-many with k3.KVPID = 'abstractText'
                if not lastDocID == row[DOCID]:
                    forum['attrs']['docshare'].insert(row[DOCID], attrs, row[PARENTID])
                lastDocID = row[DOCID]

            for doc in forum['attrs']['docshare'].insertionOrder():
                childName = doc['attrs'].get('FOLDERFRAME', None)
                if childName:
                    child = forum['attrs']['docshare'].find(childName)
                    child['attrs']['PARENTID'] = doc['name']
                    child['attrs']['TITLE'] = ''
                    forum['attrs']['docshare'].adopt(child, doc['name'])

            # create the name index and parent-child relationships:
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
        
        # owner of the subtree we are exploring:
        forumOwner = None
        treeTop = None
        
        # hook function to call after visiting all children of a subtree:
        def doneHook(name):
            nonlocal forumOwner, treeTop
            if name == treeTop:
                forumOwner = None
                
        # iterate depth first until OWNER seen
        for forum in self.forums.depthFirst(doneHook = doneHook):
            owner = forum['attrs'].get('OWNER', False)
            # set forumOwner
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
                fileName = doc['attrs'].get('FILE_NAME', '')
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
                        # if forumOwner was not provided, use get it from the document nodes:
                        owner = doc['attrs'].get('OWNER', '')
    
                    indent = '  ' * depth
                    sheet.append([
                        owner,
                        doc['name'],
                        doc['attrs'].get('DOCNUMBER', ''), 
                        indent + docTitle.strip().replace('\n', '\s'),
                        url,
                        doc['attrs'].get('FILE_NAME', ''),
                        uploadFile.strip()
                    ])
                    
        outputBook.add_sheet(sheet)

    def readDocumentsXLSX(self, inFile):
        '''
        Read a tabbed spreadsheet of docshares, recording OWNERS selected by the submittor.
        :param inFile: .xlsx spreadsheet of same format as exported by writeSelectedDocumentsXLSX()
        '''
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
        'ALMA DOC Number (++)', #8 TE_VALUES number -> ALMA_DOC_NUMBER or from DE_VALUES
        'File Name (++)',       #9 from UPLOADFILEINFO -> FILE_NAME or from DE_VALUES
        'Document Type (++)',   #10 from ALMA_DOC_NUMBER -> DOC_TYPE
        'Owner Name (++)',      #11 same as Authors[0]
        'Version (++)',         #12 from ALMA_DOC_NUMBER -> DOC_VERSION
        'Created (++)',         #13 CREATEDON 
        'Modified (++)',        #14 MODIFIEDON
        'Modified By',          #15 MODIFIEDBY
        'Reviewed By (++)',     #16 Same as AUTHORS when not CCB Flag else ''
        'Approved By (++)',     #17 ''
        'Released By (++)',     #18 ''
        'CCB Flag (++)',        #19 ISWORKFLOW not NULL
        'Security Mode (++)',   #20 ''
        'Document Status (++)', #21 TE_VALUES status -> DOC_STATUS or from WORKFLOWDATA [uncontrolled, draft, Under Revision, approved, released, superseded, obsolete, withdrawn]
        'Issuance Agency ++',   #22 from LOGO -> ISS_AGENCY [ESO, NAOJ, NRAO, JAO, Not ALMA DOC] 
        'Doc abstract',         #23 ABSTRACT
        'File Type',            #24 from FILE_NAME -> FILE_TYPE [Adobe PDF, AUTOCAD DWG, MS Word, MS PowerPoint, MS Excel, Txt, MS Project, MS Visio]
        'Posted by',            #25 from LOGO -> POSTED_BY
        'Date Posted'           #26 from UPLOADFILEINFO -> UPLOAD_DATETIME
    ] 

    def writeMigrationXLSX(self, outFile):
        sheet = tablib.Dataset()
        sheet.headers = self.OUTPUT_COLUMNS

        # owner of the document subtree we are exploring:
        docOwner = None
        treeTop = None
    
        # hook function to call after visiting all children of a subtree:
        def doneHook(name):
            nonlocal docOwner, treeTop
            if name == treeTop:
                docOwner = None
        
        # iterate forums to find OWNERs with docshares:
        for forum in [f for f in self.forums.insertionOrder() 
                      if f['attrs'].get('OWNER', False) 
                      and f['attrs'].get('docshare', False)]:

            if docOwner == 'STOP!':
                break

            for doc in forum['attrs']['docshare'].depthFirst(doneHook = doneHook):
                owner = doc['attrs'].get('OWNER', False)
                if owner and not docOwner:
                    docOwner = owner
                    treeTop = doc['name']
                
                if docOwner == 'STOP!':
                    break
                
                # write documents to the outfile
                if docOwner and doc['attrs'].get('UPLOADFILEINFO'):                    
                    docTitle = doc['attrs'].get('TITLE', '')
                    # guard against TITLE had falsy value:
                    if not docTitle:
                        docTitle = ''
                    
                    # from ALMA_DOC_NUMBER unpack DOC_TYPE, DOC_VERSION
                    almaDocNum = doc['attrs'].get('ALMA_DOC_NUMBER', None)
                    doc['attrs'] = {**doc['attrs'], **self.parseAlmaDocNum(almaDocNum)}
                    
                    # from WORKFLOWDATA unpack DOC_STATUS, REVIEWED_BY, APPROVED_BY
                    workflowData = doc['attrs'].get('WORKFLOWDATA', None)
                    doc['attrs'] = {**doc['attrs'], **self.parseWorkflowData(workflowData)}
                    
                    # unpack UPLOADFILEINFO into FILE_NAME, FILE_TYPE, UPLOAD_BY, UPLOAD_DATETIME
                    uploads = doc['attrs'].get('UPLOADFILEINFO', None) 
                    doc['attrs'] = {**doc['attrs'], **self.parseUploadFileInfo(uploads)}
                    
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
                    
                    # clean up authors:
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
                        ' '.join(self.splitOnBracketsOrSpace(doc['attrs'].get('KEYWORDS', ''))),
                        doc['attrs']['EDITORS'],
                        doc['attrs']['ALMA_DOC_NUMBER'],
                        doc['attrs']['FILE_NAME'],
                        doc['attrs']['DOC_TYPE'],
                        author0,
                        doc['attrs']['DOC_VERSION'],
                        doc['attrs']['CREATEDON'],
                        doc['attrs']['MODIFIEDON'],
                        doc['attrs'].get('MODIFIEDBY', ''),
                        doc['attrs'].get('REVIEWED_BY', '') if isWorkflow else doc['attrs']['AUTHORS'],
                        doc['attrs'].get('APPROVED_BY', '') if isWorkflow else '',
                        '',     # Released By
                        '1' if isWorkflow else '0',
                        '',     # Security Mode
                        doc['attrs']['DOC_STATUS'],
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
         
    def splitOnBracketsOrSpace(self, inputStr:str):
        output = []
        while inputStr:
            inputStr = inputStr.strip()
            if inputStr[0] == '{':
                pos = inputStr.find('}')
                if pos > 0:
                    output.append(inputStr[1:pos].strip())
                    inputStr = inputStr[pos + 1:]
                else:
                    inputStr = inputStr[1:]
            else:
                pos = inputStr.find(' ')
                if pos > 0:
                    output.append(inputStr[:pos].strip())
                    inputStr = inputStr[pos + 1:]
                else:
                    output.append(inputStr.strip())
                    inputStr = None
        return output
    
    def parsePairs(self, values:list, lookup:dict):
        attrsOut = {}
        while values:
            try:
                key = values.pop(0)
                value = values.pop(0)
            except:
                pass
            else:
                field = lookup.get(key, False)
                if field:
                    attrsOut[field] = value
        return attrsOut
    
    def parseTE_Values(self, te_values:str):
        attrsOut = {'AUTHORS' : '',
                    'EDITORS' : '',
                    'ALMA_DOC_NUMBER' : '',
                    'DOC_STATUS' : ''
                   }
        if te_values:
            lookup = {'authoreso' : 'AUTHORS',
                      'groupeso' : 'EDITORS',
                      'number' : 'ALMA_DOC_NUMBER',
                      'status' : 'DOC_STATUS'
                      }
            te_values = self.splitOnBracketsOrSpace(te_values)
            attrsOut = {**attrsOut, **self.parsePairs(te_values, lookup)}
        return attrsOut
                    
    def parseDE_Values(self, de_values:str):
        attrsOut = {'ALMA_DOC_NUMBER' : '',
                    'FILE_NAME' : '',
                    'AUTHORS' : ''
                    } 
        if de_values:
            lookup = {'de_ele8671' : 'ALMA_DOC_NUMBER',
                      'de_ele10279' : 'FILE_NAME',
                      'de_ele12796' : 'AUTHORS'
                      }
            de_values = self.splitOnBracketsOrSpace(de_values)
            attrsOut = {**attrsOut, **self.parsePairs(de_values, lookup)}
        return attrsOut
    
    def parseWorkflowData(self, workflowData:str):
        attrsOut = {'DOC_STATUS' : '',
                    'APPROVED_BY' : '',
                    'REVIEWED_BY' : '',
                    'WITHDRAWN_BY' : ''                       
                    }
        if workflowData:
            #uncontrolled, draft, Under Revision, approved, released, superseded, obsolete, withdrawn
    
            lookup = {'r.Approved Document' : 'APPROVED_BY',
                      'r.in Technical Review' : 'REVIEWED_BY',
                      'r.Withdrawn Document' : 'WITHDRAWN_BY'
                      }
            workflowData = self.splitOnBracketsOrSpace(workflowData)
            attrsOut = {**attrsOut, **self.parsePairs(workflowData, lookup)}
            if attrsOut.get('APPROVED_BY'):
                attrsOut['DOC_STATUS'] = 'approved'
            elif attrsOut.get('REVIEWED_BY'):
                attrsOut['DOC_STATUS'] = 'under revision'
            elif attrsOut.get('WITHDRAWN_BY'):
                attrsOut['DOC_STATUS'] = 'withdrawn'
        return attrsOut

    def parseAlmaDocNum(self, almaDocNum:str):
        attrsOut = {'DOC_TYPE' : '',
                    'DOC_VERSION' : '',
                    }
        if almaDocNum:
            almaDoc = almaDocNum.translate(str.maketrans('-.', '||')).split('|')
            if len(almaDoc) >= 7:
                attrsOut['DOC_VERSION'] = almaDoc[6]
            if len(almaDoc) >= 8:
                attrsOut['DOC_TYPE'] = almaDoc[7]
        return attrsOut
                    
    def parseUploadFileInfo(self, uploadFileInfo:str):
        attrsOut = {'FILE_NAME' : '',
                    'FILE_TYPE' : '',
                    'UPLOAD_NUM' : '',
                    'UPLOAD_BY' : '',
                    'UPLOAD_DATETIME' : ''
                    }
        if uploadFileInfo:
            upload = self.splitOnBracketsOrSpace(uploadFileInfo)
            if len(upload) >= 1:
                filename = upload[0]
                attrsOut['FILE_NAME'] = filename
                ext = os.path.splitext(filename)
                ext = ext[1].strip('.').lower() if len(ext) >= 2 else ''
            if len(upload) >= 2:
                attrsOut['UPLOAD_NUM'] = upload[1]    # what is this number?
            if len(upload) >= 3:
                attrsOut['UPLOAD_BY'] = upload[2]
            if len(upload) >= 4:
                attrsOut['UPLOAD_DATETIME'] = upload[3]
                
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


    def parseLogo(self, logo):
        attrsOut = {'POSTED_BY' : '',
                    'ISS_AGENCY' : ''
                    }
        if logo:
            # split on delimiters '{}':  first convert them to '|' and split:
            logo = self.splitOnBracketsOrSpace(logo)
            
            if len(logo) >= 1:
                postedBy = '.'.join(logo[0].split())
                attrsOut['POSTED_BY'] = postedBy

            if len(logo) >= 6:
                email = logo[5].split('@')
                if len(email) >= 2:
                    domain = email[1].lower()
                    if domain in ['nrao.edu', 'nrao.cl', 'nrc-cnrc.gc.ca', 'nrc.gc.ca', 'nrc.ca'] :
                        attrsOut['ISS_AGENCY'] = 'NRAO'
                    elif postedBy in ['ral'] or \
                           domain in ['eso.org', 'iram.fr', 'oan.es', 'rl.ac.uk', 'sron.rug.nl', 'sron.nl', 
                                      'astro.rug.nl', 'inaf.it', 'iasfbo.inaf.it', 'stfc.ac.uk', 'chalmers.se']:
                        attrsOut['ISS_AGENCY'] = 'ESO'
                    elif domain in ['nao.ac.jp', 'nro.nao.ac.jp', 'asiaa.sinica.edu.tw']:
                        attrsOut['ISS_AGENCY'] = 'NAOJ'
                    elif domain in ['alma.cl']:
                        attrsOut['ISS_AGENCY'] = 'JAO'
                    else:
                        attrsOut['ISS_AGENCY'] = 'Not ALMA DOC<' + domain +'>'
        return attrsOut
        
    def parseTimeStamps(self, createdOn, modifiedOn, uploadOn):
        if createdOn and type(createdOn) is str:
            createdOn = self.parseTimeStamp.parseTimeStampWithFormatString(createdOn, self.TIMESTAMP_FORMAT)
        if modifiedOn and type(modifiedOn) is str:
            modifiedOn = self.parseTimeStamp.parseTimeStampWithFormatString(modifiedOn, self.TIMESTAMP_FORMAT)
        if uploadOn and type(uploadOn) is str:
            uploadOn = self.parseTimeStamp.parseTimeStampWithFormatString(uploadOn, self.TIMESTAMP_FORMAT)
        return (createdOn, modifiedOn, uploadOn)
