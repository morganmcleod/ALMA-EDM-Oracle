class EDMTree():
    '''
    A fast, unordered N-Way tree data structure built using only dictionaries.
    For representing page/folder/file heirarchy.
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.reset()

    def reset(self):
        '''
        Reset to just-constructed state.
        '''
        self.lastId = 0
        self.store = {0 : {'name' : '000_EDMTree_Root',
                           'key' : 0,
                           'attrs' : {},                           
                           'parent' : None,
                           'pname' : '000_EDMTree_XXX',
                           'kids' : [],
                           'depth' : -1
                           }}
        self.nameIndex = {}

    def insert(self, name:str, attrs:dict = {}, parent = None):
        '''
        Insert a node.
        :param name: str must be unique because it will be used for indexing and find()
               Can be subsequently referenced via node['name']
        :param attrs: dict of your attributes you want to keep with the node.
               Can be subsequently referenced via node['attrs']
        :param parent: str name of parent for this node.
        :return int key of new node
        '''
        name = str(name)
        parent = str(parent)
       
        self.lastId += 1
        node = {'name' : name,
                'key' : self.lastId,
                'attrs' : attrs, 
                'pname' : parent,
                'kids' : []
               }
        # the store is a list of nodes indexed by key:
        self.store[self.lastId] = node
        # the nameIndex is a dict mapping name to key, for fast find()
        self.nameIndex[name] = self.lastId
        # return the key:
        return self.lastId
    
    def adopt(self, child, newParentName:str):
        '''
        Change which parent a child is associated with.
        Only changes the parent name.  
        You must subsequently index() to fixup the parent-child relationships.
        :param child: node
        :param newParentName: str name of new parent.
        '''
        key = child['key']
        if key:
            oldParent = self.find(child['pname'])
            if oldParent and key in oldParent['kids']:
                oldParent['kids'].remove[key]
            self.store[key]['pname'] = newParentName
    
    def index(self):
        '''
        Build/rebuild all parent-child relationships and calculate depth of each node.

        After this, the following items become valid:
        node['parent'] int key
        node['depth'] int
        node['kids'] list[int] keys, though you shouldn't need to use this in client code.
        '''
        # clear all the child references:
        for node in self.store.values():
            node['kids'] = []
        
        # Create all the child references based on node['pname']:
        for key, node in self.store.items():
            # only index if key >= 1:
            if key:
                # update parent key from pname:
                parent = self.nameIndex.get(node['pname'], 0)
                node['parent'] = parent
                # add this to parent's kids:
                self.store[parent]['kids'].append(key)

        # Calculate depth of all nodes:
        for node in self.depthFirst():
            # only compute depth if key >= 1:
            if node['key']:
                parent = node.get('parent', 0)
                node['depth'] = self.store[parent].get('depth', 0) + 1
    
    def find(self, name:str):
        '''
        Find a node by name.
        :param name: str to find.
        :return node or None if not found.
        '''
        key = self.nameIndex.get(str(name), None)
        if key:
            return self.store[key]
        else:
            return None
    
    def insertionOrder(self):
        '''
        Generator for traversal in the order the nodes were inserted
        :yeild the next node in the order they were inserted
        '''
        for key, node in self.store.items():
            # skip node 0:
            if key:
                yield node
    
    def depthFirst(self, key:int = 0, postOrder:bool = False, doneHook = None):
        '''
        Generator for recursive depth-first traversal of the tree or a subtree.
        
        :param key: int where to start the traversal.
        :param postOrder: if True, visit the 'key' node last. 
        :param doneHook: optional function(node) that will be called after visiting 
               the 'key' node and its entire subtree.
        :yeild All nodes at or below the 'key' node 
        '''
        
        # Pre order - visit the current node first UNLESS it is the fake root node:
        if not postOrder and key:
            yield self.store[key]

        # Depth-first: recursively traverse children:
        for cId in self.store[key]['kids']:
            for node in self.depthFirst(cId, postOrder = postOrder, doneHook = doneHook):
                yield node
            
        # PostOrder - visit the current node last UNLESS it is the fake root node:
        if postOrder and key:
            yield self.store[key]

        # Call the hook callback if defined: 
        if doneHook:
            doneHook(self.store[key]['name'])

    def breadthFirst(self, key:int = 0):
        '''
        Generator for recursive breadth-first traversal of the tree
        
        Level-order: visit the root node first.        
        :param key: int where to start the traversal.
        :yeild All nodes at or below the 'key' node
        '''
        # Level-order - visit the current node first UNLESS it is the fake root node:
        if key:
            yield self.store[key]
        
        # Breadth-first - visit each child node:
        for cId in self.store[key]['kids']:
            yield self.store[cId]
        
        # Breadth-first - recursively traverse grandchildren:
        for cId in self.store[key]['kids']:
            for gcId in self.store[cId]['kids']:
                for node in self.breadthFirst(gcId):
                    yield node
