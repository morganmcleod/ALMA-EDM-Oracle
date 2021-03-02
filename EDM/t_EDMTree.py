'''
Test the N-way tree class
'''

from EDM.EDMTree import EDMTree

def test1():
    # Test walking sample tree:
    # root-----------+-------------+
    # c1--+---+      c2--+---+     c3--+---+
    # d11 d12 d13    d21 d22 d23   d31 d32 d33--+----+
    #                                      e331 e332 e333
    
    tree = EDMTree()
    root = tree.insert('root')
    c1 = tree.insert('c1', parent = root)
    c2 = tree.insert('c2', parent = root)
    c3 = tree.insert('c3', parent = root)
    d11 = tree.insert('d11', parent = c1)
    d12 = tree.insert('d12', parent = c1)
    d13 = tree.insert('d13', parent = c1)
    d21 = tree.insert('d21', parent = c2)
    d22 = tree.insert('d22', parent = c2)
    d23 = tree.insert('d23', parent = c2)
    d31 = tree.insert('d31', parent = c3)
    d32 = tree.insert('d32', parent = c3)
    d33 = tree.insert('d33', parent = c3)
    e331 = tree.insert('e331', parent = d33)
    e332 = tree.insert('e331', parent = d33)
    e333 = tree.insert('e331', parent = d33)
    tree.index()

    print("\ndepth first pre-order:")
    for node in tree.depthFirst(root, postOrder = False):
        print(node['name'])

    print("\ndepth first post-order:")
    for node in tree.depthFirst(root, postOrder = True):
        print(node['name'])
    
    print("\nbreadth first:")    
    for node in tree.breadthFirst(root):
        print(node['name'])

def test2():
    # Test a simpler tree for post-order = postfix calculator:
    # '2 * (3 + 4)' should represent as (2 (3 4 +) *)
    #
    # *
    # 2 +
    #   3 4 

    tree = EDMTree()
    root = tree.insert('*')
    p1 = tree.insert('2', parent = root)
    p2 = tree.insert('+', parent = root)
    p21 = tree.insert('3', parent = p2)
    p22 = tree.insert('4', parent = p2)
    tree.index()

    print("\ndepth first post-order:")
    for node in tree.depthFirst(root, postOrder = True):
        print(node['name'])

