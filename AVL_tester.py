from utils.AVL import *

tree = AVLTree()
for i in range(1000):
    tree.insert(Node(i, i))
lst = tree.find_greater_than(800)
vals = [node.val for node in lst]
print(vals)

lst = tree.find_smaller_than(215)
vals = [node.val for node in lst]
print(vals)

lst = tree.find_between(12, 379)
vals = [node.val for node in lst]
print(vals)
