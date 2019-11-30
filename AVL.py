class AVLTree:

    def __init__(self):
        self._root = None

    def insert(self, node):
        self._root = self._insert(self._root, node)

    def find(self, val):
        return self._find(self._root, val)

    def find_greater_than(self, val):
        lst = []
        self._find_greater_than(self._root, val, lst)
        return lst

    def find_smaller_than(self, val):
        lst = []
        self._find_smaller_than(self._root, val, lst)
        return lst

    def find_between(self, a, b):
        lst = []
        self._find_between(self._root, a, b, lst)
        return lst

    def delete(self, val, key):
        if isinstance(key, set):
            self._root = self._delete(self._root, val, key)
        else:
            self._root = self._delete(self._root, val, set(key))
    
    def to_list(self):
        lst = []
        self._in_order(self._root, lst)
        return lst

    def pre_order(self):
        lst = []
        self._pre_order(self._root, lst)
        return lst

    @staticmethod
    def _pre_order(root, lst):
        if root:
            lst.append(root.val)
            lst.append(-1)
            AVLTree._pre_order(root.left, lst)
            lst.append(-2)
            AVLTree._pre_order(root.right, lst)

    @staticmethod
    def _in_order(root, lst):
        if root:
            if not root.left and not root.right:
                lst.append(root)
            else:
                AVLTree._in_order(root.left, lst)
                lst.append(root)
                AVLTree._in_order(root.right, lst)

    @staticmethod
    def _delete(root, val, key):
        if not root:
            return root
        elif val == root.val:
            root.key.discard(key)
            if len(root.key) == 0:
                if not root.left:
                    t = root.right
                    root = None
                    return t
                elif not root.right:
                    t = root.left
                    root = None
                    return t
                else:
                    t = AVLTree._get_min_node(root.right)
                    root.val = t.val
                    root.key = t.key
                    root.right = AVLTree._delete(root.right, t.val, t.key)
        elif val < root.val:
            root.left = AVLTree._delete(root.left, val, key)
        else:
            root.right = AVLTree._delete(root.right, val, key)

        balance = AVLTree._get_balance(root)
        if balance > 1:
            if AVLTree._get_balance(root.left) < -1:
                root.left = AVLTree._left_rotate(root.left)
            return AVLTree._right_rotate(root)
        elif balance < -1:
            if AVLTree._get_balance(root.right) > 1:
                root.left = AVLTree._right_rotate(root.right)
            return AVLTree._left_rotate(root)
        else:
            AVLTree._update_height(root)
            return root
    
    @staticmethod
    def _find(root, val):
        if not root:
            return root
        elif val == root.val:
            return root
        elif val < root.val:
            return AVLTree._find(root.left, val)
        else:
            return AVLTree._find(root.right, val)

    @staticmethod
    def _insert(root, node):
        if not root:
            return node
        elif node.val == root.val:
            root.key |= node.key
            return root
        elif node.val < root.val:
            root.left = AVLTree._insert(root.left, node)
        else:
            root.right = AVLTree._insert(root.right, node)

        balance = AVLTree._get_balance(root)
        if balance > 1:
            if node.val >= root.left.val:
                root.left = AVLTree._left_rotate(root.left)
            return AVLTree._right_rotate(root)
        elif balance < -1:
            if node.val < root.right.val:
                root.right = AVLTree._right_rotate(root.right)
            return AVLTree._left_rotate(root)
        else:
            AVLTree._update_height(root)
            return root

    @staticmethod
    def _get_balance(root):
        return AVLTree._get_height(root.left) - AVLTree._get_height(root.right)

    @staticmethod
    def _get_height(node):
        if not node:
            return 0
        else:
            return node.height

    @staticmethod
    def _update_height(node):
        if node:
            node.height = max(AVLTree._get_height(node.left), AVLTree._get_height(node.right)) + 1

    @staticmethod
    def _left_rotate(root):
        right = root.right
        root.right = right.left
        right.left = root
        AVLTree._update_height(root)
        AVLTree._update_height(right)
        return right

    @staticmethod
    def _right_rotate(root):
        left = root.left
        root.left = left.right
        left.right = root
        AVLTree._update_height(root)
        AVLTree._update_height(left)
        return left

    @staticmethod
    def _get_min_node(root):
        while root.left:
            root = root.left
        return root

    @staticmethod
    def _find_greater_than(root, val, lst):
        if root:
            if root.val > val:
                AVLTree._find_greater_than(root.left, val, lst)
                lst.append(root)
            AVLTree._find_greater_than(root.right, val, lst)

    @staticmethod
    def _find_smaller_than(root, val, lst):
        if root:
            AVLTree._find_smaller_than(root.left, val, lst)
            if root.val < val:
                lst.append(root)
                AVLTree._find_smaller_than(root.right, val, lst)

    @staticmethod
    def _find_between(root, a, b, lst):
        if root:
            if root.val <= a:
                AVLTree._find_between(root.right, a, b, lst)
            elif root.val >= b:
                AVLTree._find_between(root.left, a, b, lst)
            else:
                AVLTree._find_between(root.left, a, b, lst)
                lst.append(root)
                AVLTree._find_between(root.right, a, b, lst)


class Node:
    def __init__(self, val, key):
        self.val = val
        self.key = set(key)
        self.left = None
        self.right = None
        self.height = 1
