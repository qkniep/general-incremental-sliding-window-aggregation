import math


class FlatFAT:
    size = -1  # amount of nodes in entire tree
    capacity = -1  # amount of leafs
    largestLocation = -1  # location of last not-none leaf

    tree = None
    values = None  # quicker access to leafs -> useful for resizing
    operator = None

    # F = FlatFAT([(3, 4), (6, 2), (8, 1), (5, 3), (1, 4), (4, 1)], "operator")
    def __init__(self, values, operator):
        self.capacity = int(math.ceil(math.log2(len(values))))
        self.capacity = int(math.pow(2, self.capacity))

        self.values = values

        self.size = (2 * self.capacity) - 1
        self.tree = [None] * self.size
        self.values = [None] * self.capacity
        self.largestLocation = len(values)-1

        self.operator = operator

        self.__newStructure(values)

    def __newStructure(self, values):
        start = self.size - self.capacity
        # write leafs
        for i in range(start, start + len(values)):
            self.tree[i] = values[i-start]
            self.values[i-start] = values[i-start]

        # find intermediate states
        for i in range(self.size-1, 0, -2):
            parent = self.__getParent(i)
            combine_result = self.__combine(self.tree[i], self.tree[i-1])
            self.tree[parent] = combine_result

    def __resize(self, size, type):
        capacity = int(math.ceil(math.log2(size)))
        capacity = int(math.pow(2, capacity))
        values_tmp = [None] * capacity

        # create new values array of correct size
        if type == 0:  # enlarge
            for i in range(0, self.capacity):
                values_tmp[i] = self.values[i]
        else:  # shrink
            for i in range(0, capacity):
                values_tmp[i] = self.values[i]

        # create new instance of FlatFat
        tmp_tree = FlatFAT(values_tmp, self.operator)

        # overwrite instance variables of self
        self.tree = tmp_tree.tree
        self.capacity = tmp_tree.capacity
        self.values = tmp_tree.values
        self.size = tmp_tree.size

    def __getParent(self, i):
        parent_node = None
        if i % 2 == 0:
            parent_node = int((i/2)-1)
        else:
            parent_node = int(i/2)

        return parent_node

    def __getLeftChild(self, i):
        return (i * 2) + 1

    def __getRightChild(self, i):
        return (i * 2) + 2

    def __combine(self, v, w):
        if v is None and w is None:
            return None
        elif v is None and w is not None:
            return w
        elif v is not None and w is None:
            return v
        else:
            # for testing purposes only:
            # assumption: v,w is of type (id, value, ...)
            max = None
            if v[1] > w[1]:
                max = v
            else:  # if equal return first -> w
                max = w
            return max
            # TODO return self.operator.combine(v, w)

    def __getMaxIndex(self):
        for i in range(int(len(self.values)/2+1), 0, -1):
            if self.values[i] is not None:
                self.largestLocation = i
                break

    # assumption: value is a list of tuples: [(location, (id, value, ..)), ..]
    # eg value = [(4, None), (15, (1, 9)), (3, (9, 11))]
    def update(self, value):
        none = False
        update_queue = list()
        # update all provided leaf locations in the tree
        for i in range(0, len(value)):
            # update largest not none location
            if value[i][0] > self.largestLocation:
                self.largestLocation = value[i][0]
            # resize array if not big enough
            if value[i][0] >= self.capacity:
                self.__resize(value[i][0], 0)

            # update leaf nodes in tree and value array
            location = self.size - self.capacity + value[i][0]
            self.tree[location] = value[i][1]
            self.values[value[i][0]] = value[i][1]
            update_queue.append(self.__getParent(location))

            # check if shrinking is necessary
            if value[i][1] is None:
                self.__getMaxIndex()
                if self.largestLocation < int(self.capacity/2):
                    # remeber to shrink after updating intermediate states
                    none = True

        # update intermediate states
        for i in update_queue:
            l_child = self.tree[self.__getLeftChild(i)]
            r_child = self.tree[self.__getRightChild(i)]
            children_combined = self.__combine(l_child, r_child)

            if (
                self.__combine(self.tree[i], children_combined)
                == children_combined
            ):
                self.tree[i] = children_combined
                parent_node = self.__getParent(i)
                if i == 0:
                    break
                else:
                    update_queue.append(parent_node)

        if none is True:  # shrink tree
            self.__resize(self.largestLocation, 1)

    def aggregate(self):
        return self.tree[0]

    # TODO no new tree but O(log)?
    def prefix(self, i):
        prefix = self.values[0:i]
        prefix_tree = FlatFAT(prefix, self.operator)

        return prefix_tree.tree[0]

    # TODO no new tree but O(log)?
    def suffix(self, j):
        suffix = self.values[j:]
        suffix_tree = FlatFAT(suffix, self.operator)

        return suffix_tree.tree[0]
