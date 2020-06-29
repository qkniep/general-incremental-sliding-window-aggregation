from FlatFAT import FlatFAT


def main():
    init_values = [
                   (3, 4, 7, "hello", 543.42342), (6, 2, 3, "test", 74.234),
                   (8, 1, 3, "lallla", 7632345), (5, 3, 2, "stuff", 43151),
                   (1, 4, 1, "more stuff", 724616),
                   (4, 1, 932, "even more stuff", 14316464)
                  ]
    a = FlatFAT(init_values, "operator")

    print("original tree: ", a.tree)
    print()

    update_values = [(9, (4, 17, "update 1", 68732)),
                     (3, (9, 11, "update 2", 463824)), (2, (None))]
    a.update(update_values)
    print("updated tree: ", a.tree)
    print()

    update_values_2 = [(81, (34, 854, "delete test", 563782))]
    a.update(update_values_2)
    print(a.tree)
    print()

    update_values_3 = [(81, (None))]
    a.update(update_values_3)
    print(a.tree)

    update_values_4 = [(9, (None))]
    a.update(update_values_4)
    print(a.tree)

    ########################################################################
    # update intermediate states (OLD)
    #
    # for i in range(0, len(value)):
    #     location = self.size - self.capacity + value[i][0]
    #     parent_node = self.__getParent(location)
    #     # follow path until parent is bigger, or until root
    #     while(self.__combine(self.tree[location], self.tree[parent_node])
    #             == self.tree[location] and location > 0):
    #         combine_result = self.__combine(self.tree[location],
    #                                       self.tree[parent_node])
    #         if combine_result == self.tree[location]:
    #             self.tree[parent_node] = combine_result
    #
    #         old_parent = parent_node
    #         parent_node = self.__getParent(old_parent)
    #         location = old_parent
    #
    ########################################################################

main()
