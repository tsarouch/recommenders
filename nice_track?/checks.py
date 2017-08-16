
def ids_requirements_satisfied(data_rdd):
    """
    MLlib ALS algorithm that we 'll use here, requires that ids for users/items are numeric nonegative and 32-bit.
    Check quickly with stats / max
    The maximum positive value for a 32-bit signed binary integer in computing is 2147483647
    """
    bit32_max_num = 2147483647
    return (data_rdd.map(lambda x: float(x.split()[0])).max() < bit32_max_num) & \
        (data_rdd.map(lambda x: float(x.split()[1])).max() < bit32_max_num)
