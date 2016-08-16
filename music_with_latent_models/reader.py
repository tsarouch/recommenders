from config import user_artist_data, artist_data, artist_alias

def read_user_artist_data(sc):
    """
    Reads the user artist data
    The total number of rows of the user-artist data is 24296858
    too big for a single machine test - we ll sample
    after the sampling it becomes 2442
    
    data example:
    1000002 1 55
    1000002 1000006 33
    1000002 1000007 8
    """
    data_rdd = sc.textFile(user_artist_data, 2)
    fraction = 0.0001
    seed = 42
    with_replacement = True
    return data_rdd.sample(with_replacement, fraction, seed=seed)

    
    
def read_artist_id_map(sc):
    """ 
    Reads the artist id-name data
    data example:
    1134999    06Crazy Life
    6821360    Pang Nakarin
    10113088   Terfel, Bartoli- Mozart: Don
    6826647    Bodenstandig 3000
    """
    def _parse_pair(pair, delimiter, id_name=False):
        pair_parts = pair.rsplit(delimiter)
        if len(pair_parts) != 2:
            return []
        try:
            return [(int(pair_parts[0]), pair_parts[1])]
        except:
            return []
    artist_name_rdd = sc.textFile(artist_data)
    return dict(artist_name_rdd\
                .flatMap(lambda x: _parse_pair(x, '\t'))\
                .collect())

 
def read_artist_alias_map(sc):
    """ 
    Maps artist IDs that are known misspellings or variants to the canonical ID of the artist
    data example:
    1092764	1000311
    1095122	1000557
    6708070	1007267
    """
    def _parse_pair(pair, delimiter, id_name=False):
        pair_parts = pair.rsplit(delimiter)
        if len(pair_parts) != 2:
            return []
        try:
            return [(int(pair_parts[0]), int(pair_parts[1]))]
        except:
            return []

    artist_alias_rdd = sc.textFile(artist_alias)
    return artist_alias_rdd\
                    .flatMap(lambda x: _parse_pair(x, '\t'))\
                    .collectAsMap()


