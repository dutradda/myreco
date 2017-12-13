from myreco.engine_strategies.filters.apply_filters cimport apply_all_filters


cpdef _build_recommendations(
        object redis_bind,
        list recos,
        dict filters
    ):
    cdef int i = 0
    cdef list filtered_recos = []

    for i in range(len(recos)):
        item_key, score = recos[i]

        if apply_all_filters(redis_bind, item_key, filters):
            filtered_recos.append((item_key, int(score)))

    return filtered_recos
