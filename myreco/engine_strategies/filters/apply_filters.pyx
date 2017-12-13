
cdef apply_all_filters(object redis_bind, bytes item_key, dict filters):
    return (len(filters['inclusive']) == 0 or _apply_filters(redis_bind, item_key, 1, filters['inclusive'])) and \
                (len(filters['exclusive']) == 0 or not _apply_filters(redis_bind, item_key, 0, filters['exclusive']))


cdef _apply_filters(object redis_bind, bytes item_key, int is_inclusive, list filters_keys):
    cdef tuple filter_
    cdef list redis_args = list()
    cdef int i = 0

    for i in range(len(filters_keys)):
        redis_args.append(
            ('sismember', filters_keys[i], item_key)
        )

    filter_ = redis_bind.execute_pipeline(*redis_args)

    if is_inclusive:
        return all(filter_)
    else:
        return any(filter_)
