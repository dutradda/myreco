
class FiltersSet(set):

    def __init__(self, is_inclusive=True):
        self.is_inclusive = is_inclusive
        super().__init__()

    async def update(self, session, filter_, var_value):
        filtering_keys = await filter_.get_filtering_keys(session, var_value)
        super().update(filtering_keys)
