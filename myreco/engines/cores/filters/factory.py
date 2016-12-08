# MIT License

# Copyright (c) 2016 Diogo Dutra

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from myreco.engines.cores.filters.filters import (BooleanFilterBy, SimpleFilterBy, ObjectFilterBy,
    ArrayFilterBy, SimpleFilterOf, ObjectFilterOf,
    ArrayFilterOf, IndexFilterOf, IndexFilterByPropertyOf)


class FiltersFactory(object):

    @classmethod
    def make(cls, items_model, engine_variable, schema, skip_values=None):
        filter_type = schema['type']
        var_name = engine_variable['inside_engine_name']
        type_ = engine_variable['filter_type']
        is_inclusive = engine_variable['is_inclusive_filter']
        id_names = None

        if type_ == 'Index Of':
            filter_class = IndexFilterOf

        elif type_ == 'Index By Property Of':
            filter_class = IndexFilterByPropertyOf

        elif filter_type == 'integer' or filter_type == 'string':
            if type_ == 'By Property':
                filter_class = SimpleFilterBy
            else:
                filter_class = SimpleFilterOf

        elif filter_type == 'boolean':
            filter_class = BooleanFilterBy

        elif filter_type == 'object':
            id_names = schema['id_names']
            if type_ == 'By Property':
                filter_class = ObjectFilterBy
            else:
                filter_class = ObjectFilterOf

        elif filter_type == 'array':
            if type_ == 'By Property':
                filter_class = ArrayFilterBy
            else:
                filter_class = ArrayFilterOf

        return filter_class(items_model, var_name, is_inclusive, id_names, skip_values)
