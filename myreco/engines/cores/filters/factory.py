# MIT License

# Copyright (c) 2016 Diogo Dutra <dutradda@gmail.com>

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
    _filters_types_map = {
        'property_value': {
            'name': 'Filter by property value',
            'types': {
                'integer': SimpleFilterBy,
                'string': SimpleFilterBy,
                'object': ObjectFilterBy,
                'array': ArrayFilterBy,
                'boolean': BooleanFilterBy
            }
        },
        'item_property_value': {
            'name': 'Filter by item property value',
            'types': {
                'integer': SimpleFilterOf,
                'string': SimpleFilterOf,
                'object': ObjectFilterOf,
                'array': ArrayFilterOf,
                'boolean': BooleanFilterBy
            }
        },
        'property_value_index': {
            'name': 'Filter by property value index',
            'types': {
                'integer': IndexFilterOf,
                'string': IndexFilterOf,
                'object': IndexFilterOf,
                'array': IndexFilterOf,
                'boolean': IndexFilterOf
            }
        },
        'item_property_value_index': {
            'name': 'Filter by item property value index',
            'types': {
                'integer': IndexFilterByPropertyOf,
                'string': IndexFilterByPropertyOf,
                'object': IndexFilterByPropertyOf,
                'array': IndexFilterByPropertyOf,
                'boolean': IndexFilterByPropertyOf
            }
        }
    }

    @classmethod
    def get_filter_types(cls):
        return [{'name': filter_type['name'], 'id': filter_type_id}
            for filter_type_id, filter_type in cls._filters_types_map.items()]

    @classmethod
    def get_filter_type(cls, filter_type_id):
        filter_type = cls._filters_types_map.get(filter_type_id)
        return {
            'name': filter_type['name'],
            'id': filter_type_id
        } if filter_type else None

    @classmethod
    def make(cls, items_model, slot_filter, schema, skip_values=None):
        value_type = schema['type']
        filter_name = slot_filter['property_name']
        type_id = slot_filter['type_id']
        is_inclusive = slot_filter['is_inclusive']
        id_names = schema.get('id_names')
        filter_class = cls._filters_types_map.get(type_id, {'types': {}})['types'].get(value_type)

        if filter_class:
            return filter_class(items_model, filter_name, is_inclusive, id_names, skip_values)
