import json


class JSONLDBase(object):

    def __init__(self):

        self.context = None
        self.at_id = None
        self.id = None
        self.type = None

    def to_json_dict(self):

        data = {}

        add_if_not_none(data, '@context', self.context)
        add_if_not_none(data, '@id', self.at_id)
        add_if_not_none(data, 'id', self.id)
        add_if_not_none(data, '@type', self.type)

        return data

    def as_json(self):

        return json.dumps(self.to_json_dict())


class JSONLDBaseWithHydraContext(JSONLDBase):

    def __init__(self):

        super(JSONLDBaseWithHydraContext, self).__init__()
        self.include_context = True
        self.context = "http://www.w3.org/ns/hydra/context.jsonld"

    def to_json_dict(self):

        data = super(JSONLDBaseWithHydraContext, self).to_json_dict()
        if self.include_context:
            data['@context'] = self.context
        else:
            data['@context'] = None

        return data


class ImageCollection(JSONLDBaseWithHydraContext):

    def __init__(self, images=None):

        super(ImageCollection, self).__init__()
        self.type = "Collection"
        self.members = None
        if images is not None:
            self.members = images

    @property
    def total_items(self):

        if self.members is None:
            return 0
        return len(self.members)

    def to_json_dict(self):

        data = super(ImageCollection, self).to_json_dict()
        add_if_not_none(data, 'member', map(lambda x: x.to_json_dict(), self.members))
        add_if_not_none(data, 'total_items', self.total_items)

        return data


class Image(JSONLDBase):

    def __init__(self, id=None, at_id=None, space=None, origin=None, tags=None, string_1=None, string_2=None, number_1=None, number_2=None):
        super(Image, self).__init__()
        self.id = id
        self.at_id = at_id
        self.space = space
        self.origin = origin
        self.tags = tags
        self.string_1 = string_1
        self.string_2 = string_2
        self.number_1 = number_1
        self.number_2 = number_2

    def to_json_dict(self):

        data = super(Image, self).to_json_dict()
        add_if_not_none(data, 'space', self.space)
        add_if_not_none(data, 'origin', self.origin)
        add_if_not_none(data, 'tags', self.tags)
        add_if_not_none(data, 'string1', self.string_1, str)
        add_if_not_none(data, 'string2', self.string_2, str)
        add_if_not_none(data, 'number1', self.number_1, int)
        add_if_not_none(data, 'number2', self.number_2, int)
        return data


def add_if_not_none(dictionary, key, value, wrap=None):

    if value is not None:
        if wrap is not None:
            value = wrap(value)
        dictionary[key] = value
