

class ParserResponse:

    def __init__(self):

        self.collections = None
        self.works = None


class Collection:

    def __init__(self):

        self.id = None
        self.label = None
        self.metadata = None  # dictionary of string->string
        self.works = None  # list of works


class Work:

    def __init__(self):
        self.id = None
        self.label = None
        self.work_metadata = None  # dictionary of string->string
        self.image_collection = None  # ImageCollection instance
        self.toc = None
        self.image_metadata = None
        self.flags = None
