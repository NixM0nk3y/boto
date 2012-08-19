
class Event(object):

    def __init__(self, connection=None):
        self.connection = connection
        self.message = None
        self.source_identifier = None
        self.source_type = None
        self.engine = None
        self.date = None

    def __repr__(self):
        return '"%s"' % self.message

    def startElement(self, name, attrs, connection):
        pass

    def endElement(self, name, value, connection):
        if name == 'SourceIdentifier':
            self.source_identifier = value
        elif name == 'SourceType':
            self.source_type = value
        elif name == 'Message':
            self.message = value
        elif name == 'Date':
            self.date = value
        else:
            setattr(self, name, value)
