

class CacheNode(object):
    """
    Represents a ElasticCache Cache Cluster Node
    """
    
    def __init__(self, connection=None, id=None):
        self.connection = connection
        self.id = id
        self.create_time = None
        self.status = None
        self.endpoint = None
        self._in_endpoint = False
        self._port = None
        self._address = None
        self.parameter_group_status = None

    def __repr__(self):
        return 'CacheNode:%s' % self.id

    def startElement(self, name, attrs, connection):
        if name == 'Endpoint':
            self._in_endpoint = True
        return None

    def endElement(self, name, value, connection):
        if name == 'CacheNodeId':
            self.id = value
        elif name == 'CacheNodeCreateTime':
            self.create_time = value
        elif name == 'CacheNodeStatus':
            self.status = value
        elif name == 'Port':
            if self._in_endpoint:
                self._port = int(value)
        elif name == 'Address':
            if self._in_endpoint:
                self._address = value
        elif name == 'Endpoint':
            self.endpoint = (self._address, self._port)
            self._in_endpoint = False
        elif name == 'ParameterGroupStatus':
            self.parameter_group_status = value
        else:
            setattr(self, name, value)
