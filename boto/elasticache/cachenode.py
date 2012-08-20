# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

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
