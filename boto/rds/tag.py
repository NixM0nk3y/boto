#
#
#
#

class Tag(object):

    """
    A Tag is used when creating or listing all tags related to
    an AWS account.  It records not only the key and value but
    also the ID of the resource to which the tag is attached
    as well as the type of the resource.
    """

    def __init__(self, connection=None, key=None, value=None,
                 resource_id=None,
                 resource_type=None):
        self.connection = connection
        self.key = key
        self.value = value
        self.resource_id = resource_id
        self.resource_type = resource_type

    def __repr__(self):
        return 'Tag(%s=%s)' % (self.key, self.value)

    def startElement(self, name, attrs, connection):
        pass

    def endElement(self, name, value, connection):
        if name == 'Key':
            self.key = value
        elif name == 'Value':
            self.value = value
        elif name == 'ResourceId':
            self.resource_id = value
        elif name == 'ResourceType':
            self.resource_type = value

    def build_params(self, params, i):
        """
        Populates a dictionary with the name/value pairs necessary
        to identify this Tag in a request.
        """
        prefix = 'Tags.member.%d.' % i
        if self.resource_id:
            params[prefix+'ResourceId'] = self.resource_id
        if self.resource_type:
            params[prefix+'ResourceType'] = self.resource_type
        params[prefix+'Key'] = self.key
        params[prefix+'Value'] = self.value

    def build_keyparams(self, params, i):
        """
        Populates a list with the names necessary
        to identify this Tag in a request.
        """
        #prefix = 'TagKeys.member.%d.' % i
        #params[prefix+'Key'] = self.key
        prefix = 'TagKeys.member.%d' % i
        params[prefix] = self.key

    def delete(self):
        return self.connection.delete_tags([self])
