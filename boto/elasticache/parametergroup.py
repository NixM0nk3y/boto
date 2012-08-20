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

from boto.resultset import ResultSet

class ParameterGroup(object):

    def __init__(self, connection=None):
        self.connection = connection
        self.name = None
        self.description = None
        self.family = None
	self.parameters = []
	self.typespecific_parameters = []

    def __repr__(self):
        return 'ParameterGroup:%s' % self.name

    def startElement(self, name, attrs, connection):
        if name == 'Parameters':
            self.parameters = ResultSet([('Parameter', Parameter)])
            return self.parameters
        elif name == 'CacheNodeTypeSpecificParameters':
            self.typespecific_parameters = ResultSet([('CacheNodeTypeSpecificParameter', TypeSpecificParameter)])
            return self.typespecific_parameters
        else:
            return None

    def endElement(self, name, value, connection):
        if name == 'CacheParameterGroupName':
            self.name = value
        elif name == 'Description':
            self.description = value
        elif name == 'CacheParameterGroupFamily':
            self.family = value
        else:
            setattr(self, name, value)

    def modifiable(self):
        mod = []
        for p in self.parameters:
            if p.is_modifiable:
                mod.append(p)
        return mod

    def get_params(self):
        self.parameters = self.connection.get_all_cacheparameters(self.name)
        return self.parameters

    def add_param(self, name, value, apply_method):
        param = Parameter()
        param.name = name
        param.value = value
        param.apply_method = apply_method
        self.parameters.append(param)

class Parameter(object):
    """
    Represents a Cache Parameter
    """

    ValidTypes = {'integer' : int,
                  'string' : str,
                  'boolean' : bool}
    ValidSources = ['user', 'system', 'engine-default']

    def __init__(self, name=None):
        self.name = name
        self._value = None
        self.type = 'string'
        self.source = None
        self.is_modifiable = True
        self.description = None
        self.minimum_engine_version = None
        self.allowed_values = None

    def __repr__(self):
        return 'Parameter:%s' % self.name

    def startElement(self, name, attrs, connection):
        return None

    def endElement(self, name, value, connection):
        if name == 'ParameterName':
            self.name = value
        elif name == 'ParameterValue':
            self._value = value
        elif name == 'DataType':
            if value in self.ValidTypes:
                self.type = value
        elif name == 'Source':
            if value in self.ValidSources:
                self.source = value
        elif name == 'IsModifiable':
            if value.lower() == 'true':
                self.is_modifiable = True
            else:
                self.is_modifiable = False
        elif name == 'Description':
            self.description = value
        elif name == 'MinimumEngineVersion':
            self.minimum_engine_version = value
        elif name == 'AllowedValues':
            self.allowed_values = value
        else:
            setattr(self, name, value)

    def merge(self, d, i):
        prefix = 'Parameters.member.%d.' % i
        if self.name:
            d[prefix+'ParameterName'] = self.name
        if self._value is not None:
            d[prefix+'ParameterValue'] = self._value

    def _set_string_value(self, value):
        if not isinstance(value, str) or isinstance(value, unicode):
            raise ValueError('value must be of type str')
        if self.allowed_values:
            choices = self.allowed_values.split(',')
            if value not in choices:
                raise ValueError('value must be in %s' % self.allowed_values)
        self._value = value

    def _set_integer_value(self, value):
        if isinstance(value, str) or isinstance(value, unicode):
            value = int(value)
        if isinstance(value, int) or isinstance(value, long):
            if self.allowed_values:
                min, max = self.allowed_values.split('-')
                if value < int(min) or value > int(max):
                    raise ValueError('range is %s' % self.allowed_values)
            self._value = value
        else:
            raise ValueError('value must be integer')

    def _set_boolean_value(self, value):
        if isinstance(value, bool):
            self._value = value
        elif isinstance(value, str) or isinstance(value, unicode):
            if value.lower() == 'true':
                self._value = True
            else:
                self._value = False
        else:
            raise ValueError('value must be boolean')

    def set_value(self, value):
        if self.type == 'string':
            self._set_string_value(value)
        elif self.type == 'integer':
            self._set_integer_value(value)
        elif self.type == 'boolean':
            self._set_boolean_value(value)
        else:
            raise TypeError('unknown type (%s)' % self.type)

    def get_value(self):
        if self._value == None:
            return self._value
        if self.type == 'string':
            return self._value
        elif self.type == 'integer':
            if not isinstance(self._value, int) and not isinstance(self._value, long):
                self._set_integer_value(self._value)
            return self._value
        elif self.type == 'boolean':
            if not isinstance(self._value, bool):
                self._set_boolean_value(self._value)
            return self._value
        else:
            raise TypeError('unknown type (%s)' % self.type)

    value = property(get_value, set_value, 'The value of the parameter')

    def apply(self, group):
        self.group.connection.modify_parameter_group(group.name, [self])

class TypeSpecificParameter(object):
    """
    Represents a Cache  TypeSpecific Parameter
    """

    ValidTypes = {'integer' : int,
                  'string' : str,
                  'boolean' : bool}

    ValidSources = ['user', 'system', 'engine-default']

    def __init__(self, name=None):
        self.name = name
        self._value = {}
        self.type = 'string'
        self.source = None
        self.is_modifiable = True
        self.description = None
        self.minimum_engine_version = None
        self.allowed_values = None
        self._in_specific_value = False
        self._spec_type = None
        self._spec_value = None

    def __repr__(self):
        return 'TypeSpecificParameter:%s' % self.name

    def startElement(self, name, attrs, connection):
        if name == 'CacheNodeTypeSpecificValue':
            self._in_specific_value = True
        return None

    def endElement(self, name, value, connection):
        if name == 'ParameterName':
            self.name = value
        elif name == 'DataType':
            if value in self.ValidTypes:
                self.type = value
        elif name == 'Source':
            if value in self.ValidSources:
                self.source = value
        elif name == 'IsModifiable':
            if value.lower() == 'true':
                self.is_modifiable = True
            else:
                self.is_modifiable = False
        elif name == 'Description':
            self.description = value
        elif name == 'MinimumEngineVersion':
            self.minimum_engine_version = value
        elif name == 'AllowedValues':
            self.allowed_values = value
        elif name == 'Value':
            if self._in_specific_value:
                self._spec_value = value
        elif name == 'CacheNodeType':
            if self._in_specific_value:
                self._spec_type = value
        elif name == 'CacheNodeTypeSpecificValue':
            self._value[ self._spec_type ] = self._spec_value
            self._in_specific_value = False
        else:
            setattr(self, name, value)

