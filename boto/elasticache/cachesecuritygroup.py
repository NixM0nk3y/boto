
"""
Represents an CacheSecurityGroup
"""
from boto.ec2.securitygroup import SecurityGroup

class CacheSecurityGroup(object):

    def __init__(self, connection=None, owner_id=None,
                 name=None, description=None):
        self.connection = connection
        self.owner_id = owner_id
        self.name = name
        self.description = description
        self.ec2_groups = []
        self.ip_ranges = []

    def __repr__(self):
        return 'CacheSecurityGroup:%s' % self.name

    def startElement(self, name, attrs, connection):
        if name == 'IPRange':
            cidr = IPRange(self)
            self.ip_ranges.append(cidr)
            return cidr
        elif name == 'EC2SecurityGroup':
            ec2_grp = EC2SecurityGroup(self)
            self.ec2_groups.append(ec2_grp)
            return ec2_grp
        else:
            return None

    def endElement(self, name, value, connection):
        if name == 'OwnerId':
            self.owner_id = value
        elif name == 'CacheSecurityGroupName':
            self.name = value
        elif name == 'Description':
            self.description = value
        elif name == 'IPRanges':
            pass
        else:
            setattr(self, name, value)

    def delete(self):
        return self.connection.delete_cachesecurity_group(self.name)

    def authorize(self, cidr_ip=None, ec2_group=None):
        """
        Add a new rule to this CacheSecurityGroup group.
        You need to pass in either a CIDR block to authorize or
        and EC2 SecurityGroup.

        @type cidr_ip: string
        @param cidr_ip: A valid CIDR IP range to authorize

        @type ec2_group: :class:`boto.ec2.securitygroup.SecurityGroup>`

        @rtype: bool
        @return: True if successful.
        """
        if isinstance(ec2_group, SecurityGroup):
            group_name = ec2_group.name
            group_owner_id = ec2_group.owner_id
        else:
            group_name = None
            group_owner_id = None
        return self.connection.authorize_cachesecurity_group(self.name,
                                                          cidr_ip,
                                                          group_name,
                                                          group_owner_id)

    def revoke(self, cidr_ip=None, ec2_group=None):
        """
        Revoke access to a CIDR range or EC2 SecurityGroup.
        You need to pass in either a CIDR block or
        an EC2 SecurityGroup from which to revoke access.

        @type cidr_ip: string
        @param cidr_ip: A valid CIDR IP range to revoke

        @type ec2_group: :class:`boto.ec2.securitygroup.SecurityGroup>`

        @rtype: bool
        @return: True if successful.
        """
        if isinstance(ec2_group, SecurityGroup):
            group_name = ec2_group.name
            group_owner_id = ec2_group.owner_id
            return self.connection.revoke_cachesecurity_group(
                self.name,
                ec2_security_group_name=group_name,
                ec2_security_group_owner_id=group_owner_id)

        # Revoking by CIDR IP range
        return self.connection.revoke_cachesecurity_group(
            self.name, cidr_ip=cidr_ip)

class IPRange(object):

    def __init__(self, parent=None):
        self.parent = parent
        self.cidr_ip = None
        self.status = None

    def __repr__(self):
        return 'IPRange:%s' % self.cidr_ip

    def startElement(self, name, attrs, connection):
        pass

    def endElement(self, name, value, connection):
        if name == 'CIDRIP':
            self.cidr_ip = value
        elif name == 'Status':
            self.status = value
        else:
            setattr(self, name, value)

class EC2SecurityGroup(object):

    def __init__(self, parent=None):
        self.parent = parent
        self.name = None
        self.owner_id = None

    def __repr__(self):
        return 'EC2SecurityGroup:%s' % self.name

    def startElement(self, name, attrs, connection):
        pass

    def endElement(self, name, value, connection):
        if name == 'EC2SecurityGroupName':
            self.name = value
        elif name == 'EC2SecurityGroupOwnerId':
            self.owner_id = value
        else:
            setattr(self, name, value)

