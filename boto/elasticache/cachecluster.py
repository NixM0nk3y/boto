
from boto.elasticache.cachesecuritygroup import CacheSecurityGroup
from boto.elasticache.cachenode import CacheNode
from boto.elasticache.parametergroup import ParameterGroup
from boto.resultset import ResultSet

class CacheCluster(object):
    """
    Represents a ElasticCache Cache Cluster
    """
    
    def __init__(self, connection=None, id=None):
        self.connection = connection
        self.id = id
        self.create_time = None
        self.engine = None
        self.engine_version = None
        self.status = None
        self.cache_node_type = None
        self.parameter_group = None
        self.security_groups = None
        self.availability_zone = None
        self.num_cache_nodes = None
        self.cache_nodes = None
        self.minor_version_upgrade = False
        self.preferred_maintenance_window = None
        self.pending_modified_values = None
        self.topic_status = None
        self.topic_arn = None

    def __repr__(self):
        return 'CacheCluster:%s' % self.id

    def startElement(self, name, attrs, connection):
        if name == 'CacheParameterGroup':
            self.parameter_group = ParameterGroup(self.connection)
            return self.parameter_group
        elif name == 'CacheSecurityGroups':
	    self.security_groups = ResultSet([('CacheSecurityGroup', CacheSecurityGroup)])
            return self.security_groups
        elif name == 'CacheNodes':
            self.cache_nodes = ResultSet([('CacheNode', CacheNode)])
            return self.cache_nodes
        elif name == 'PendingModifiedValues':
            self.pending_modified_values = PendingModifiedValues()
            return self.pending_modified_values
        return None

    def endElement(self, name, value, connection):
        if name == 'CacheClusterId':
            self.id = value
        elif name == 'CacheClusterStatus':
            self.status = value
        elif name == 'CacheClusterCreateTime':
            self.create_time = value
        elif name == 'Engine':
            self.engine = value
        elif name == 'EngineVersion':
            self.engine_version = value
        elif name == 'CacheNodeType':
            self.cache_node_type = value
        elif name == 'PreferredAvailabilityZone':
            self.availability_zone = value
        elif name == 'NumCacheNodes':
            self.num_cache_nodes = value
        elif name == 'TopicStatus':
            self.topic_status = value
        elif name == 'TopicArn':
            self.topic_arn = value
        elif name == 'AutoMinorVersionUpgrade':
            if value.lower() == 'true':
                self.minor_version_upgrade = True
        elif name == 'PreferredMaintenanceWindow':
            self.preferred_maintenance_window = value
        else:
            setattr(self, name, value)

    def update(self, validate=False):
        """
        Update the Cache cluster's status information by making a call to fetch
        the current instance attributes from the service.

        :type validate: bool
        :param validate: By default, if EC2 returns no data about the
                         instance the update method returns quietly.  If
                         the validate param is True, however, it will
                         raise a ValueError exception if no data is
                         returned from EC2.
        """
        rs = self.connection.get_all_cacheclusters(self.id)
        if len(rs) > 0:
            for i in rs:
                if i.id == self.id:
                    self.__dict__.update(i.__dict__)
        elif validate:
            raise ValueError('%s is not a valid Instance ID' % self.id)
        return self.status

    def stop(self):
        """
        Delete this Cache Cluster.

        :rtype: :class:`boto.elasticache.cachecluster.CacheCluster`
        :return: The deleted cache cluster.
        """
        return self.connection.delete_cachecluster(self.id)

class PendingModifiedValues(dict):

    def startElement(self, name, attrs, connection):
        return None

    def endElement(self, name, value, connection):
        if name != 'PendingModifiedValues':
            self[name] = value
