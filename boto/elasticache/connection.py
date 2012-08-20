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

from boto.connection import AWSQueryConnection
from boto.regioninfo import RegionInfo
from boto.elasticache.cachecluster import CacheCluster
from boto.elasticache.cachenode import CacheNode
from boto.elasticache.event import Event
from boto.elasticache.cachesecuritygroup import CacheSecurityGroup
from boto.elasticache.parametergroup import ParameterGroup

import boto
import uuid
try:
    import simplejson as json
except ImportError:
    import json


class ElastiCacheConnection(AWSQueryConnection):

    DefaultRegionName = 'us-east-1'
    DefaultRegionEndpoint = 'elasticache.us-east-1.amazonaws.com'
    APIVersion = '2012-03-09'

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, region=None, path='/',
                 security_token=None):
        if not region:
            region = RegionInfo(self, self.DefaultRegionName,
                                self.DefaultRegionEndpoint,
                                connection_cls=ElastiCacheConnection)
        self.region = region
        AWSQueryConnection.__init__(self, aws_access_key_id,
                                    aws_secret_access_key,
                                    is_secure, port, proxy, proxy_port,
                                    proxy_user, proxy_pass,
                                    self.region.endpoint, debug,
                                    https_connection_factory, path,
                                    security_token=security_token)

    def _required_auth_capability(self):
        return ['elasticache']

    def _credentials_expired(self, response):
        if response.status != 403:
            return False
        try:
            parsed = json.loads(response.read())
            return parsed['Error']['Code'] == 'ExpiredToken'
        except Exception:
            return False
        return False

    def get_all_cacheclusters(self, cluster_id=None, max_records=None,
                            marker=None ):
        """
        Retrieve all the CacheClusters in your account.

        :type cluster_id: str
        :param cluster_id: Cluster identifier.  If supplied, only
                            information this instance will be returned.
                            Otherwise, info about all DB Instances will
                            be returned.

        :type max_records: int
        :param max_records: The maximum number of records to be returned.
                            If more results are available, a MoreToken will
                            be returned in the response that can be used to
                            retrieve additional records.  Default is 100.

        :type marker: str
        :param marker: The marker provided by a previous request.

        :rtype: list
        :return: A list of :class:`boto.elasticache.cachecluster.CacheCluster`
        """
        params = {}

        # always pull the node info
        params['ShowCacheNodeInfo'] = 'true'

        if cluster_id:
            params['CacheClusterId'] = cluster_id
        if max_records:
            params['MaxRecords'] = max_records
        if marker:
            params['Marker'] = marker
        return self.get_list('DescribeCacheClusters', params,
                             [('CacheCluster', CacheCluster)])

    def create_cachecluster(self,
                          id,
                          cluster_class,
                          port=11211,
                          num_cache_nodes=1,
                          engine='memcached',
                          param_group=None,
                          security_groups=None,
                          availability_zone=None,
                          notification_arn=None,
                          preferred_maintenance_window=None,
                          engine_version=None,
                          auto_minor_version_upgrade=True,
                          ):
        # API version: 2012-03-09
        # Parameter notes:
        # =================
        #
        """
        Create a new CacheCluster.

        :type id: str
        :param id: Unique identifier for the new instance.
                   Must contain 1-63 alphanumeric characters.
                   First character must be a letter.
                   May not end with a hyphen or contain two consecutive hyphens

        :type cluster_class: str
        :param cluster_class: The compute and memory capacity of
                               the CacheCluster. Valid values are:

                               * cache.m1.small
                               * cache.m1.large
                               * cache.m2.xlarge
                               * cache.m2.2xlarge
                               * cache.m2.4xlarge
                               * cache.c1.xlarge 

        :type engine: str
        :param engine: Name of cache engine. Defaults to memcachedL but can be;

                       * memcached

        :type port: int
        :param port: Port number on which cache accepts connections.
                     Valid values [1115-65535].

                     * Memcache defaults to 11211

        :type num_cache_nodes: int
        :param num_cache_nodes:  Specifies the number of Cache Nodes the Cache Cluster contains. 
                     Valid values [1-24].

        :type param_group: str
        :param param_group: Name of CacheParameterGroup to associate with
                            this CacheInstance.  If no groups are specified
                            no parameter groups will be used.

        :type security_groups: list of str or list of CacheSecurityGroup objects
        :param security_groups: List of names of CacheSecurityGroup to
            authorize on this CacheInstance.

        :type availability_zone: str
        :param availability_zone: Name of the availability zone place cluster

        :type notification_arn: str
        :param notification_arn:  The Amazon Resource Name (ARN) of the Amazon Simple 
                                  Notification Service (SNS) topic to which notifications
                                  will be sent. 

        :type preferred_maintenance_window: str
        :param preferred_maintenance_window: The weekly time range (in UTC)
                                             during which maintenance can occur.
                                             Default is Sun:05:00-Sun:09:00

        :type engine_version: str
        :param engine_version: The version number of the cache engine to use.

                               * memcache format example: 1.4.5

        :type auto_minor_version_upgrade: bool
        :param auto_minor_version_upgrade: Indicates that minor engine
                                           upgrades will be applied
                                           automatically to the Read Replica
                                           during the maintenance window.
                                           Default is True.

        :rtype: :class:`boto.elasticache.cacheinstance.CacheInstance`
        :return: The new cache instance.
        """
        # boto argument alignment with AWS API parameter names:
        # =====================================================
        #
        params = {
                  'AutoMinorVersionUpgrade': str(auto_minor_version_upgrade).lower() if auto_minor_version_upgrade else None,
                  'PreferredAvailabilityZone': availability_zone,
                  'CacheNodeType': cluster_class,
                  'CacheClusterId': id,
                  'NumCacheNodes': num_cache_nodes,
                  'NotificationTopicArn': notification_arn,
                  'CacheParameterGroup': param_group,
                  'Engine': engine,
                  'EngineVersion': engine_version,
                  'Port': port,
                  'PreferredMaintenanceWindow': preferred_maintenance_window,
                  }
        if security_groups:
            l = []
            for group in security_groups:
                if isinstance(group, CacheSecurityGroup):
                    l.append(group.name)
                else:
                    l.append(group)
            self.build_list_params(params, l, 'CacheSecurityGroups.member')

        # Remove any params set to None
        for k, v in params.items():
          if not v: del(params[k])

        return self.get_object('CreateCacheCluster', params, CacheCluster)

    def modify_cachecluster(self, id, param_group=None, security_groups=None,
                          preferred_maintenance_window=None,
                          engine_version=None,
                          notification_topic_arn=None,
                          notification_topic_status=None,
                          preferred_backup_window=None,
                          num_cache_nodes=None,
                          cache_nodeids_to_remove=None,
                          auto_minor_version_upgrade=None,
                          apply_immediately=False):
        """
        Modify an existing CacheCluster. 

        :type id: str
        :param id: Unique identifier for the cluster.

        :type security_groups: list of str or list of CacheSecurityGroup objects
        :param security_groups: List of names of CacheSecurityGroup to authorize on
                                this CacheCluster.

        :type cache_nodeids_to_remove: list of str or list of CacheNode objects
        :param cache_nodeids_to_remove: List of names of CacheNodes to removes from 
                                this CacheCluster.

        :type preferred_maintenance_window: str
        :param preferred_maintenance_window: The weekly time range (in UTC)
                                             during which maintenance can
                                             occur.
                                             Default is Sun:05:00-Sun:09:00

        :type num_cache_nodes: int
        :param num_cache_nodes:  Specifies the number of Cache Nodes the Cache Cluster contains.
                                 Valid values [1-24].

        :type engine_version: str
        :param engine_version: The version of the cache engine to upgrade this cluster to. 

        :type auto_minor_version_upgrade: bool
        :param auto_minor_version_upgrade:  Indicates that minor engine upgrades will be applied 
                                            automatically to the Cache Cluster during the maintenance 
                                            window. 

        :type notification_topic_arn: str
        :param notification_topic_arn: The Amazon Resource Name (ARN) of the SNS topic to which 
                                       notifications will be sent. 

        :type notification_topic_status: str
        :param notification_topic_status:  The status of the Amazon SNS notification topic. 
                                           The value can be active or inactive. Notifications 
                                           are sent only if the status is active. 

        :type apply_immediately: bool
        :param apply_immediately: If true, the modifications will be applied
                                  as soon as possible rather than waiting for
                                  the next preferred maintenance window.

        :rtype: :class:`boto.elasticache.cachecluster.CacheCluster`
        :return: The modified cache cluster.
        """
        params = {'CacheClusterId': id}
        if param_group:
            params['CacheParameterGroupName'] = param_group
        if security_groups:
            l = []
            for group in security_groups:
                if isinstance(group, CacheSecurityGroup):
                    l.append(group.name)
                else:
                    l.append(group)
            self.build_list_params(params, l, 'CacheSecurityGroupNames.member')
        if cache_nodeids_to_remove:
            l = []
            for node in cache_nodeids_to_remove:
                if isinstance(node, CacheNode):
                    l.append(node.id)
                else:
                    l.append(node)
            self.build_list_params(params, l, 'CacheNodeIdsToRemove.member')
        if preferred_maintenance_window:
            params['PreferredMaintenanceWindow'] = preferred_maintenance_window
        if notification_topic_arn:
            params['NotificationTopicArn'] = notification_topic_arn
        if notification_topic_status:
            params['NotificationTopicStatus'] = notification_topic_status
        if num_cache_nodes:
            params['NumCacheNodes'] = num_cache_nodes
        if engine_version:
            params['EngineVersion'] = engine_version
        if apply_immediately:
            params['ApplyImmediately'] = 'true'
        if auto_minor_version_upgrade:
            params['AutoMinorVersionUpgrade'] = 'true'

        return self.get_object('ModifyCacheCluster', params, CacheCluster)

    def delete_cachecluster(self, id):
        """
        Delete an existing CacheCluster.

        :type id: str
        :param id: Unique identifier for the cluster to be deleted.

        :rtype: :class:`boto.elasticache.cachecluster.CacheCluster`
        :return: The deleted cache cluster.
        """
        params = {'CacheClusterId': id}

        return self.get_object('DeleteCacheCluster', params, CacheCluster)

    def reboot_cachecluster(self, id, nodes):
        """
        Reboot CacheCluster.

        :type id: str
        :param id: Unique identifier of the instance.

        :rtype: :class:`boto.elasticache.cachecluster.CacheCluster`
        :return: The rebooting cache cluster.
        """
        params = {'CacheClusterId': id}

        l = []

        for node in nodes:
            if isinstance(node, CacheNode):
                 l.append(node.id)
            else:
                 l.append(node)

        self.build_list_params(params, l, 'CacheNodeIdsToReboot.member')

        return self.get_object('RebootCacheCluster', params, CacheCluster)

    # CacheParameterGroup methods

    def get_all_cacheparameter_groups(self, groupname=None, max_records=None,
                                  marker=None):
        """
        Get all parameter groups associated with your account in a region.

        :type groupname: str
        :param groupname: The name of the CacheParameter group to retrieve.
                          If not provided, all CacheParameter groups will be returned.

        :type max_records: int
        :param max_records: The maximum number of records to be returned.
                            If more results are available, a MoreToken will
                            be returned in the response that can be used to
                            retrieve additional records.  Default is 100.

        :type marker: str
        :param marker: The marker provided by a previous request.

        :rtype: list
        :return: A list of :class:`boto.elasticache.parametergroup.ParameterGroup`
        """
        params = {}
        if groupname:
            params['CacheParameterGroupName'] = groupname
        if max_records:
            params['MaxRecords'] = max_records
        if marker:
            params['Marker'] = marker
        return self.get_list('DescribeCacheParameterGroups', params,
                             [('CacheParameterGroup', ParameterGroup)])

    def get_all_cacheparameters(self, groupname, source=None,
                             max_records=None, marker=None):
        """
        Get all parameters associated with a ParameterGroup

        :type groupname: str
        :param groupname: The name of the CacheParameter group to retrieve.

        :type source: str
        :param source: Specifies which parameters to return.
                       If not specified, all parameters will be returned.
                       Valid values are: user|system|engine-default

        :type max_records: int
        :param max_records: The maximum number of records to be returned.
                            If more results are available, a MoreToken will
                            be returned in the response that can be used to
                            retrieve additional records.  Default is 100.

        :type marker: str
        :param marker: The marker provided by a previous request.

        :rtype: :class:`boto.elasticache.parametergroup.ParameterGroup`
        :return: The ParameterGroup
        """
        params = {'CacheParameterGroupName': groupname}
        if source:
            params['Source'] = source
        if max_records:
            params['MaxRecords'] = max_records
        if marker:
            params['Marker'] = marker

        pg =self.get_object('DescribeCacheParameters', params, ParameterGroup)

        pg.name = groupname

        return pg

    def create_parameter_group(self, name, description, family='memcached1.4'):
        """
        Create a new cacheparameter group for your account.

        :type name: string
        :param name: The name of the new cacheparameter group

        :type family: str
        :param family: Currently, memcached1.4 is the only cache parameter 
		       group family supported by the service

        :type description: string
        :param description: The description of the new cacheparameter group

        :rtype: :class:`boto.elasticache.parametergroup.ParameterGroup`
        :return: The ParameterGroup
        """
        params = {'CacheParameterGroupName': name,
                  'CacheParameterGroupFamily': family,
                  'Description': description}
        return self.get_object('CreateCacheParameterGroup', params, ParameterGroup)

    def modify_parameter_group(self, name, parameters=None):
        """
        Modify a parameter group for your account.

        :type name: string
        :param name: The name of the new parameter group

        :type parameters: list of :class:`boto.elasticache.parametergroup.Parameter`
        :param parameters: The new parameters

        :rtype: :class:`boto.elasticache.parametergroup.ParameterGroup`
        :return: The newly created ParameterGroup
        """
        params = {'CacheParameterGroupName': name}
        for i in range(0, len(parameters)):
            parameter = parameters[i]
            parameter.merge(params, i+1)
        return self.get_list('ModifyCacheParameterGroup', params,
                             ParameterGroup, verb='POST')

    def reset_parameter_group(self, name, reset_all_params=False,
                              parameters=None):
        """
        Resets some or all of the parameters of a ParameterGroup to the
        default value

        :type key_name: string
        :param key_name: The name of the ParameterGroup to reset

        :type parameters: list of :class:`boto.elasticache.parametergroup.Parameter`
        :param parameters: The parameters to reset.  If not supplied,
                           all parameters will be reset.
        """
        params = {'CacheParameterGroupName': name}
        if reset_all_params:
            params['ResetAllParameters'] = 'true'
        else:
            params['ResetAllParameters'] = 'false'
            for i in range(0, len(parameters)):
                parameter = parameters[i]
                parameter.merge(params, i+1)
        return self.get_status('ResetCacheParameterGroup', params)

    def delete_parameter_group(self, name):
        """
        Delete a DBSecurityGroup from your account.

        :type key_name: string
        :param key_name: The name of the DBSecurityGroup to delete
        """
        params = {'CacheParameterGroupName': name}
        return self.get_status('DeleteCacheParameterGroup', params)

    # CacheSecurityGroup methods

    def get_all_cachesecurity_groups(self, groupname=None, max_records=None,
                                  marker=None):
        """
        Get all security groups associated with your account in a region.

        :type groupnames: list
        :param groupnames: A list of the names of security groups to retrieve.
                           If not provided, all security groups will
                           be returned.

        :type max_records: int
        :param max_records: The maximum number of records to be returned.
                            If more results are available, a MoreToken will
                            be returned in the response that can be used to
                            retrieve additional records.  Default is 100.

        :type marker: str
        :param marker: The marker provided by a previous request.

        :rtype: list
        :return: A list of :class:`boto.elasticache.cachesecuritygroup.CacheSecurityGroup`
        """
        params = {}
        if groupname:
            params['CacheSecurityGroupName'] = groupname
        if max_records:
            params['MaxRecords'] = max_records
        if marker:
            params['Marker'] = marker
        return self.get_list('DescribeCacheSecurityGroups', params,
                             [('CacheSecurityGroup', CacheSecurityGroup)])

    def create_cachesecurity_group(self, name, description=None):
        """
        Create a new security group for your account.
        This will create the security group within the region you
        are currently connected to.

        :type name: string
        :param name: The name of the new security group

        :type description: string
        :param description: The description of the new security group

        :rtype: :class:`boto.elasticache.cachesecuritygroup.CacheSecurityGroup`
        :return: The newly created CacheSecurityGroup
        """
        params = {'CacheSecurityGroupName': name}
        if description:
            params['Description'] = description
        group = self.get_object('CreateCacheSecurityGroup', params,
                                CacheSecurityGroup)
        group.name = name
        group.description = description
        return group

    def delete_cachesecurity_group(self, name):
        """
        Delete a CacheSecurityGroup from your account.

        :type key_name: string
        :param key_name: The name of the DBSecurityGroup to delete
        """
        params = {'CacheSecurityGroupName': name}
        return self.get_status('DeleteCacheSecurityGroup', params)

    def authorize_cachesecurity_group(self, group_name, cidr_ip=None,
                                   ec2_security_group_name=None,
                                   ec2_security_group_owner_id=None):
        """
        Add a new rule to an existing security group.
        You need to pass in either src_security_group_name and
        src_security_group_owner_id OR a CIDR block but not both.

        :type group_name: string
        :param group_name: The name of the security group you are adding
                           the rule to.

        :type ec2_security_group_name: string
        :param ec2_security_group_name: The name of the EC2 security group
                                        you are granting access to.

        :type ec2_security_group_owner_id: string
        :param ec2_security_group_owner_id: The ID of the owner of the EC2
                                            security group you are granting
                                            access to.

        :type cidr_ip: string
        :param cidr_ip: The CIDR block you are providing access to.
                        See http://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing

        :rtype: bool
        :return: True if successful.
        """
        params = {'CacheSecurityGroupName': group_name}
        if ec2_security_group_name:
            params['EC2SecurityGroupName'] = ec2_security_group_name
        if ec2_security_group_owner_id:
            params['EC2SecurityGroupOwnerId'] = ec2_security_group_owner_id
        if cidr_ip:
            params['CIDRIP'] = urllib.quote(cidr_ip)
        return self.get_object('AuthorizeCacheSecurityGroupIngress', params,
                               CacheSecurityGroup)

    def revoke_cachesecurity_group(self, group_name, ec2_security_group_name=None,
                                ec2_security_group_owner_id=None, cidr_ip=None):
        """
        Remove an existing rule from an existing security group.
        You need to pass in either ec2_security_group_name and
        ec2_security_group_owner_id OR a CIDR block.

        :type group_name: string
        :param group_name: The name of the security group you are removing
                           the rule from.

        :type ec2_security_group_name: string
        :param ec2_security_group_name: The name of the EC2 security group
                                        from which you are removing access.

        :type ec2_security_group_owner_id: string
        :param ec2_security_group_owner_id: The ID of the owner of the EC2
                                            security from which you are
                                            removing access.

        :type cidr_ip: string
        :param cidr_ip: The CIDR block from which you are removing access.
                        See http://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing

        :rtype: bool
        :return: True if successful.
        """
        params = {'CacheSecurityGroupName': group_name}
        if ec2_security_group_name:
            params['EC2SecurityGroupName'] = ec2_security_group_name
        if ec2_security_group_owner_id:
            params['EC2SecurityGroupOwnerId'] = ec2_security_group_owner_id
        if cidr_ip:
            params['CIDRIP'] = cidr_ip
        return self.get_object('RevokeCacheSecurityGroupIngress', params,
                               CacheSecurityGroup)

    # Events

    def get_all_events(self, source_identifier=None, source_type=None,
                       start_time=None, end_time=None,
                       max_records=None, marker=None):
        """
        Get information about events related to your Cache Clusters,
        Cache Security Groups and Cache Parameter Groups.

        :type source_identifier: str
        :param source_identifier: If supplied, the events returned will be
                                  limited to those that apply to the identified
                                  source.  The value of this parameter depends
                                  on the value of source_type.  If neither
                                  parameter is specified, all events in the time
                                  span will be returned.

        :type source_type: str
        :param source_type: Specifies how the source_identifier should
                            be interpreted.  Valid values are:
                            b-instance | db-security-group |
                            db-parameter-group | db-snapshot

        :type start_time: datetime
        :param start_time: The beginning of the time interval for events.
                           If not supplied, all available events will
                           be returned.

        :type end_time: datetime
        :param end_time: The ending of the time interval for events.
                         If not supplied, all available events will
                         be returned.

        :type max_records: int
        :param max_records: The maximum number of records to be returned.
                            If more results are available, a MoreToken will
                            be returned in the response that can be used to
                            retrieve additional records.  Default is 100.

        :type marker: str
        :param marker: The marker provided by a previous request.

        :rtype: list
        :return: A list of class:`boto.elasticache.event.Event`
        """
        params = {}
        if source_identifier and source_type:
            params['SourceIdentifier'] = source_identifier
            params['SourceType'] = source_type
        if start_time:
            params['StartTime'] = start_time.isoformat()
        if end_time:
            params['EndTime'] = end_time.isoformat()
        if max_records:
            params['MaxRecords'] = max_records
        if marker:
            params['Marker'] = marker
        return self.get_list('DescribeEvents', params, [('Event', Event)])
