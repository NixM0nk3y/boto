#
#
#
#
#

from connection import ElastiCacheConnection
from boto.regioninfo import RegionInfo

def regions():
    """
    Get all available regions for the ElastiCache service.

    :rtype: list
    :return: A list of :class:`boto.regioninfo.RegionInfo` instances
    """
    return [RegionInfo(name='us-east-1',
                       endpoint='elasticache.us-east-1.amazonaws.com',
                       connection_cls=ElastiCacheConnection),
            RegionInfo(name='eu-west-1',
                       endpoint='elasticache.eu-west-1.amazonaws.com',
                       connection_cls=ElastiCacheConnection),
            RegionInfo(name='us-west-1',
                       endpoint='elasticache.us-west-1.amazonaws.com',
                       connection_cls=ElastiCacheConnection),
            RegionInfo(name='sa-east-1',
                       endpoint='elasticache.sa-east-1.amazonaws.com',
                       connection_cls=ElastiCacheConnection),
            RegionInfo(name='us-west-2',
                       endpoint='elasticache.us-west-2.amazonaws.com',
                       connection_cls=ElastiCacheConnection),
            RegionInfo(name='ap-northeast-1',
                       endpoint='elasticache.ap-northeast-1.amazonaws.com',
                       connection_cls=ElastiCacheConnection),
            RegionInfo(name='ap-southeast-1',
                       endpoint='elasticache.ap-southeast-1.amazonaws.com',
                       connection_cls=ElastiCacheConnection),
            ]

def connect_to_region(region_name, **kw_params):
    """
    Given a valid region name, return a 
    :class:`boto.elasticache.connection.ElastiCacheConnection`.

    :type: str
    :param region_name: The name of the region to connect to.
    
    :rtype: :class:`boto.elasticache.connection.ElastiCacheConnection` or ``None``
    :return: A connection to the given region, or None if an invalid region
             name is given
    """
    for region in regions():
        if region.name == region_name:
            return region.connect(**kw_params)
    return None

def get_region(region_name, **kw_params):
    """
    Find and return a :class:`boto.regioninfo.RegionInfo` object
    given a region name.

    :type: str
    :param: The name of the region.

    :rtype: :class:`boto.regioninfo.RegionInfo`
    :return: The RegionInfo object for the given region or None if
             an invalid region name is provided.
    """
    for region in regions(**kw_params):
        if region.name == region_name:
            return region
    return None
