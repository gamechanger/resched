__author__ = 'Kiril Savino'

import simplejson

class ContentType(object):
    STRING = 'string'
    JSON = 'json'
    INT = 'int'
    FLOAT = 'float'
    ALL_TYPES = (STRING, JSON, INT, FLOAT)

class RedisBacked(object):
    __slots__ = ('server', 'namespace', 'content_type')

    def __init__(self, redis_client, namespace, content_type, **kwargs):
        assert redis_client, "got invalid Redis client"
        assert namespace, "yo, bro, need to pass in a valid name, or just leave it defaulted, mkay?"
        assert content_type in ContentType.ALL_TYPES, "invalid content_type"
        self.server = redis_client
        self.namespace = namespace
        self.content_type = content_type
        self.content_type_args = kwargs

    def pack(self, value):
        if value is None:
            return value
        if isinstance(value, basestring):
            return value
        if self.content_type == ContentType.INT:
            return str(value)
        if self.content_type == ContentType.FLOAT:
            return str(value)
        if self.content_type == ContentType.JSON:
            if 'encoder' in self.content_type_args:
                return simplejson.dumps(value, cls=self.content_type_args['encoder'])
            return simplejson.dumps(value)
        if self.content_type == ContentType.STRING:
            return str(value)
        raise Exception("I don't understand content type %s" % self.content_type)

    def unpack(self, value):
        if value is None:
            return value
        if self.content_type == ContentType.STRING:
            return value
        if self.content_type == ContentType.INT:
            return int(value)
        if self.content_type == ContentType.FLOAT:
            return float(value)
        if self.content_type == ContentType.JSON:
            if 'decode_hook' in self.content_type_args:
                return simplejson.loads(value, object_hook=self.content_type_args['decode_hook'])
            return simplejson.loads(value)
        raise Exception("I don't understand content type %s" % self.content_type)
