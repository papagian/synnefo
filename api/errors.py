#
# Copyright (c) 2010 Greek Research and Technology Network
#

def camelCase(s):
    return s[0].lower() + s[1:]


class Fault(BaseException):
    def __init__(self, message='', details='', name=''):
        BaseException.__init__(self, message, details, name)
        self.message = message
        self.details = details
        self.name = name or camelCase(self.__class__.__name__)

class BadRequest(Fault):
    code = 400

class Unauthorized(Fault):
    code = 401

class ResizeNotAllowed(Fault):
    code = 403

class ItemNotFound(Fault):
    code = 404

class NotImplemented(Fault):
    code = 501

class ServiceUnavailable(Fault):
    code = 503
