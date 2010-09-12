"""
    Copyright 2009 Oregon State University

    This file is part of Pydra.

    Pydra is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Pydra is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Pydra.  If not, see <http://www.gnu.org/licenses/>.
"""
import cookielib
import hashlib
import simplejson
import urllib
import urllib2

from pydra.cluster.auth.rsa_auth import load_crypto
from pydra.cluster.controller import ControllerException, \
    ControllerRemoteException, CONTROLLER_ERROR_FUNCTION_NOT_FOUND, \
    CONTROLLER_ERROR_DISCONNECTED, CONTROLLER_ERROR_NO_RSA_KEY, \
    CONTROLLER_ERROR_AUTH_FAIL


class WebControllerFunction():
    """
    Proxy to a remote function exposed to the interface.  Receiving this
    object does not indicate that the class exists.  It may still raise
    an AttributeError if the remote functions does not exist.
    
    WebControllerFunction also encapsulates authentication and will make
    additional requests to authenticate with the master if needed.
    """
    def __init__(self, controller, key):
        """
        :parameters:
            controller: controller this function is associated with
            key: function name this object wraps
        """
        self.controller = controller
        self.key = key
    
    def __call__(self, *args, **kwargs):
        """
        Runs this function by calling the corresponding remote function.  args
        and kwargs will be JSON encoded and included as POST arguments
        """
        try:
            values = {'args':simplejson.dumps(args),
                      'kwargs':simplejson.dumps(kwargs)}
            data = urllib.urlencode(values)
            url = self.controller.opener.open(self.make_url(), data)
            return simplejson.loads(url.read())
        except urllib2.HTTPError, e:
            if e.code == 404:
                raise ControllerException(CONTROLLER_ERROR_FUNCTION_NOT_FOUND)
            elif e.code == 401:
                return self._authenticate(*args, **kwargs)
            elif e.code == 500:
                errors = simplejson.loads(e.msg)
                raise ControllerRemoteException(errors)
            else:
                raise ControllerException()
        except urllib2.URLError, e:
            raise ControllerException(CONTROLLER_ERROR_DISCONNECTED)

    def make_url(self):
        return 'https://%s:%s/%s' % (self.controller.host,
                                    self.controller.port,
                                    self.key)

    def _authenticate(self, *args, **kwargs):
        """
        Authenticate with server and then recall this WebControllerFunction
        if successful
        """
        
        # If priv_key is None, then master.key didn't get loaded correctly.
        if self.controller.priv_key is None:
            raise ControllerException(CONTROLLER_ERROR_NO_RSA_KEY)
        
        # get challenge string from server
        challenge = str(self.controller.authenticate())
        
        # re-encrypt using servers key and then sha hash it before sending it
        # back
        response_encode = self.controller.priv_key.encrypt(challenge, None)
        response_hash = hashlib.sha512(response_encode[0]).hexdigest()
        
        # if challenge is successful, call this function again
        if self.controller.challenge_response(response=response_hash):
            return self(*args, **kwargs)
        
        raise ControllerException(CONTROLLER_ERROR_AUTH_FAIL)


class WebController(object):
    """
    Controller for interacting with pydra web interface
    """
    
    def __init__(self, host='localhost', port=18801, key='./master.key'):
        """
        Initialize WebController
        
        :parameters:
            host:  host of interface
            port:  port of interface
            key:  path to key file for authentication [./master.key]
        """
        self.host = host
        self.port = port
        
        # load rsa crypto
        self.pub_key, self.priv_key = load_crypto(key, False)
        cj = cookielib.CookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))

    def __getattribute__(self, key):
        """
        Overridden to return remote (unknown) methods as WebControllerFunctions.
        The functions are cached on the controller so that repeat calls will
        use the existing WebControllerFunction instance.
        """
        if key == '__dict__' or key in self.__dict__.keys():
            return object.__getattribute__(self, key)
        
        # wrap and cache remote function
        func =  WebControllerFunction(self, key)
        self.__dict__[key] = func
        return func