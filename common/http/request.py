# Author: Jieyi Long (jieyi@sliver.tv)
# Date: Apr 2016


import traceback
import sys
import json
import subprocess
import urllib
import requests
from ..utils import Logger
from ..constants import ApiStatus, ApiKey, ApiErrorCode
from ..http.response import HttpSuccess, HttpError


class HttpMethod:
    GET = 'GET'
    PUT = 'PUT'
    POST = 'POST'
    DELETE = 'DELETE'


class HttpConnectLib(object):

    # verify: whether to perform verification for Https connections
    def get(self, url, data, verify):
        pass

    def post(self, url, data, verify):
        pass

    def put(self, url, data, verify):
        pass

    def delete(self, url, data, verify):
        pass

    def _assembleQueryString(self, data):
        if len(data.keys()) == 0:
            return ''
        else:
            return '?' + urllib.urlencode(data)

    """
    def _assembleQueryString(self, data):
      query_string = ''
      conn = '?'
      for key in data.keys():
        val = data[key]
        query_filter = conn + '%s=%s'%(key, val)
        query_string += query_filter 
        conn = '&'
      return query_string
    """


# FIXME: using requests POST request to Dropwizard server returns HTTP 415 errors
class RequestsLib(HttpConnectLib):

    def updateToken(self, token):
        self._token = token

    def get(self, url, data, verify):
        # Need to put the query params into the url, otherwise some 3rd party API call may fail
        full_url = url + self._assembleQueryString(data)
        return requests.get(full_url, {}, verify).text

    def post(self, url, data, verify):
        return requests.post(url, data, verify).text

    def put(self, url, data, verify):
        return requests.put(url, data, verify).text

    def delete(self, url, data, verify):
        return requests.delete(url, data, verify).text


class CurlLib(HttpConnectLib):

    def updateToken(self, token):
        self._token = token

    def get(self, url, data, verify):
        return self._connect(HttpMethod.GET, url, data, verify)

    def post(self, url, data, verify):
        return self._connect(HttpMethod.POST, url, data, verify)

    def put(self, url, data, verify):
        return self._connect(HttpMethod.PUT, url, data, verify)

    def delete(self, url, data, verify):
        return self._connect(HttpMethod.DELETE, url, data, verify)

    def _connect(self, method, url, data, verify):
        if method == HttpMethod.GET:
            if not data:
                full_url = url
            else:
                full_url = url + self._assembleQueryString(data)
            curl_cmd = 'curl "%s"' % (full_url)
        elif (method == HttpMethod.DELETE):
            # Need to add "X-Auth-User" header for DELETE request
            json_str = json.dumps(data)
            rectified_json_str = json_str.replace("'", '\u0027')
            curl_cmd = """curl -X %s -H "Content-Type: application/json" %s -d '%s'""" % (
            method, url, rectified_json_str)
        elif (method == HttpMethod.POST) or (method == HttpMethod.PUT):
            # Need to add "X-Auth-Token" header for POST and PUT request
            json_str = json.dumps(data)
            rectified_json_str = json_str.replace("'", '\u0027')
            try:
                curl_cmd = """curl -X %s -H "Content-Type: application/json" %s -d '%s'""" % (
                method, url, rectified_json_str)
            except:
                curl_cmd_str = """curl -X {0} -H "Content-Type: application/json" {1} -d '{2}'""".format(method, url,
                                                                                                         rectified_json_str)
                # curl_cmd = unicode(curl_cmd_str, errors='ignore')
        else:
            raise Exception('Http method not supported! method: %s' % (method))

        Logger.printDebug('Curl command: %s' % (curl_cmd))

        proc = subprocess.Popen(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = proc.communicate()
        return out

    '''
    def _connect(self, method, url, data, verify):
      if method == HttpMethod.GET:
        full_url = url + self._assembleQueryString(data)
        curl_cmd = 'curl "%s"'%(full_url)
      elif (method == HttpMethod.POST) or (method == HttpMethod.PUT):
        try:
          json_str = json.dumps(data)
        except:
          json_str = json.dumps(data, ensure_ascii=False)
        rectified_json_str = json_str.replace("'", '\u0027')
        
        #curl_cmd = """curl -X %s -H "Content-Type: application/json" %s -d '%s'"""%(method, url, rectified_json_str)
        try:
          curl_cmd = """curl -X %s -H "Content-Type: application/json" %s -d '%s'"""%(method, url, rectified_json_str)
        except:
          curl_cmd_str = """curl -X {0} -H "Content-Type: application/json" {1} -d '{2}'""".format(method, url, rectified_json_str)
          curl_cmd = unicode(curl_cmd_str, errors='ignore')
      else:
        raise Exception('Http method not supported! method: %s'%(method))
      
      Logger.printDebug('Curl command: %s'%(curl_cmd))
      proc = subprocess.Popen(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
      out, err = proc.communicate()
      return out
    '''


class HttpRequest:

    def __init__(self, base_url, use_curl_lib=True):
        self._base_url = base_url
        if use_curl_lib:
            self._http_connect_lib = CurlLib()
        else:
            self._http_connect_lib = RequestsLib()

    def updateToken(self, token):
        self._http_connect_lib.updateToken(token)

    def get(self, rel_path, params):
        return self._connect(HttpMethod.GET, rel_path, params, False)

    def post(self, rel_path, params):
        return self._connect(HttpMethod.POST, rel_path, params, False)

    def put(self, rel_path, params):
        return self._connect(HttpMethod.PUT, rel_path, params, False)

    def delete(self, rel_path, params):
        return self._connect(HttpMethod.DELETE, rel_path, params, False)

    def getRawResponseString(self, rel_path, params):
        return self._connect(HttpMethod.GET, rel_path, params, True)

    def _connect(self, http_method, rel_path, params, return_raw_response=False):
        url = self._base_url + rel_path
        data = params

        if http_method == HttpMethod.GET:
            connect = self._http_connect_lib.get
        elif http_method == HttpMethod.PUT:
            connect = self._http_connect_lib.put
        elif http_method == HttpMethod.POST:
            connect = self._http_connect_lib.post
        elif http_method == HttpMethod.DELETE:
            connect = self._http_connect_lib.delete
        else:
            return None  # not supported

        success, raw_response_json = False, None

        raw_response_str = None

        retries_num = 0

        max_retries = 4

        for i in range(max_retries):

            try:
                # TODO: verify certificate for Https connections
                # print("url: {0}, data: {1}".format(url, data))
                raw_response_str = connect(url=url, data=data, verify=False)
                raw_response_json = json.loads(raw_response_str)
                success = True
                break
            except Exception as e:
                print("raw_response_str: {0}".format(raw_response_str))
                Logger.printError('Failed to call API: %s, data: %s, error: %s' % (url, data, e))
                traceback.print_exc()
                sys.stdout.flush()

                if i + 1 < max_retries:
                    retries_num += 1
                    print("Retrying...")

                else:
                    print("EXCEPTION PERSISTED AFTER {0} RETRIES!!!".format(max_retries - 1))

        if retries_num > 0:
            print("Success after {0} retries.".format(retries_num))

        if return_raw_response:
            return raw_response_str
        else:
            api_response = self._parseRawResponse(success, raw_response_json)
            return api_response

    def _parseRawResponse(self, success, raw_response_json):
        # print("raw_response_json: {0}".format(raw_response_json))
        if not success:
            Logger.printError('Invalid server response! raw_response_json: %s' % (raw_response_json))
            return HttpError(ApiErrorCode.SERVER_CONNECTION_ERROR,
                             'Failed to connect to server!')
        status = ApiStatus.OK

        body = '0'

        if 'result' in raw_response_json.keys():
            body = raw_response_json['result']

        return HttpSuccess(body)
