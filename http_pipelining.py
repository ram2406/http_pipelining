"""
Python implementation
HTTP pipelining is a feature of HTTP/1.1 which allows multiple HTTP requests 
  to be sent over a single TCP connection without waiting for the corresponding responses
  https://en.wikipedia.org/wiki/HTTP_pipelining
"""

import socket
import ssl
from typing import Tuple
import requests

import time
import re
import warnings

_debug = lambda s: None
# _debug = lambda s: print(s)

def perform(urls: list[str], host: str, port: int, method: str,
            buffer_size=1024*1024,
            timeout=2,
            use_ssl=None,
            connection_keep_alive=False,
            headers=dict(),
    ):
    
    sep = '\r\n'
    data_sep = (sep *2).encode()

    intput_headers_str = ''.join( [str(key) + ': ' + str(val) + sep for key, val in headers.items()])

    class CaseInsensitiveDict(dict):
        def __setitem__(self, key, value):
            super(CaseInsensitiveDict, self).__setitem__(key.lower(), value)

        def __getitem__(self, key):
            return super(CaseInsensitiveDict, self).__getitem__(key.lower())

    
    def prepare_packet() -> bytearray:

        con_type = '' if connection_keep_alive else f'Connection: close{sep}'
        packet = f'{sep}'.join( [
            # f"{method.upper()} {url} HTTP/1.1{sep}Host: {host}{sep}"
            f"{method.upper()} {url} HTTP/1.1{sep}Host: {host}{sep}{intput_headers_str}"
            for url in urls
        ] )
        packet += con_type
        packet += sep *2
        packet = packet.encode()
        return packet


    def prepare_headers(raw_headers) -> CaseInsensitiveDict:
        headers = CaseInsensitiveDict()
        for h in raw_headers:
            left, right = h.split(':',1)
            headers[left.strip()] = right.strip()
        return headers


    def get_response(raw: bytearray, url: str) -> Tuple[requests.Response, bytearray]:
        headers_end = raw.find(data_sep)
        top, *raw_headers = raw[0:headers_end].decode().split(sep)
        
        protocol, rcode, rmsg = top.split(' ', 2)
        
        resp = requests.Response()
        resp.status_code = int(rcode)
        resp.reason = rmsg
        resp.headers = prepare_headers(raw_headers)
        resp._content_consumed = True
        offset = int( resp.headers.get('content-length', 0) ) if method != 'HEAD' else 0
        resp._content = raw[headers_end + len(data_sep): offset] if offset else None 
        resp.url = url
        return resp, memoryview(raw)[headers_end + len(data_sep) + offset:].tobytes()


    def parse_response(content: bytearray) -> list[requests.Response]:
        
        prepared_resps = []
        for idx, url in enumerate(urls):
            try:
                resp, content = get_response(content, url)
            except Exception as ex:
                _debug(content[:100])
                _debug(f'url with error {idx}: {url}')
                raise ex
            
            prepared_resps.append(resp)

            if not content:
                _debug('data finish')
                break
            

        if len(prepared_resps) != len(urls):
            _debug(prepared_resps)
            _debug(urls)
            ex = RuntimeError("data is not enough")
            ex.ready_responses = prepared_resps
            raise ex
        return prepared_resps


    def recvall(sock: socket.socket, resp_count) -> bytearray:
        _debug(f'recvall {resp_count}')
        data = bytearray()
        try:
            while data.count(data_sep) != resp_count:
                packet = sock.recv(buffer_size)
                if not packet:
                    _debug('no packet')
                    break
                data.extend(packet)
        except Exception as ex:
            _debug('timeout')
            pass
        _debug(f'data length {len(data)}')
        # _debug(f'data content {(data)}')
        _debug(f'data count {data.count(data_sep)}')
        return data


    def read_response_sock(sock) -> bytearray:
        sock.setblocking(0)
        sock.settimeout(timeout)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        sock.connect((host, port))
        sock.sendall(prepare_packet())

        raw_responses = recvall(sock, len(urls))
        sock.shutdown(socket.SHUT_RDWR)
        return raw_responses


    def read_response_ssl():
    
        with ssl.create_default_context().wrap_socket(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                server_hostname=host,
        ) as sock:
            return parse_response(
                read_response_sock(sock)
            )

    def read_response():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            return parse_response(
                read_response_sock(sock)
            )

    return read_response_ssl() if use_ssl or port == 443 else read_response()

def http_pipelining(urls, method = 'HEAD', buffer_size=1024*1024, timeout=2, headers={}):
    if not len(urls):
        return []
    protocol, host, port = re.findall(r'(https?):\/\/(.*?)(:\d+)?\/', urls[0])[0]
    port = port and int(port[1:] or 0)
    default_port = (443 if protocol == 'https' else 80)
    return perform(urls, host, port or default_port, method,
        buffer_size=buffer_size, timeout=timeout,
        headers=headers,
    )

def http_pipelining_with_retries(   urls, method = 'HEAD', 
                                    buffer_size=1024*1024, timeout=2,
                                    max_retries=2, backoff_factor=2, status_forcelist=[ 500, 502, 503, 504 ], headers={}):
    total_ex = None
    total_responses = []
    backoff = backoff_factor
    urls_to_perform = urls
    for it in range(0, max_retries):
        _debug(f'{it} attempt of {max_retries}')
        try:
            total_ex = None
            responses = http_pipelining(
                    urls_to_perform, 
                    method, 
                    buffer_size=buffer_size,
                    timeout=timeout,
                    headers=headers,
                )
            resps = [r for r in responses if r.status_code in status_forcelist]
            if len(resps):
                ex = RuntimeError("Response with code from forcelist")
                ex.ready_responses = responses
                ex.forcelist_responses = resps
                raise ex
            total_responses += responses
            break
        except Exception as ex:
            warnings.warn(f'exception on {it} attempt {ex}')
            ready_resps = getattr(ex, 'ready_responses', None)
            if ready_resps:
                handled = [r for r in ready_resps if r.status_code < 500]
                need_urls = urls_to_perform
                urls_to_perform = [ u for u in need_urls if not u in handled_urls ]
                total_responses += handled
                warnings.warn(f'exclude complete urls from {len(urls)} to {len(urls_to_perform)}')
                if not len(urls_to_perform): break
            total_ex = ex
            time.sleep(backoff)
            backoff += it * backoff_factor
    if total_ex:
        raise total_ex
    return total_responses

# %%
