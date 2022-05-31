from __future__ import annotations
from typing import Optional, Collection, Callable, Union
from sortedcontainers.sortedlist import SortedList
import requests
import socket
import http.client
from time import time
from concurrent.futures import ThreadPoolExecutor
import random
from netutils import generate_headers
from datetime import datetime
from sys import maxsize


PROXY_PROTOCOLS = {"http", "https", "socks4", "socks4a", "socks5", "socks5h"}


def assert_protocol(name: str) -> None:
    if name not in PROXY_PROTOCOLS:
        raise AttributeError(f"Invalid proxy server protocol '{name}'.")


def proxy_string(protocol: str, host: str, port: int, a: auth) -> str:
    protocol = protocol.lower()
    s = f"{protocol}://"
    if a:
        if protocol == "socks4" or protocol == "socks4a":
            username, _ = a  # socks4 and socks4a don't support password auth
            s += f"{username}@"
        else:
            username, password = a  # the rest of the protocols do
            s += f"{username}:{password}@"
    s += f"{host}:{port}"
    return s


class Proxy:
    protocols: list[str]
    host: str
    port: int
    auth: Optional[tuple[str, Optional[str]]]  # socks4, socks4a do not have a password by design
    response_stats: list[tuple[float, datetime]]  # speed of the proxy at given DT
    online_checks: list[tuple[bool, datetime]]
    _response_speed: float
    _uptime: float
    pool: ProxyPool

    def __init__(self, pool: ProxyPool, host: str, port: int, auth: Optional[tuple[str, Optional[str]]] = None):
        self.host = host
        self.port = port
        # idk, maybe there are proxies which use passwords but not usernames
        # for example, it would probably be possible with HTTP "Proxy-Authentication: Basic ..." header value
        # since it's just encoded Base64, so doesn't matter what you write there
        # basically, my logic is: don't overthink. Server might be configured to whatever a human mind can think of.
        self.auth = auth
        self.pool = pool
        self.protocols = []

        self.response_stats = []
        self._response_speed = 0.0
        self.online_checks = []
        self._uptime = 0.0

    def supports(self, protocol: str) -> bool:
        return protocol in self.protocols

    def _cache_uptime(self) -> None:
        if not self.online_checks:
            self._uptime = 0.0
            return
        times_online = 0.
        for online in self.online_checks:
            b, dt = online
            times_online += b
        self._uptime = times_online / len(self.online_checks)

    def _cache_speed(self) -> None:
        """
        :return: mean response speed (kbytes per second)
        """
        if not self.response_stats:
            self._response_speed = 0.0
            return
        speed = 0.0
        for sp in self.response_stats:
            s, dt = sp
            speed += s
        self._response_speed = speed / len(self.response_stats) / 1024.  # there is 1024 bytes per kbyte

    def rating(self) -> float:
        return self._response_speed * self._uptime

    def __eq__(self, other: hostport) -> bool:
        host, port = parse_host_port(other)
        return self.host == host and self.port == port

    def __str__(self):
        speed = f"{int(self._response_speed)} KB/s"
        uptime = f"{self._uptime * 100}%"
        return f"{self.__repr__()};{speed};{uptime};"

    def add_online(self, b: bool) -> None:
        self.online_checks.append((b, datetime.now()))

    def add_speed(self, f: float) -> None:
        self.response_stats.append((f, datetime.now()))

    def __repr__(self):
        s = f"[{','.join(self.protocols)}]://"
        if self.auth:
            username, password = self.auth
            if username:
                username = username.strip()
            if not username:
                username = ''
            if password:
                password = password.strip()
            if not password:
                password = ''
            if username and password:
                s += f"{username}:{password}@"
            elif username:
                s += f"{username}@"
            elif password:
                s += f":{password}@"
        s += f"{self.host}:{self.port}"
        return s

    def last_online(self) -> bool:
        return self.online_checks[-1][0]

    def dict(self, protocol: Optional[str] = None):
        if not protocol:
            random.choice(self.protocols)
        s = proxy_string(protocol, self.host, self.port, self.auth)
        return {
            "https": s,
            "http": s
        }

    def check_protocol(self, protocol: str) -> bool:
        """
        :param protocol:
        :return: 
        """
        # the only way to check if a proxy follows the protocol is to connect through it to a server.
        # only in case of a successful connection can we speak of the proxy following the protocol.

        # requests proxy routing dict
        proxies = self.dict(protocol)  # this means "route all https and http traffic through this proxy"
        urls = self.pool.urls.copy()
        random.shuffle(urls)
        for url in urls:
            try:
                # check every test url
                start = time()
                response = requests.get(url, headers=generate_headers(), proxies=proxies,
                                        timeout=self.pool.timeout, stream=True)
                if response.status_code == 200:
                    # if 200, then most probably this is a working proxy server which speaks this protocol
                    # (rarely it will be a server, which allows CONNECT requests
                    #  and answers with 200 to anything you feed it)
                    end = time()
                    # calculate speed of the response (bytes per second)
                    size = len(response.raw.data)
                    dt = end - start
                    self.add_speed(size / dt)  # add speed record
                    self._cache_speed()  # calculate cached value
                    return True
                elif response.status_code == 407:
                    # this means bad authentication
                    # no use checking further
                    return False  # no speed
                # else try another url
            except requests.exceptions.Timeout as ex:
                pass
            except requests.exceptions.ProxyError as ex:
                pass
            except requests.exceptions.ConnectionError as ex:
                pass
            except http.client.IncompleteRead as ex:
                pass
            except Exception as ex:
                pass
        # no server has responded positively
        # 3 main reasons for that:
        # 1. Proxy is bad.
        # 2. Proxy doesn't know the protocol we're trying to speak.
        # 3. Proxy isn't allowed to reach any of the test servers.
        # By trying every possible protocol, we're addressing the #1 and #2 issues.
        # #3 doesn't really matter, in the end, since we cannot be held responsible for this issue.
        return False  # no speed

    def check(self) -> None:
        try:
            # if this sequence goes well,
            # then the remote server allows connections to the port
            # and it might be a proxy server
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.host, self.port))
            s.close()
            was_able_to_connect = True
        except ConnectionRefusedError or socket.gaierror or Exception:
            # connection to the alleged proxy server was refused
            was_able_to_connect = False
        if was_able_to_connect:
            # time to check if we can speak to the proxy via any of the protocols
            with ThreadPoolExecutor(max_workers=self.pool.max_protocol_workers) as pool:
                futures = []
                for protocol in self.pool.protocols:
                    future = pool.submit(self.check_protocol, protocol)
                    futures.append(future)
                # we await for all results
                # must be the same as submitted, both the sequence and the number of the results
                results = [f.result() for f in futures]
            # check results
            protocols = []
            proxy_is_online = False
            for protocol, result in zip(self.pool.protocols, results):
                if result:
                    protocols.append(protocol)
                    proxy_is_online = True
            self.protocols = protocols
            self.add_online(proxy_is_online)
        else:
            self.add_online(False)
        self._cache_uptime()  # new cached uptime result


def empty_callback(proxy: Proxy):
    # does nothing
    pass


def print_callback(proxy: Proxy):
    # prints found proxy in green text
    print(proxy)


class ProxyPool:
    """
    Proxy Pool class.
    Best used with Python's "with" clause.
    >>> urls = ["https://google.com"]  # we're searching for proxies which work with google
    >>> proxy_pool = ProxyPool(urls)
    >>> random_proxies: Collection[tuple[hostport, auth]] = [...]  # array of some proxies
    >>> # the code below is blocking, yet inside for each proxy multiple threads are used
    >>> with proxy_pool.limit_capacity(10) as pool:
    >>>     pool.add_many(random_proxies)
    >>> # this will exit when 10 or more proxies were found
    >>> # capacity is limited due to the fact that sometimes there are too many proxies passed (tens of thousands)
    >>> # and we only need to find a few for our purposes
    >>> proxy: hostport = "127.0.0.1:9050"  # check out hostport signature
    >>> a: auth = ("user", "pass")  # check out auth signature
    >>> # single proxies can be added
    >>> with proxy_pool:  # NOTE: the limit_capacity(10) method DOESN'T work, since it is ANOTHER context
    >>>     pool.add(proxy, auth)
    >>>
    """
    proxies: SortedList[Proxy]
    cached_proxies: set[tuple[hostport, auth]]
    urls: list[str]
    protocols: Collection[str]
    timeout: float
    callback: Callable[[Proxy, ], None]

    max_proxy_workers: int
    max_protocol_workers: int

    submit_count: int  # count of proxies submitted to thread pool executor
    submit_limit: int  # limit of submitted proxies (max number of)
    capacity_limit: int  # limit of alive proxies currently in the pool
    # when any limit exceeds, all other proxies put into the pool are passed to the cached_proxies set

    def __init__(self,
                 urls: list[str], timeout: float = 2.0,
                 protocols: Optional[Collection[str]] = None,
                 max_protocol_workers: int = len(PROXY_PROTOCOLS),
                 max_proxy_workers: int = 5,
                 callback: Callable[[Proxy, ], None] = empty_callback):
        """
        :param urls: urls to test the proxies against
        :param timeout: request timeout
        :param max_protocol_workers: max number of protocols per proxy checked simultaneously (min 1)
        :param protocols: set of protocols to check proxies for
        :param callback: callback triggered, when a new alive proxy was found
        """
        self.urls = urls
        self._headers = generate_headers()
        self.timeout = timeout
        if not protocols:
            protocols = PROXY_PROTOCOLS.copy()
        self.protocols = tuple(protocols)
        self.max_protocol_workers = max_protocol_workers
        self._initialize_sorted_list()
        self.callback = callback
        self.max_proxy_workers = max_proxy_workers
        self._initialize_state_variables()
        self.cached_proxies = set()

    def _initialize_sorted_list(self):
        self.proxies = SortedList(key=lambda proxy: -proxy.rating())  # sort via rating DESC (best first)

    def clear(self):
        self._initialize_sorted_list()

    def _add(self, proxy: Proxy) -> None:
        proxy.check()  # check working protocols
        if proxy.last_online():  # proxy is online
            self.proxies.add(proxy)  # add the proxy to the list
            self.callback(proxy)  # trigger the callback

    def add_many(self, proxies: Union[Collection[hostport], Collection[tuple[hostport, auth]]], flag=None) -> None:
        if flag == "noauth":
            for p in proxies:
                self.add(p, None)
        else:
            for i in proxies:
                p, a = i
                self.add(p, a)

    def add(self, p: hostport, a: auth = None) -> bool:
        """
        A non-blocking function which adds the specified host, port and authentication data to an execution queue,
        where a function tries to make sense of the data and find out which protocol the proxy server serves.
        In case the proxy server is alive and fulfills its role, the callback function is executed.
        :param p: alleged host of the proxy server
        :param a: alleged proxy server authentication credentials
        :return: nothing. Calls the self.callback(Proxy) function on success.
        """
        proxy = parse_host_port(p)
        a = parse_auth(a)
        if proxy not in self:
            if not self.any_limit_reached():
                host, port = proxy
                proxy = Proxy(self, host, port, a)
                self.executor.submit(self._add, proxy)  # submit to the executor
                self.submit_count += 1  # add count
                return True  # submitted
            else:
                self.cached_proxies.add((proxy, a))
                return False  # not submitted
        else:
            return False  # already in the pool

    def is_empty(self) -> bool:
        return len(self.proxies) == 0

    def cache_is_empty(self) -> bool:
        return len(self.cached_proxies) == 0

    def _initialize_state_variables(self) -> None:
        self.submit_limit = maxsize
        self.capacity_limit = maxsize
        self.submit_count = 0

    def any_limit_reached(self) -> bool:
        return self.submit_count >= self.submit_limit or len(self.proxies) >= self.capacity_limit

    def limit_capacity(self, n: int) -> ProxyPool:
        if n < 1:
            raise ValueError(f"n={n}: capacity limit must be a positive number.")
        self.capacity_limit = n
        return self

    def limit_submits(self, n: int) -> ProxyPool:
        if n < 1:
            raise ValueError(f"n={n}: submits limit must be a positive number.")
        self.submit_limit = n
        return self

    def __enter__(self):
        self.executor = ThreadPoolExecutor(max_workers=self.max_proxy_workers)
        return self

    def __exit__(self, type, value, traceback):
        self.executor.shutdown(wait=True)
        self._initialize_state_variables()
        return False

    def __contains__(self, item: hostport) -> bool:
        for proxy in self.proxies:
            if proxy == item:
                return True
        return False

    def __len__(self):
        return len(self.proxies)

    def __bool__(self):
        return len(self) < 1

    def remove(self, p: hostport) -> bool:
        proxy = parse_host_port(p)
        if proxy not in self:
            return False
        self.proxies.remove(p)
        return True


# host port pair type
hostport = Union[str, tuple[str, str], tuple[str, int], Proxy]
# auth pair type
auth = Optional[Union[str, tuple[str, Optional[str]]]]


def parse_host_port(p: hostport) -> tuple[str, int]:
    if type(p) is tuple:
        host, port = p
        port = int(port)
    elif type(p) is str:
        hp = p.rsplit(":", 1)
        if len(hp) == 2:
            host, port = hp
            host = host.strip()
            port = port.strip()
        else:
            raise ValueError(f"{p} doesn't contain either a host or a port.")
        port = int(port)
    elif type(p) is Proxy:
        host = p.host
        port = p.port
    else:
        raise TypeError(
            f"'{type(p)}' is an invalid type for a proxy server address definition. "
            f"Valid types are: '{hostport}'. Value passed: '{p}'."
        )
    return host, port


def parse_auth(a: auth) -> Optional[tuple[str, Optional[str]]]:
    if type(a) is str:
        # consider it a "<user>:<pass>" string
        parts = a.rsplit(":", 1)
        if len(parts) == 2:
            user, password = parts
            # don't strip any spaces
            return user, password
        else:
            return parts[0], None
    elif type(a) is tuple:
        if len(a) == 2:
            return a  # return as is
        elif len(a) == 1:
            return a[0], None  # consider it a username
        else:
            raise ValueError(f"How do you parse a {len(a)}-element tuple to a USER:PASS pair, huh?")
    elif a is None:
        return None
    else:
        raise ValueError(f"No idea how to parse a {type(a)} into a USER:PASS pair.")
