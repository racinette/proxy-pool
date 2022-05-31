import requests
from netutils import (
    generate_headers, IPv4_REGEX, find_host_port_pairs,
    BASE64_WORD_REGEX, valid_ip, valid_host_port_pair, valid_port
)
from base64 import b64decode
from typing import Optional, Collection
import json
import bs4
import re


# -1
def scrape_ip3366() -> Collection[str]:  # shitty chinese proxies (only <<1% works)
    def get_proxies(s) -> Collection[str]:
        proxy_table = s.find(id="list").find("table").find("tbody")
        proxy_records = proxy_table.find_all("tr")
        proxies = set()
        for proxy_record in proxy_records:
            try:
                fields = proxy_record.find_all("td")
                ip = str(fields[0].get_text())
                if not valid_ip(ip):
                    continue
                port = int(fields[1].get_text())
                proxies.add(f"{ip}:{port}")
            except:
                pass
        return proxies

    def get_hrefs(s) -> set[str]:
        a_tags = s.find(id="listnav").find("ul").find_all("a")
        h = set(str(a["href"]) for a in a_tags)
        return h

    proxy_list = []
    base_url = "http://www.ip3366.net/free/"
    hrefs = {"?stype=1&page=1", "?stype=2&page=1"}
    visited_hrefs = set()
    while hrefs:
        href = hrefs.pop()
        url = base_url + href
        # this opens the first page
        try:
            html = requests.get(url, headers=generate_headers()).content
            soup = bs4.BeautifulSoup(html, features="html.parser")
            hrefs = hrefs.union(get_hrefs(soup)).difference(visited_hrefs)
            proxy_list += get_proxies(soup)
        except:
            pass
        visited_hrefs.add(href)
    return proxy_list


# 0
def scrape_89ip() -> Collection[str]:  # chinese proxies
    url = "https://www.89ip.cn/tqdl.html?num=9999&address=&kill_address=&port=&kill_port=&isp="
    res = requests.get(url, headers=generate_headers())
    soup = bs4.BeautifulSoup(res.text, features='html.parser')
    elems = soup.find('div', attrs={'class': 'fly-panel'}).find('div').find_all(text=True)
    proxies = set()
    for e in elems:
        proxy_str = e.strip()
        if not proxy_str:
            continue
        proxies.add(proxy_str)
    return proxies


# 1
def scrape_proxynova() -> Collection[str]:
    proxies = set()
    url = "https://www.proxynova.com/proxy-server-list/"
    try:
        res = requests.get(url, headers=generate_headers())
        soup = bs4.BeautifulSoup(res.text, features='html.parser')
        table = soup.find(id="tbl_proxy_list")
        if not table:
            return []
        table_body = table.find("tbody")
        if not table_body:
            return []
        rows = table_body.find_all("tr")
        if not rows:
            return []
        for row in rows:
            columns = row.find_all("td")
            if len(columns) < 2:
                continue
            col = columns[0].find("abbr")
            texts = [t.strip() for t in col.find_all(text=True)]
            host = None
            for text in texts:
                if not text:
                    continue
                ip = re.search(IPv4_REGEX, text)
                if not ip:
                    continue
                else:
                    host = ip[0].strip()
                    break
            if not host:
                host = col['title'].strip()
                if not valid_ip(host):
                    continue
            port = columns[1].text.strip()
            proxies.add(f"{host}:{port}")
    except Exception as ex:
        print(f"'{ex}' while handling '{url}'.")
    return proxies


# 2
def scrape_myproxy():
    urls = [
        "https://www.my-proxy.com/free-socks-4-proxy.html",
        "https://www.my-proxy.com/free-socks-5-proxy.html",
        "https://www.my-proxy.com/free-proxy-list.html",
        "https://www.my-proxy.com/free-proxy-list-2.html",
        "https://www.my-proxy.com/free-proxy-list-3.html",
        "https://www.my-proxy.com/free-proxy-list-4.html",
        "https://www.my-proxy.com/free-proxy-list-5.html",
        "https://www.my-proxy.com/free-proxy-list-6.html",
        "https://www.my-proxy.com/free-proxy-list-7.html",
        "https://www.my-proxy.com/free-proxy-list-8.html",
        "https://www.my-proxy.com/free-proxy-list-9.html",
        "https://www.my-proxy.com/free-proxy-list-10.html",
    ]
    proxies = set()
    for url in urls:
        try:
            res = requests.get(url, headers=generate_headers())
            if res.status_code == 200:
                text = res.content.decode("utf-8")
                proxies = proxies.union(find_host_port_pairs(text))
        except Exception as ex:
            print(f"'{ex}' while handling '{url}'.")
    return proxies


# 3
def scrape_freeproxy_cz(pages=20) -> Collection[str]:
    regex = fr'(?<=Base64.decode\("){BASE64_WORD_REGEX}(?="\))'
    base_url = "http://free-proxy.cz/en/proxylist/main/uptime/"
    proxies = set()
    for page in range(1, pages+1):
        url = base_url + str(page)
        try:
            res = requests.get(url, headers=generate_headers())
            soup = bs4.BeautifulSoup(res.text, features='html.parser')
            proxy_list = soup.find(id="proxy_list")
            if not proxy_list:
                continue
            tbody = proxy_list.find('tbody')
            if not tbody:
                continue
            rows = tbody.find_all("tr")
            if not rows:
                continue
            for row in rows:
                columns = row.find_all("td")
                if not columns or len(columns) < 2:
                    continue
                host_column = columns[0]
                script = host_column.find("script")
                if not script or not script.string:
                    continue
                encoded_host = re.search(regex, script.string)
                if not encoded_host:
                    continue
                encoded_host = encoded_host[0]
                try:
                    host = b64decode(encoded_host).decode()
                    if not valid_ip(host):
                        continue
                except Exception as ex:
                    print(f"'{ex}' while decoding '{encoded_host}'.")
                    continue
                port_column = columns[1]
                if not port_column:
                    continue
                port_span = port_column.find("span")
                if not port_span or not port_span.text:
                    continue
                port = port_span.text
                proxy = f"{host}:{port}"
                proxies.add(proxy)
        except Exception as ex:
            print(f"'{ex}' while handling {url}.")
            pass
    return proxies


# 4
def scrape_ipaddress() -> Collection[str]:
    proxies = set()
    url = "https://www.ipaddress.com/proxy-list/"
    try:
        res = requests.get(url, headers=generate_headers())
        soup = bs4.BeautifulSoup(res.text, features='html.parser')
        table_body = soup.find("tbody")
        if not table_body:
            return []
        rows = table_body.find_all("tr")
        if not rows:
            return []
        for row in rows:
            columns = row.find_all("td")
            if not columns:
                continue
            column = columns[0]
            if not column.text:
                continue
            proxy = column.text.strip()
            if proxy and valid_ip(proxy.split(":")[0]):
                proxies.add(proxy)
    except Exception as ex:
        print(f"'{ex}' while handling {url}.")
    return proxies


# 5
def scrape_proxylistplus(pages=6) -> Collection[str]:
    proxies = set()
    base_url = "https://list.proxylistplus.com/Fresh-HTTP-Proxy-List-"
    for page in range(1, pages+1):
        url = base_url + str(page)
        try:
            res = requests.get(url, headers=generate_headers())
            if res.status_code != 200:
                continue
            soup = bs4.BeautifulSoup(res.text, features='html.parser')
            rows = soup.find_all("tr")  # find all rows in the document
            if not rows:
                continue
            for row in rows:
                columns = row.find_all("td")
                if not columns or len(columns) < 3:
                    continue
                host = columns[1].text
                if not valid_ip(host):
                    continue
                port = columns[2].text
                proxy = f"{host}:{port}"
                proxies.add(proxy)
        except Exception as ex:
            print(f"'{ex}' while handling {url}.")
    return proxies


# 6
def scrape_proxyrack(pages=5):
    proxies = set()
    base_url = "https://www.proxyrack.com/proxyfinder/proxies.json"
    step = 50
    for page in range(1, pages+1):
        offset = (page - 1) * step
        url = base_url + f"?page={page}&perPage={step}&offset={offset}"
        try:
            headers = generate_headers()
            headers['Accept'] = "application/json, text/javascript, */*"
            res = requests.get(url, headers=headers)
            if res.status_code != 200:
                continue
            try:
                data = res.json()["records"]
                for proxy_json in data:
                    host = proxy_json['ip']
                    if not valid_ip(host):
                        continue
                    port = proxy_json['port']
                    proxy_str = f"{host}:{port}"
                    proxies.add(proxy_str)
            except json.JSONDecodeError:
                continue
            except KeyError:
                continue
        except Exception as ex:
            print(f"'{ex}' while handling url {url}.")
    return proxies


# 7
def scrape_proxy_list_download() -> Collection[str]:
    proxies = set()
    urls = [
        "https://www.proxy-list.download/api/v0/get?l=en&t=socks4",
        "https://www.proxy-list.download/api/v0/get?l=en&t=http",
        "https://www.proxy-list.download/api/v0/get?l=en&t=socks5",
        "https://www.proxy-list.download/api/v0/get?l=en&t=https",
    ]
    for url in urls:
        try:
            headers = generate_headers()
            headers["Accept"] = "*/*"
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                try:
                    data = res.json()[0]["LISTA"]
                    for record in data:
                        try:
                            host = record["IP"]
                            if not valid_ip(host):
                                continue
                            port = record["PORT"]
                            proxy = f"{host}:{port}"
                            proxies.add(proxy)
                        except KeyError:
                            continue
                except json.JSONDecodeError:
                    pass
                except KeyError:
                    pass
                except IndexError:
                    pass
        except:
            pass
    return proxies


# 8
def scrape_spysone() -> Collection[str]:
    sources = [
        "https://spys.one/en/http-proxy-list/",
        "https://spys.one/en/anonymous-proxy-list/",
        "https://spys.one/en/socks-proxy-list/",
        "https://spys.one/en/free-proxy-list/"
    ]

    def get_port_encoding(soup: bs4.BeautifulSoup) -> dict[str, int]:
        script = soup.find("body").find_all("script")[2].string
        encodings = dict()
        expressions = script.split(";")
        for expression in expressions:
            expression = expression.strip()
            if expression:
                variable, value = expression.split("=", 1)
                try:
                    value = int(value)
                    encodings[variable] = value
                except ValueError:
                    value1, value2 = value.split("^", 1)
                    # which is int?
                    try:
                        value1 = int(value1)
                        value1_is_int = True
                    except ValueError:
                        value1_is_int = False
                    try:
                        value2 = int(value2)
                        value2_is_int = True
                    except ValueError:
                        value2_is_int = False
                    if not value1_is_int:  # then it means that value1 is a variable
                        value1 = encodings[value1]
                    if not value2_is_int:  # then value2 is a variable
                        value2 = encodings[value2]
                    # those MUST be known previously from the script, this is the way interpreter works
                    # now both value1 and value2 are ints, so do the maths
                    encodings[variable] = value1 ^ value2
        return encodings

    def extract_proxy(row: bs4.BeautifulSoup, encoding: dict[str, int]) -> Optional[str]:
        columns = row.find_all("td")
        if not columns:
            return None
        column = columns[0]
        font = column.find("font")
        if not font:
            return None
        script = font.find("script")
        if not script or not script.string:
            return None
        regex = r"(?<=\()[A-z0-9^]+(?=\))"
        encoded = re.findall(regex, script.string)
        if not encoded:
            return None
        port = ""
        for e in encoded:
            try:
                a, b = e.split("^", 1)
                a = a.strip()
                b = b.strip()
                c = encoding[a] ^ encoding[b]
                port += str(c)
            except ValueError or KeyError:
                # exception during decoding either part of the port
                try:
                    e = e.strip()
                    c = encoding[e]
                    port += str(c)
                except KeyError:
                    # unable to decode the port
                    return None
        if not port:  # unable to decode
            return None
        host = font.text
        if not valid_ip(host):
            return None
        return f"{host}:{port}"

    def scrape_url(url: str) -> Collection[str]:
        # stage one: get token + get proxies
        data = {"xpp": "5", "xf1": "0", "xf2": "0", "xf3": "0", "xf4": "0", "xf5": "0"}
        proxies = set()

        response = requests.get(url, headers=generate_headers())
        if response.status_code != 200:
            return proxies
        soup = bs4.BeautifulSoup(response.text, features='html.parser')
        rows1 = soup.find_all(attrs={"class": "spy1xx"})
        rows2 = soup.find_all(attrs={"class": "spy1x"})
        encoding = get_port_encoding(soup)

        for row in rows1:
            proxy = extract_proxy(row, encoding)
            if proxy:
                proxies.add(proxy)
        for row in rows2:
            proxy = extract_proxy(row, encoding)
            if proxy:
                proxies.add(proxy)

        token = soup.find("input", attrs={"type": "hidden", "name": "xx0"})
        if not token:  # if no token is found, then whatever, return what there is
            return proxies
        try:
            token = token["value"]
        except KeyError:  # html tag has no value attribute
            return proxies
        data["xx0"] = token  # this is a one time token to access more proxies

        # now the second stage: get 500 proxies at once
        response = requests.post(url, headers=generate_headers(), data=data)
        if response.status_code != 200:
            return proxies
        soup = bs4.BeautifulSoup(response.text, features='html.parser')
        encoding = get_port_encoding(soup)
        if not encoding:
            return proxies
        # now scrape all rows containing proxies
        rows1 = soup.find_all(attrs={"class": "spy1xx"})
        rows2 = soup.find_all(attrs={"class": "spy1x"})
        for row in rows1:
            proxy = extract_proxy(row, encoding)
            if proxy:
                proxies.add(proxy)
        for row in rows2:
            proxy = extract_proxy(row, encoding)
            if proxy:
                proxies.add(proxy)
        return proxies
    res = set()
    for url in sources:
        try:
            r = scrape_url(url)
            res = res.union(r)
        except Exception as ex:
            print(f"'{ex}' while scraping {url}.")
            pass  # ignore any exception that comes here
    return res


# 9
# website's got ~300-ish proxies, most of which are rather dead than alive
def scrape_xseo_in() -> Collection[str]:
    # url encoded data
    data = {"submit": "Показать по 150 прокси на странице"}
    # means that we grab 150 proxies at once, ignoring the initial list

    def get_port_encoding(soup: bs4.BeautifulSoup) -> dict[str, int]:
        scripts = soup.find_all("script")
        if not scripts or len(scripts) < 2:
            return {}
        script = scripts[1]
        script = script.string
        expressions = script.split(";")
        encoding_table = dict()
        for expression in expressions:
            expression = expression.strip()
            if expression:
                try:
                    variable, value = expression.split("=", 1)
                    variable = variable.strip()
                    value = int(value.strip())
                    encoding_table[variable] = value
                except ValueError:
                    pass  # couldn't split into 2 parts OR couldn't parse value to int
                    # yet, we shouldn't raise an error, since some ports can still be parsed
        return encoding_table

    def extract_proxy(row: bs4.BeautifulSoup, encoding: Optional[dict[str, int]]) -> Optional[str]:
        columns = row.find_all("td")
        if not columns:
            return None
        column = columns[0]
        font = column.find("font")
        if not font:
            return None
        if encoding:
            script = font.find("script")
            if not script or not script.string:
                return None
            js_code = script.string
            regex = r"(?<=document.write\().+(?=\))"
            inner_code = re.search(regex, js_code)
            if not inner_code:
                return None
            variables = inner_code[0].split("+")[1:]
            if not variables:
                return None
            port = "".join([str(encoding[var]) for var in variables])
            try:
                port = int(port)
            except ValueError:
                return None
            if not port or not valid_port(port):
                return None
            host = font.text
            if not host:
                return None
            host = host.strip()
            if host[-1] == ":":
                host = host[:-1]
            if not valid_ip(host):
                return None
            return f"{host}:{port}"
        else:
            proxy = font.text
            if not proxy or not valid_host_port_pair(proxy):
                return None
            return proxy

    def scrape_page(url: str, free: bool) -> Collection[str]:
        proxies = set()
        response = requests.post(url, headers=generate_headers(), data=data)
        if response.status_code != 200:
            return proxies

        soup = bs4.BeautifulSoup(response.text, features='html.parser')
        if not free:
            encoding = get_port_encoding(soup)
            if not encoding:
                return proxies
        else:
            encoding = None

        rows1 = soup.find_all("tr", attrs={"class": "cls81"})
        rows2 = soup.find_all("tr", attrs={"class": "cls8"})
        for row in rows1:
            p = extract_proxy(row, encoding)
            if p:
                proxies.add(p)
        for row in rows2:
            p = extract_proxy(row, encoding)
            if p:
                proxies.add(p)
        return proxies

    r = set()
    url = "https://xseo.in/proxylist"
    try:
        res = scrape_page("https://xseo.in/proxylist", False)
        r = r.union(res)  # put proxies to the set
    except Exception as ex:
        print(f"'{ex}' while scraping {url}.")
    url = "https://xseo.in/freeproxy"
    try:
        res = scrape_page(url, True)
        r = r.union(res)
    except Exception as ex:
        print(f"'{ex}' while scraping {url}.")
    return r
