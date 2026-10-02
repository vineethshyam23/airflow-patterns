#!/usr/bin/env python3

import hashlib
import re
import sys
import ujson
from argparse import ArgumentParser
from urllib.parse import parse_qsl, quote_plus, urlparse
from w3lib.url import canonicalize_url

url = "https://www.duckduckgo.com/search?q=hello+world"

wwwre = re.compile(".*www\.")
toplevels = [
    re.compile(".*\.de"),
    re.compile(".*\.com"),
    re.compile(".*\.pl"),
    re.compile(".*\.at"),
    re.compile(".*\.info"),
    re.compile(".*\.it"),
    re.compile(".*\.eu"),
    re.compile(".*\.berlin"),
    re.compile(".*\.bar"),
    re.compile(".*\.menu"),
    re.compile(".*\.site"),
    re.compile(".*\.pt"),
    re.compile(".*\.net"),
    re.compile(".*\.va"),
    re.compile(".*\.nl")
]


def from_parts(urldict):
    url = "".join(
        [
            urldict["scheme"],
            "://",
            urldict["www"],
            urldict["domain"],
            urldict["path"],
        ]
    )
    if urldict["query"]:
        url = "".join([url, "?", urldict["query"]])

    return url


def fingerprint(url, shorten=True, clean_url=True):
    def get_url_hash(url, shorten=True):
        # fp = hashlib.sha1()
        fp = hashlib.md5()
        fp.update("GET".encode())
        fp.update(url.encode())
        fp.update(b"")

        if shorten == True:
            return fp.hexdigest()[:32]
        return fp.hexdigest()

    if clean_url == True:
        url_clean = clean(url)
        url = concat_url_parts(url_clean)

    if shorten == True:
        return get_url_hash(url)
    return get_url_hash(url, shorten=False)


def clean(url, split_query=False, calc_fingerprint=True):
    """
    Cleans a single url by splitting each
    in their parts (scheme,netloc, path) and
    simplifies the parts to make them unambigious
    """
    if url:
        
        try:
            parts = urlparse(canonicalize_url(url))
            domain = parts.netloc
            path = parts.path
            www = url.find("www.") >= 0 and "www." or ""
            scheme = "http" if url.find("http://") >= 0 else "https"
            if not domain:
                for toplevel in toplevels:
                    if toplevel.findall(path):
                        domain = toplevel.findall(path)[0]
                        path = toplevel.sub("", path)
            domain = wwwre.sub("", domain).split(":")[0]

            if parts.path:
                if path.endswith("index.html"):
                    path = path.replace("/index.html", "")
                elif path.endswith("index.htm"):
                    path = path.replace("/index.htm", "")
                if path.endswith(".html"):
                    path = path.replace(".html", "")
                elif path.endswith(".htm"):
                    path = path.replace(".htm", "")
                elif path.endswith(".php"):
                    path = path.replace(".php", "")
                path = path.rstrip("/")
                if path:
                    path = path.startswith("/") and path or "/" + path
            else:
                path = ""

            if parts.query:
                query_parts = [
                    (k, v)
                    for k, v in sorted(parse_qsl(parts.query))
                    if (
                        not k.endswith("id")
                        and not k.endswith("token")
                        and not len(v) >= 64
                    )
                    or k == "id"
                    and len(v) <= 16
                ]
            else:
                query_parts = []
            query = query_parts

            if split_query == False:
                query_string = "&".join(
                    [
                        quote_plus(k) + "=" + quote_plus(v)
                        for k, v in sorted(query_parts)
                    ]
                )
                query = query_string

            urldict = {
                "scheme": scheme,
                "www": www,
                "domain": domain,
                "path": path,
                "query": query,
            }

            urldict["fingerprint"] = fingerprint(
                from_parts(urldict), clean_url=False
            )

            return urldict

        except Exception as e:
            sys.stderr.write(str(e) + "\n")
            return None


def gen_urlitem(url):
    return {"type": "url", "data": clean(url)}


def gen_domainitem(url):
    item = clean(url)
    return {
        "type": "domain",
        "data": {k: item[k] for k in {"scheme", "www", "domain"}},
    }


def readlines():
    while True:
        line = sys.stdin.readline().rstrip("\n")
        if not line:
            break
        yield line


def parse_args():
    parser = ArgumentParser()

    parser.add_argument(
        "-d",
        "--domain",
        action="store_true",
        help="Produces domain item",
    )
    parser.add_argument(
        "-u",
        "--url",
        action="store_true",
        help="Produces url item",
    )
    parser.add_argument(
        "-n",
        "--normalize",
        action="store_true",
        help="Prints normalized url",
    )
    return parser.parse_args(sys.argv[1:])

# if __name__ == "__main__":

#     args = parse_args()
#     # print("args", args)
#     # print("gen_urlitem(url): ", gen_urlitem(url))
#     if args.domain:
#         for url in readlines():
#             domainitem = gen_domainitem(url)
#             domainstr = domainitem['data']['scheme'] + '://' + domainitem['data']['www'] + domainitem['data']['domain']
#             #sys.stdout.write(ujson.dumps(domainitem) + "\n")
#             print("domainstr: ", domainstr)
#     elif args.url:
#         for url in readlines():
#             urlitem = gen_urlitem(url)
#             #sys.stdout.write(ujson.dumps(urlitem) + "\n")
#             print("urlitem: ", urlitem)
#     elif args.normalize:
#         for url in readlines():
#             urlstr = from_parts(gen_urlitem(url)["data"])
#             #sys.stdout.write(urlstr + "\n")
#             print("urlstr: ", urlstr)
#     else:
#         for url in readlines():
#             try:
#                 urlitem = gen_urlitem(url)
#             except:
#                 continue
#             #sys.stdout.write(ujson.dumps(urlitem) + "\n")
#             print("urlitem: ", urlitem)
