"""HTTP-first, public-address-only capture. Never executes downloaded content."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import http.client
import io
import ipaddress
import socket
import ssl
import time
from urllib.parse import urljoin, urlsplit, urlunsplit


class CaptureBlocked(ValueError):
    pass


def resolve_public_url(url, allowed_hosts):
    parsed=urlsplit(url)
    host=(parsed.hostname or "").encode("idna").decode("ascii").lower()
    if parsed.scheme not in {"http","https"} or not host or parsed.username or parsed.password:
        raise CaptureBlocked("unsupported_url")
    if any(ord(c)<33 or ord(c)==127 for c in url) or "\\" in url:
        raise CaptureBlocked("invalid_url")
    if host not in allowed_hosts or parsed.port not in {None,80 if parsed.scheme=="http" else 443}:
        raise CaptureBlocked("host_or_port_not_approved")
    port=parsed.port or (443 if parsed.scheme=="https" else 80)
    addresses=sorted({r[4][0] for r in socket.getaddrinfo(host,port,type=socket.SOCK_STREAM)})
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise CaptureBlocked("non_public_address")
    return parsed,host,port,addresses[0]


def _fetch_once(url, allowed_hosts, deadline, max_bytes):
    parsed,host,port,address=resolve_public_url(url,allowed_hosts)
    remaining=deadline-time.monotonic()
    if remaining<=0:
        raise TimeoutError("capture_deadline")
    conn=http.client.HTTPConnection(host,port,timeout=min(remaining,15))
    response=None
    # Pin the validated DNS result. HTTPS SNI and verification still use hostname.
    sock=socket.create_connection((address,port),timeout=min(remaining,15))
    try:
        if parsed.scheme=="https":
            sock=ssl.create_default_context().wrap_socket(sock,server_hostname=host)
        conn.sock=sock
        target=urlunsplit(("","",parsed.path or "/",parsed.query,""))
        conn.request("GET",target,headers={"Host":host,"User-Agent":"CivicEvidenceLab/2.0 research capture","Accept-Encoding":"identity","Connection":"close"})
        # Keep ownership of the socket until the bounded body read completes.
        # HTTPConnection.getresponse() closes it immediately on Connection: close.
        response=http.client.HTTPResponse(sock,method="GET")
        response.begin()
        headers=response.getheaders()
        length=response.getheader("Content-Length")
        if length is not None and int(length)>max_bytes:
            raise CaptureBlocked("response_size_limit")
        body=bytearray()
        while True:
            remaining=deadline-time.monotonic()
            if remaining<=0:
                raise TimeoutError("capture_deadline")
            sock.settimeout(min(remaining,15))
            block=response.read1(min(65536,max_bytes-len(body)+1))
            if not block:
                break
            body.extend(block)
            if len(body)>max_bytes:
                raise CaptureBlocked("response_size_limit")
        return response.status,response.reason,headers,bytes(body)
    finally:
        if response is not None:
            response.close()
        conn.close()
        sock.close()


def capture_http(url, *, allowed_hosts, archive, timeout=30, max_bytes=8*1024**2, max_redirects=3):
    """Return a capture envelope including failed HTTP snapshots; no truth verdict."""
    if not 0<timeout<=120 or not 0<max_bytes<=32*1024**2 or not 0<=max_redirects<=5:
        raise ValueError("Invalid capture limits")
    from warcio.warcwriter import WARCWriter
    from warcio.statusandheaders import StatusAndHeaders
    started=datetime.now(timezone.utc).isoformat()
    deadline=time.monotonic()+timeout
    current=url
    chain=[]
    warc=io.BytesIO()
    writer=WARCWriter(warc,gzip=True)
    for step in range(max_redirects+1):
        status,reason,headers,body=_fetch_once(current,set(allowed_hosts),deadline,max_bytes)
        record=writer.create_warc_record(current,"response",payload=io.BytesIO(body),length=len(body),
            http_headers=StatusAndHeaders(f"{status} {reason}",headers,protocol="HTTP/1.1"))
        try:
            writer.write_record(record)
        finally:
            record.raw_stream.close()
        chain.append({"url":current,"status":status,"sha256":hashlib.sha256(body).hexdigest()})
        header_map={k.lower():v for k,v in headers}
        if status in {301,302,303,307,308} and header_map.get("location"):
            if step==max_redirects:
                raise CaptureBlocked("redirect_limit")
            current=urljoin(current,header_map["location"])
            continue
        original=archive.store_bytes(body) if body else None
        warc_file=archive.store_bytes(warc.getvalue())
        access=status in {401,403,429} or any(marker in body[:500000].lower() for marker in (b"captcha",b"verify you are human"))
        return {"source_url":url,"final_url":current,"fetched_at":started,"completed_at":datetime.now(timezone.utc).isoformat(),
                "http_status":status,"status":"needs_user_access" if access else "captured" if 200<=status<300 else "http_error",
                "sha256":original.sha256 if original else None,"storage_path":str(original.path) if original else None,
                "byte_size":len(body),"media_type":header_map.get("content-type","application/octet-stream"),
                "warc_path":str(warc_file.path),"warc_sha256":warc_file.sha256,"redirect_chain":chain,
                "parser_version":"http-capture-v1","publication_allowed":False}
    raise CaptureBlocked("redirect_limit")
