#!/usr/bin/env python
"""PowerDNS remote http backend for Zookeeper/finagle serversets.

Not yet implemented:
    - NS records
    - Instrumentation (Prometheus metrics)
    - Some kind of status page
    - Correct handling of ANY queries(?)
    - Testing
    - Documentation
"""

import itertools
import socket
import time

from pyglib import app
from pyglib import flags
from pyglib import log
from twitter.common import http
from twitter.common.exceptions import ExceptionalThread
from twitter.common.http.diagnostics import DiagnosticsEndpoints
from twitter.common.zookeeper import kazoo_client
from twitter.common.zookeeper.serverset import serverset

import metrics

FLAGS = flags.FLAGS

flags.DEFINE_string('zk', 'localhost:2181/',
                    'Zookeeper ensemble (comma-delimited, optionally '
                    'followed by /chroot path)')
flags.DEFINE_string('domain', 'zk.example.com',
                    'Serve records for this DNS domain.')
flags.DEFINE_integer('port', 8080, 'HTTP listen port.')
flags.DEFINE_string('listen', '0.0.0.0',
                    'IP address to listen for http connections.')

flags.DEFINE_integer('ttl', 60, 'TTL for normal records.')
flags.DEFINE_integer('soa_ttl', 300, 'TTL for SOA record itself.')
flags.DEFINE_string('soa_nameserver', '',
                    'Authoritative nameserver for the SOA record. '
                    'Uses the system hostname if left blank.')
flags.DEFINE_string('soa_email', '',
                    'Email address field for the SOA record. '
                    'Autogenerated if left blank.')
flags.DEFINE_integer('soa_refresh', 1200,
                     'Refresh field for the SOA record.')
flags.DEFINE_integer('soa_retry', 180,
                     'Retry field for the SOA record.')
flags.DEFINE_integer('soa_expire', 86400,
                     'Expire field for the SOA record.')
flags.DEFINE_integer('soa_nxdomain_ttl', 60,
                     'Negative caching TTL for the SOA record.')


class SOAData(object):
    """DNS SOA data representation."""

    def __init__(self, ttl, ns1, email, refresh, retry, expire, nxdomain_ttl):
        self.ttl = int(ttl)
        self.ns1 = str(ns1)
        self.email = str(email)
        self.refresh = int(refresh)
        self.retry = int(retry)
        self.expire = int(expire)
        self.nxdomain_ttl = int(nxdomain_ttl)

    def __str__(self):
        # Can't decide if this is cool or a terrible hack.
        return ('%(ns1)s %(email)s %(refresh)s 1 '
                '%(retry)s %(expire)s %(nxdomain_ttl)s') % self.__dict__


def dnsresponse(data):
    """Construct a response for the PowerDNS remote backend.

    Remote api docs:
        https://doc.powerdns.com/md/authoritative/backend-remote/
    """
    resp = {'result': list(data or [])}
    log.debug('DNS response: %s', resp)
    return resp


class ZknsServer(http.HttpServer,
                 DiagnosticsEndpoints,
                 metrics.MetricsEndpoints):
    """Zookeeper-backed powerdns remote api backend"""

    plugins = [metrics.MetricsPlugin()]

    def __init__(self, zk_handle, domain, ttl, soa_data):
        self.zkclient = zk_handle
        self.domain = domain.strip('.')
        self.soa_data = soa_data
        self.ttl = ttl

        http.HttpServer.__init__(self)
        DiagnosticsEndpoints.__init__(self)
        metrics.MetricsEndpoints.__init__(self)

    @http.route('/dnsapi/lookup/<qname>/<qtype>', method='GET')
    def dnsapi_lookup(self, qname, qtype):
        """pdns lookup api"""
        log.debug('QUERY: %s %s', qname, qtype)
        # TODO: better ANY handling (what's even correct here?)
        if qtype == 'ANY':
            return dnsresponse(itertools.chain(
                self.a_lookup(qname),
                self.ns_lookup(qname),
                self.soa_lookup(qname),
                self.srv_lookup(qname)))
        elif qtype == 'A':
            return dnsresponse(self.a_lookup(qname))
        elif qtype == 'NS':
            return dnsresponse(self.ns_lookup(qname))
        elif qtype == 'SOA':
            return dnsresponse(self.soa_lookup(qname))
        elif qtype == 'SRV':
            return dnsresponse(self.srv_lookup(qname))
        else:
            return dnsresponse(False)

    @staticmethod
    @http.route('/dnsapi/getDomainMetadata/<qname>/<qkind>', method='GET')
    def dnsapi_getdomainmetadata(qname, qkind):
        """pdns getDomainMetadata api"""
        log.debug('QUERY: %s %s', qname, qkind)
        if qkind == 'SOA-EDIT':
            # http://jpmens.net/2013/01/18/understanding-powerdns-soa-edit/
            return dnsresponse(['EPOCH'])
        else:
            return dnsresponse(False)

    def resolve_hostname(self, hostname):
        """Resolve a hostname to a list of serverset instances."""
        zkpaths = construct_paths(hostname, self.domain) or []
        for (zkpath, shard) in zkpaths:
            sset = list(serverset.ServerSet(self.zkclient, zkpath))
            if not sset:
                continue
            elif shard is None:
                return sset
            else:
                for ss_instance in sset:
                    if ss_instance.shard == shard:
                        return [ss_instance]
                continue
        return []  # Nothing found :(

    def a_lookup(self, qname):
        """Handle A record lookup."""
        instances = self.resolve_hostname(qname)
        for x in instances:
            yield a_response(qname, x.service_endpoint.host, ttl=self.ttl)

    def ns_lookup(self, qname):
        """Handle NS record lookup."""
        if qname.lower().strip('.') == self.domain:
            yield ns_response(qname, target=self.soa_data.ns1, ttl=self.ttl)

    def soa_lookup(self, qname):
        """Handle SOA record lookup."""
        if not qname.lower().strip('.').endswith(self.domain):
            log.debug('nope')
            return
        yield soa_response(self.domain, self.soa_data.ttl, str(self.soa_data))

    def srv_lookup(self, qname):
        """Handle SRV record lookup.

        Currently only works for serverset instances that have a shard number.
        """
        # Convert the hostname to the form it would be in for an A lookup
        # e.g. _http._tcp.job.env.role.cluster.subdomain.example.com
        # becomes         job.env.role.cluster.subdomain.example.com

        _service, _proto, a_name = qname.lower().split('.', 2)
        if not (_service.startswith('_') and _proto in ['_tcp', '_udp']):
            return
        service = _service[1:]
        instances = self.resolve_hostname(a_name)
        for instance in instances:
            if not instance.shard:
                continue
            shard_a = '.'.join([instance.shard, a_name])
            endpoint = instance.additional_endpoints.get(service)
            if endpoint:
                yield srv_response(qname, shard_a, endpoint.port, ttl=self.ttl)


def ns_response(qname, target, ttl):
    """Generate a pdns NS query response."""
    return {'qtype': 'NS',
            'qname': str(qname),
            'ttl': int(ttl),
            'content': str(target)}


def srv_response(srv_name, target, port, ttl, priority=0, weight=0):
    """Generate a pdns SRV query response."""
    return {'qtype': 'SOA',
            'qname': srv_name,
            'ttl': ttl,
            'content': ' '.join(
                [srv_name, ttl, 'IN SRV', priority, weight, port, target])}


def soa_response(domain, ttl, content):
    """Generate a pdns SOA query response."""
    return {'qtype': 'SOA',
            'qname': str(domain),
            'ttl': int(ttl),
            'content': str(content)}


def a_response(qname, ip_addr, ttl):
    """Generate a pdns A query response."""
    return {'qtype': 'A',
            'qname': qname,
            'ttl': ttl,
            'content': ip_addr}


def construct_paths(hostname, basedomain=None):
    """Generate paths to search for a serverset in Zookeeper.

    Yields tuples of (<subpath to search>, <shard number or None>).

    >>> construct_paths('0.job.foo.bar.bas.buz.basedomain.example.com',
                        'basedomain.example.com')
    ('buz/bas/bar/foo/job', 0)
    ('buz/bas/bar/job.foo', 0)
    ('buz/bas/job.foo.bar', 0)
    ('buz/job.foo.bar.bas', 0)
    ('job.foo.bar.bas.buz', 0)
    """
    # e.g. 0.job.foo.bar.bas.buz.basedomain.example.com
    if basedomain:
        qrec, _, _ = hostname.strip('.').rpartition(basedomain)
    else:
        qrec = hostname.strip('.')
    # -> 0.job.foo.bar.bas.buz

    path_components = list(reversed(qrec.strip('.').split('.')))
    # -> ['buz', 'bas', 'bar', 'foo', 'job', '0']

    # maybe it has a shard number?
    try:
        shard = int(path_components[-1])
        path_components = path_components[:-1]
    except ValueError:
        shard = None

    while path_components:
        yield ('/'.join(path_components), shard)
        if len(path_components) == 1:
            return

        # Extend the last element with the previous one
        # e.g. ['a', 'b', 'c', 'f.e.d'] --> ['a', 'b', 'f.e.d.c']
        elem = '.'.join([path_components.pop(), path_components.pop()])
        path_components.append(elem)


def wait_forever():
    """An interruptable do-nothing-forever sleep."""
    while True:
        time.sleep(60)


def main(_):
    """Main"""
    zkconn = kazoo_client.TwitterKazooClient(FLAGS.zk)
    zkconn.start()

    soa_data = SOAData(ttl=FLAGS.soa_ttl,
                       ns1=FLAGS.soa_nameserver or socket.getfqdn(),
                       email=FLAGS.soa_email or 'root.%s' % FLAGS.domain,
                       refresh=FLAGS.soa_refresh,
                       retry=FLAGS.soa_retry,
                       expire=FLAGS.soa_expire,
                       nxdomain_ttl=FLAGS.soa_nxdomain_ttl)

    server = ZknsServer(zk_handle=zkconn,
                        domain=FLAGS.domain,
                        ttl=FLAGS.ttl,
                        soa_data=soa_data)

    thread = ExceptionalThread(
        target=lambda: server.run(FLAGS.listen,
                                  FLAGS.port,
                                  server='cherrypy'))
    thread.daemon = True
    thread.start()

    try:
        wait_forever()
    except KeyboardInterrupt:
        log.fatal('KeyboardInterrupt! Shutting down.')


if __name__ == '__main__':
    app.run()
