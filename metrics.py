#!/usr/bin/env python
"""bottlepy plugin for prometheus metrics."""

import functools

import prometheus_client
from twitter.common import http

INF = float('inf')

Counter = prometheus_client.Counter
Histogram = prometheus_client.Histogram


def powers_of(logbase, count, lower=0, include_zero=True):
    """List powers of logbase (from logbase**lower)."""
    if not include_zero:
        return [logbase ** i for i in range(lower, count+lower)] + [INF]
    else:
        return [0] + [logbase ** i for i in range(lower, count+lower)] + [INF]


class Metrics(object):
    RequestCounter = Counter(
        'http_requests_total', 'Total number of HTTP requests.',
        ['method', 'scheme'])
    ResponseCounter = Counter(
        'http_responses_total', 'Total number of HTTP responses.',
        ['status'])
    LatencyHistogram = Histogram(
        'http_latency_seconds', 'Overall HTTP transaction latency.')
    RequestSizeHistogram = Histogram(
        'http_requests_body_bytes',
        'Breakdown of HTTP requests by content length.',
        buckets=powers_of(2, 30))
    ResponseSizeHistogram = Histogram(
        'http_responses_body_bytes',
        'Breakdown of HTTP responses by content length.',
        buckets=powers_of(2, 30))


class MetricsPlugin(http.Plugin):

    name = 'PrometheusMetrics'

    def apply(self, callback, route):
        @Metrics.LatencyHistogram.time()
        @functools.wraps(callback)
        def wrapped_callback(*args, **kwargs):

            Metrics.RequestCounter.labels(
                http.request.method,
                http.request.get('wsgi.url_scheme')).inc()
            if http.request.content_length is not None:
                Metrics.RequestSizeHistogram.observe(
                    http.request.content_length)

            body = callback(*args, **kwargs)

            status_code = http.response.status_code
            Metrics.ResponseCounter.labels(status_code).inc()

            try:
                content_length = len(body)
                Metrics.ResponseSizeHistogram.observe(content_length)
            except (ValueError, TypeError):
                pass

            return body
        return wrapped_callback


class MetricsEndpoints(object):

    @http.route('/metrics', method='ANY')
    def metrics(self):
        http.response.content_type = prometheus_client.CONTENT_TYPE_LATEST
        return prometheus_client.generate_latest()
