package org.example.config;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Exposes all Prometheus counters on http://localhost:8082/metrics.
 *
 * Prometheus is pull-based: it periodically reads that page and stores the
 * numbers with timestamps. This mini HTTP server is separate from Spring's
 * web server (port 8081) and serves only metrics.
 */
@Configuration
public class PrometheusConfig {

    @PostConstruct // runs once, right after the application starts
    public void init() throws IOException {
        // Also expose JVM metrics (heap memory, threads, GC) for free.
        DefaultExports.initialize();
        // Port 8082 must match the target in prometheus.yml.
        new HTTPServer(8082);
    }
}
