package com.alibaba.nacossync.it.infra;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

/**
 * Host ports for the parts of the test stack that cannot use Testcontainers' random mappings.
 */
final class HostPorts {

    /**
     * Nacos HTTP host ports come only from this dedicated range (gRPC lands on +1000, i.e. 41000-41999 by default).
     * It keeps clear of the fixed host ports of the local E2E docker-compose stack, which may run at the same time
     * and leaves them briefly free during "compose down / up": 8848, 9848, 18848, 19848, 8083, 18033, 8900, 2181,
     * 28080, 28081, 18032-18099, 20880-20882, 32002, 32012 and 32022. Override with -Dit.nacos.port.min and
     * -Dit.nacos.port.max.
     */
    private static final int NACOS_PORT_MIN = Integer.getInteger("it.nacos.port.min", 40000);

    private static final int NACOS_PORT_MAX = Integer.getInteger("it.nacos.port.max", 40999);

    private static final Set<Integer> HANDED_OUT = new HashSet<>();

    private HostPorts() {
    }

    /**
     * Scans the dedicated Nacos range for a port P such that P and P + offset are both free, and reserves both for
     * this JVM. Containers with random mappings can still take a port in the range between this check and the
     * Nacos container start; the caller then retries and the scan moves on to the next pair.
     */
    static synchronized int freeNacosPortPair(int offset) {
        if (NACOS_PORT_MIN > NACOS_PORT_MAX) {
            throw new IllegalArgumentException(
                    "it.nacos.port.min (" + NACOS_PORT_MIN + ") > it.nacos.port.max (" + NACOS_PORT_MAX + ")");
        }
        for (int port = NACOS_PORT_MIN; port <= NACOS_PORT_MAX; port++) {
            if (isAvailable(port) && isAvailable(port + offset)) {
                HANDED_OUT.add(port);
                HANDED_OUT.add(port + offset);
                return port;
            }
        }
        throw new IllegalStateException("No free Nacos host port pair (P, P + " + offset + ") with P in "
                + NACOS_PORT_MIN + "-" + NACOS_PORT_MAX);
    }

    /** An OS-assigned ephemeral port, outside every fixed port the E2E stack uses. */
    static int ephemeral() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isAvailable(int port) {
        if (HANDED_OUT.contains(port)) {
            return false;
        }
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress("0.0.0.0", port));
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
