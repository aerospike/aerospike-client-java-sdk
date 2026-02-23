package com.aerospike.util;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;

public final class ASNodeController {

    private static final String DEFAULT_CONTAINER_NAME_PREFIX = "aerospike";
    private static final long DELAY_TIMEOUT_MS = 2000;
    private static final int DEFAULT_SCRIPT_TIMEOUT_SEC = 120;
    private final String containerNamePrefix;
    private static final String ASD_STOP = "./etc/init.d/aerospike stop";
    private static final String ASD_STATUS = "./etc/init.d/aerospike status";
    private static final String ASD_START = "./etc/init.d/aerospike start";
    private final String[] containers;
    private final int port;

    /**
     * wait for each node's service port to accept connections.
     * Env: CONTAINERS (newline-separated), TIMEOUT, SERVICE_PORT.
     */
    private static final String SCRIPT_WAIT_SERVICE_PORT = """
        CONTAINERS_ARR=()
        while IFS= read -r line; do [[ -n "$line" ]] && CONTAINERS_ARR+=("$line"); done <<< "$CONTAINERS"
        TIMEOUT=${TIMEOUT:-60}
        SERVICE_PORT=${SERVICE_PORT:-3000}
        echo 'Phase 1: Waiting for individual node readiness...'
        for container in "${CONTAINERS_ARR[@]}"; do
          echo "Waiting for $container..."
          elapsed=0
          ready="false"
          while ((elapsed < TIMEOUT)); do
            if docker exec -i "$container" bash -c "</dev/tcp/localhost/${SERVICE_PORT}" 2>/dev/null; then
              echo "  $container is ready (${elapsed}s)"
              ready="true"
              break
            fi
            sleep 2
            elapsed=$((elapsed + 2))
          done
          if [[ "$ready" != "true" ]]; then
            echo "Error: $container not ready within ${TIMEOUT}s" >&2
            exit 1
          fi
          ((SERVICE_PORT++))
        done
        echo 'All nodes are ready.'
        """;

    /**
     * wait for cluster formation (multi-node only).
     * Env: CONTAINERS (newline-separated), TIMEOUT, NUM_NODES.
     * Log must be accessible via docker logs command
     */
    private static final String SCRIPT_WAIT_CLUSTER_FORMATION = """
        CONTAINERS_ARR=()
        while IFS= read -r line; do [[ -n "$line" ]] && CONTAINERS_ARR+=("$line"); done <<< "$CONTAINERS"
        TIMEOUT=${TIMEOUT:-60}
        NUM_NODES=${NUM_NODES:-1}
        echo "Waiting for cluster formation (expected size: $NUM_NODES)..."
        first_container=$(printf "%s\\n" "${CONTAINERS_ARR[@]}" | head -n1)
        #first_container="${CONTAINERS_ARR[0]}"
        elapsed=0
        cluster_size=0
        while ((elapsed < TIMEOUT)); do
            cluster_size=$(docker logs "$first_container" 2>&1 \\
                               | grep 'CLUSTER-SIZE' \\
                               | sed -n 's/.*CLUSTER-SIZE[[:space:]]*\\([0-9]*\\).*/\\1/p' \\
                               | tail -1)
            echo ${cluster_size}
            [[ -z "$cluster_size" ]] && cluster_size=0
            if [[ "$cluster_size" == "$NUM_NODES" ]]; then
              echo "Cluster formed: $cluster_size nodes (${elapsed}s)"
              exit 0
            fi
            sleep 2
            elapsed=$((elapsed + 2))
        done
        echo "Error: Cluster did not form within ${TIMEOUT}s (expected $NUM_NODES nodes)" >&2
        exit 1
        """;

    /**
     * wait for cluster stability (migrations complete on each node).
     * Env: CONTAINERS (newline-separated), TIMEOUT.
     * Log must be accessible via docker logs command
     */
    private static final String SCRIPT_WAIT_CLUSTER_STABILITY = """
        CONTAINERS_ARR=()
        while IFS= read -r line; do [[ -n "$line" ]] && CONTAINERS_ARR+=("$line"); done <<< "$CONTAINERS"
        TIMEOUT=${TIMEOUT:-60}
        echo 'Waiting for cluster stability...'
        for container in "${CONTAINERS_ARR[@]}"; do
          echo "Waiting for $container to stabilize..."
          elapsed=0
          stable="false"
          while ((elapsed < TIMEOUT)); do
            if docker logs "$container" 2>&1 | grep -q 'migrations: complete'; then
              echo "  $container is stable (${elapsed}s)"
              stable="true"
              break
            fi
            sleep 2
            elapsed=$((elapsed + 2))
          done
          if [[ "$stable" != "true" ]]; then
            echo "Error: $container did not stabilize within ${TIMEOUT}s" >&2
            exit 1
          fi
        done
        echo 'All nodes are stable.'
        """;

    public ASNodeController(String containerNamePrefix, Integer port) throws Exception {
        this.containerNamePrefix = containerNamePrefix != null && !containerNamePrefix.isEmpty()
                ? containerNamePrefix
                : DEFAULT_CONTAINER_NAME_PREFIX;
        containers = listContainersWithPrefix().toArray(new String[0]);
        this.port = Optional.ofNullable(port).orElse(3000);
    }


    private List<String> listContainersWithPrefix() throws Exception {
        // List only running containers so we never exec into a stopped one
        List<String> cmd = List.of(
                "docker", "ps",
                "--filter", "name=" + this.containerNamePrefix,
                "--format", "{{.Names}}"
        );

        String output = runCommand(cmd, (s) -> s.contains(this.containerNamePrefix));
        List<String> nodes = new ArrayList<>();
        for (String line : output.split("\n")) {
            String name = line.trim();
            if (name.isEmpty()) continue;
            if (name.toLowerCase().startsWith(containerNamePrefix)) {
                nodes.add(name);
            }
        }
        Collections.sort(nodes);
        return nodes;

    }

    private String runCommand(List<String> cmd, Predicate<String> expected) throws Exception {
        long start = System.currentTimeMillis();
        long endTime = start + DELAY_TIMEOUT_MS;
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;

            while ((line = r.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        int exit = p.waitFor();
        if (exit != 0) {
            throw new RuntimeException("Command failed (exit " + exit + "): " + cmd + "\n" + sb);
        }
        String result = sb.toString();
        if (!expected.test(result)) {
            throw new RuntimeException("Command failed - Expected result not captured: " + cmd + "\n" + sb);
        }
        return result;
    }

    /**
     * Run a bash script with the given environment variables. Script is fed via stdin to {@code bash -s}.
     * Output is forwarded to this process stdout/stderr. Throws if script exits non-zero.
     * Script include timeout properties
     */
    private void runScript(String script, String... envKeyValues) throws Exception {
        if (envKeyValues.length % 2 == 0) {
            ProcessBuilder pb = new ProcessBuilder("bash", "-s");
            pb.redirectErrorStream(true);
            Map<String, String> env = pb.environment();
            for (int i = 0; i < envKeyValues.length; i += 2) {
                env.put(envKeyValues[i], envKeyValues[i + 1]);
            }
            Process p = pb.start();
            try (Writer stdin = new OutputStreamWriter(p.getOutputStream(), StandardCharsets.UTF_8)) {
                stdin.write(script);
            }
            StringBuilder out = new StringBuilder();
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    System.out.println(line);
                    out.append(line).append('\n');
                }
            }
            int exit = p.waitFor();
            if (exit != 0) {
                throw new RuntimeException("Script failed with exit " + exit + "\n" + out);
            }
        } else {
            throw new IllegalArgumentException("envKeyValues must be key, value pairs");
        }
    }

    private void waitForClusterFormation(int timeoutSec) throws Exception {
        if (containers.length <= 1) return;
        String containersStr = String.join("\n", containers);
        runScript(SCRIPT_WAIT_CLUSTER_FORMATION,
                "CONTAINERS", containersStr,
                "TIMEOUT", String.valueOf(timeoutSec),
                "NUM_NODES", String.valueOf(containers.length));
    }

    private void waitForServicePort(Integer timeoutSec) throws Exception {
        if (containers.length == 0) return;
        String containersStr = String.join("\n", containers);
        runScript(SCRIPT_WAIT_SERVICE_PORT,
                "CONTAINERS", containersStr,
                "TIMEOUT", String.valueOf(timeoutSec),
                "SERVICE_PORT", String.valueOf(port));
    }

    private void waitForClusterStability(int timeoutSec) throws Exception {
        if (containers.length == 0) return;
        String containersStr = String.join("\n", containers);
        runScript(SCRIPT_WAIT_CLUSTER_STABILITY,
                "CONTAINERS", containersStr,
                "TIMEOUT", String.valueOf(timeoutSec));
    }

    public void startNode(int nodeLabel, Integer timeoutSecs) throws Exception {
        if (nodeLabel < 1 || nodeLabel > containers.length) {
            throw new IllegalArgumentException("nodeLabel must be between 1 and " + containers.length + " (1-based)");
        }
        startNode(containers[nodeLabel - 1], timeoutSecs);
    }

    private void startNode(String chosenContainer, Integer timeoutSecs) throws Exception {
        int effectiveTimeOut = Optional.ofNullable(timeoutSecs).orElse(DEFAULT_SCRIPT_TIMEOUT_SEC);
        List<String> cmd = List.of(
                "docker", "exec", chosenContainer,
                "bash", "-c", ASD_START
        );

        runCommand(cmd, (s) -> s.contains("Starting"));
        waitForServicePort(effectiveTimeOut);
        waitForClusterFormation(effectiveTimeOut);
        waitForClusterStability(effectiveTimeOut);
    }

    // Stops the Asd process within a container (nodeLabel is 1-based)
    public void stopNode(int nodeLabel) throws Exception {
        if (nodeLabel < 1 || nodeLabel > containers.length) {
            throw new IllegalArgumentException("nodeLabel must be between 1 and " + containers.length + " (1-based)");
        }
        stopNode(containers[nodeLabel - 1]);
    }

    private void stopNode(String chosenContainer) throws Exception {
        List<String> cmd = List.of(
                "docker", "exec", chosenContainer,
                "sh", "-c", ASD_STOP
        );
       runCommand(cmd, (s) -> s.toLowerCase().contains("stopping"));
    }

    public boolean isNodeStopped(String chosenContainer) {
        try {
            List<String> cmd = List.of(
                    "docker", "exec", chosenContainer,
                    "sh", "-c", ASD_STATUS
            );
            runCommand(cmd, (s) -> s.toLowerCase().contains("stopped"));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void startAllNodes() throws Exception {
        for (String container : containers) {
            if (isNodeStopped(container)) {
                startNode(container, null);
            }
        }
    }

}


