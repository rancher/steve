#!/bin/bash

set -e

STEVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RANCHER_IMAGE="${RANCHER_IMAGE:-}"
RANCHER_AGENT_IMAGE="${RANCHER_AGENT_IMAGE:-}"
CONTAINER_NAME="rancher-server-test"
BOOTSTRAP_PASSWORD="${CATTLE_BOOTSTRAP_PASSWORD:-admin}"
SKIP_SETUP=false

detect_rancher_images() {
    local rancher_image=$1
    local agent_image=$2
    
    echo "Image detection - Input: rancher_image=${rancher_image}, agent_image=${agent_image}"
    
    if [ -z "$rancher_image" ] || [ -z "$agent_image" ]; then
        echo "Detecting most recently built Rancher images..."
        echo "All rancher images:"
        docker images | grep rancher || echo "No rancher images found"
        
        if [ -z "$rancher_image" ]; then
            local detected_server=$(docker images --format "{{.Repository}}:{{.Tag}}" --filter "reference=rancher/rancher" 2>/dev/null | head -n1)
            if [ -z "$detected_server" ]; then
                echo "Trying alternative detection method for rancher server..."
                detected_server=$(docker images rancher/rancher --format "{{.Repository}}:{{.Tag}}" 2>/dev/null | head -n1)
            fi
            if [ -n "$detected_server" ]; then
                rancher_image="$detected_server"
                echo "  Detected rancher server image: ${rancher_image}"
            else
                echo "  Warning: Could not detect rancher server image"
            fi
        fi
        
        if [ -z "$agent_image" ]; then
            local detected_agent=$(docker images --format "{{.Repository}}:{{.Tag}}" --filter "reference=rancher/rancher-agent" 2>/dev/null | head -n1)
            if [ -z "$detected_agent" ]; then
                echo "Trying alternative detection method for rancher agent..."
                detected_agent=$(docker images rancher/rancher-agent --format "{{.Repository}}:{{.Tag}}" 2>/dev/null | head -n1)
            fi
            if [ -n "$detected_agent" ]; then
                agent_image="$detected_agent"
                echo "  Detected rancher agent image: ${agent_image}"
            else
                echo "  Warning: Could not detect rancher agent image"
            fi
        fi
    fi
    
    RANCHER_IMAGE="${rancher_image}"
    RANCHER_AGENT_IMAGE="${agent_image}"
    
    echo "Image detection - Final: RANCHER_IMAGE=${RANCHER_IMAGE}, RANCHER_AGENT_IMAGE=${RANCHER_AGENT_IMAGE}"
}

cleanup() {
    echo ""
    echo "Cleaning up..."
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Stopping and removing container ${CONTAINER_NAME}..."
        docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
        docker rm "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    fi
}

trap cleanup EXIT

detect_rancher_ip() {
    local ip=$(hostname -I | awk '{print $1}')
    if [ -z "$ip" ]; then
        echo "Error: Could not determine IP address" >&2
        return 1
    fi
    RANCHER_IP="$ip"
    echo "Using IP address: $RANCHER_IP"
}

remove_existing_container() {
    local container_name=$1
    if docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "Removing existing container ${container_name}..."
        docker stop "${container_name}" >/dev/null 2>&1 || true
        docker rm "${container_name}" >/dev/null 2>&1 || true
    fi
}

start_rancher_server() {
    local container_name=$1
    local rancher_image=$2
    local agent_image=$3
    local rancher_ip=$4
    local bootstrap_password=$5
    
    echo "Starting Rancher server container..."
    echo "  Image: ${rancher_image}"
    echo "  Agent Image: ${agent_image}"
    echo "  Server URL: https://${rancher_ip}"
    echo ""
    
    docker create --name "${container_name}" --restart=unless-stopped \
        -p 80:80 -p 443:443 \
        --privileged \
        -e CATTLE_SERVER_URL="https://${rancher_ip}" \
        -e CATTLE_BOOTSTRAP_PASSWORD="${bootstrap_password}" \
        -e CATTLE_DEV_MODE="yes" \
        -e CATTLE_AGENT_IMAGE="${agent_image}" \
        "${rancher_image}"
    
    docker start "${container_name}"
    
    docker logs -f "${container_name}" 2>&1 &
    LOGS_PID=$!
    
    cleanup_logs() {
        kill $LOGS_PID 2>/dev/null || true
    }
    trap cleanup_logs EXIT
}

wait_for_rancher() {
    local rancher_ip=$1
    local max_wait=${2:-300}  # Default 5 minutes
    local interval=5
    
    local elapsed=0
    local state="waiting_for_response"
    
    while [ $elapsed -lt $max_wait ]; do
        local response=$(curl -k -s -L \
            -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" \
            -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
            "https://${rancher_ip}" 2>/dev/null || echo "")
        local http_code=$(curl -k -s -o /dev/null -w "%{http_code}" -L \
            -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" \
            -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36" \
            "https://${rancher_ip}" 2>/dev/null || echo "000")
        
        case "$state" in
            "waiting_for_response")
                if [ "$http_code" != "000" ] && [ -n "$response" ]; then
                    state="checking_status"
                fi
                ;;
            "checking_status")
                if [ "$http_code" = "503" ] || echo "$response" | grep -qi "API Aggregation not ready"; then
                    # Still waiting
                    :
                elif [ "$http_code" = "200" ]; then
                    echo ""
                    echo "Rancher is ready!"
                    return 0
                fi
                ;;
        esac

        sleep $interval
        elapsed=$((elapsed + interval))
    done
    
    echo "Error: Rancher did not become ready within ${max_wait} seconds"
    echo "Check logs with: docker logs ${CONTAINER_NAME}"
    return 1
}

export_env_vars() {
    local steve_dir=$1
    local bootstrap_password=$2
    local agent_image=$3
    
    export CATTLE_BOOTSTRAP_PASSWORD="${bootstrap_password}"
    export CATTLE_AGENT_IMAGE="${agent_image}"
    export CATTLE_TEST_CONFIG="${steve_dir}/tests/integration/steveapi/steveapi.yaml"
}

run_setup() {
    local steve_dir=$1
    local skip_setup=$2
    
    if [ "$skip_setup" = true ]; then
        echo "Skipping integration test setup..."
        return 0
    fi
    
    echo ""
    echo "Running integration test setup..."
    cd "${steve_dir}/tests/integration/setup"
    
    if [ ! -f "./setup" ] || [ "./setup" -ot "./main.go" ]; then
        echo "Building setup binary..."
        go build -o setup .
    fi
    
    ./setup
}

run_integration_tests() {
    local steve_dir=$1
    
    echo ""
    echo "Running integration tests..."
    cd "${steve_dir}"
    
    # Run the tests
    go test -v ./tests/integration/...
    
    echo ""
    echo "Integration tests completed!"
}

main() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --rancher-image)
                RANCHER_IMAGE="$2"
                shift 2
                ;;
            --agent-image)
                RANCHER_AGENT_IMAGE="$2"
                shift 2
                ;;
            --container-name)
                CONTAINER_NAME="$2"
                shift 2
                ;;
            --skip-setup)
                SKIP_SETUP=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --rancher-image IMAGE    Rancher server image (default: auto-detect or rancher/rancher:dev)"
                echo "  --agent-image IMAGE      Rancher agent image (default: auto-detect or rancher/rancher-agent:dev)"
                echo "  --container-name NAME    Docker container name (default: rancher-server-test)"
                echo "  --skip-setup             Skip running the setup program"
                echo "  -h, --help               Show this help message"
                echo ""
                echo "Environment variables:"
                echo "  RANCHER_IMAGE            Same as --rancher-image"
                echo "  RANCHER_AGENT_IMAGE      Same as --agent-image"
                echo "  CATTLE_BOOTSTRAP_PASSWORD Bootstrap password (default: admin)"
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    detect_rancher_images "${RANCHER_IMAGE}" "${RANCHER_AGENT_IMAGE}"
    
    detect_rancher_ip || exit 1
    
    remove_existing_container "${CONTAINER_NAME}"
    
    start_rancher_server "${CONTAINER_NAME}" "${RANCHER_IMAGE}" "${RANCHER_AGENT_IMAGE}" "${RANCHER_IP}" "${BOOTSTRAP_PASSWORD}"
    
    wait_for_rancher "${RANCHER_IP}" 300 || exit 1
    
    export_env_vars "${STEVE_DIR}" "${BOOTSTRAP_PASSWORD}" "${RANCHER_AGENT_IMAGE}"
    
    run_setup "${STEVE_DIR}" "${SKIP_SETUP}"
    
    run_integration_tests "${STEVE_DIR}"
}

main "$@"
