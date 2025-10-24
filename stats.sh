#!/bin/bash

# Check for the namespace argument
if [ -z "$1" ]; then
  echo "Usage: $0 <namespace>"
  exit 1
fi

NAMESPACE=$1
echo "CPU and Memory Usage for Pods in Namespace: $NAMESPACE (without Metrics Server)"
echo "--------------------------------------------------------"
echo "Note: These are raw cgroup values and not easy to interpret."


# Loop through each pod in the specified namespace
kubectl get pods -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}' | xargs -n 1 -I{} bash -c '
  POD_NAME="{}"
  # Check if the pod has a shell to execute into
  kubectl exec "$POD_NAME" -n "'"$NAMESPACE"'" -- which sh >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "Warning: Skipping pod $POD_NAME (no shell to execute into)"
    exit 0
  fi

  echo "Pod: $POD_NAME"

  # Get raw memory usage from cgroup
  MEMORY_USAGE=$(kubectl exec "$POD_NAME" -n "'"$NAMESPACE"'" -- cat /sys/fs/cgroup/memory/memory.usage_in_bytes)
  echo "  Memory Usage (bytes): $MEMORY_USAGE"

  # Get raw cumulative CPU usage from cgroup
  CPU_USAGE=$(kubectl exec "$POD_NAME" -n "'"$NAMESPACE"'" -- cat /sys/fs/cgroup/cpu/cpuacct.usage)
  echo "  CPU Usage (nanoseconds, cumulative): $CPU_USAGE"
  echo ""
'
