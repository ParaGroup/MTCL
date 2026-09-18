#Bash needed to test the 4 server implementations with different number of clients.
#(synchronous, synchronousMT, Asynchronous, Asynchronous with I/O thread)


#!/bin/bash
#SBATCH --job-name=mtcl_scale
#SBATCH --output=mtcl_scale_%j.log
#SBATCH --nodes=8                    # 8 nodes for the clients
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:30:00

echo "=== STARTING JOB ==="
date

NODELIST=$(scontrol show hostnames $SLURM_JOB_NODELIST)

# Server is hosted on the frontend node
MASTER_NODE="spmln"

CLIENT_NODES="$NODELIST"
NUM_CLIENT_NODES=$(echo "$CLIENT_NODES" | wc -l)

echo "-> Server (Frontend): $MASTER_NODE"
echo "-> Client ($NUM_CLIENT_NODES available):"
echo "$CLIENT_NODES" | tr '\n' ' '
echo ""

# compile
make test_multiclient_sync TPROTOCOL=TCP

echo "-> (Server must be alrady istantiated on the frontend node)"

sleep 2

for TOTAL_CLIENTS in 1 2 4 8 16 32 64 128 256 512; do
    echo "=========================================================="
    echo ">>> LAUNCHING $TOTAL_CLIENTS CLIENTS <<<"

    # distribute clients on the nodes
    EXTRA_CLIENTS=$((TOTAL_CLIENTS % NUM_CLIENT_NODES))
    BASE_CLIENTS=$((TOTAL_CLIENTS / NUM_CLIENT_NODES))

    echo "-> $EXTRA_CLIENTS nodes with $((BASE_CLIENTS + 1)) clients, $((NUM_CLIENT_NODES - EXTRA_CLIENTS)) nodes with $BASE_CLIENTS clients."

    NODES_WITH_EXTRA=$(echo "$CLIENT_NODES" | head -n $EXTRA_CLIENTS | tr '\n' ',' | sed 's/,$//')
    NODES_WITH_BASE=$(echo "$CLIENT_NODES" | tail -n +$((EXTRA_CLIENTS + 1)) | tr '\n' ',' | sed 's/,$//')

    CLIENT_PID_1=""
    CLIENT_PID_2=""

    if [ $EXTRA_CLIENTS -gt 0 ]; then
        srun --nodes=$EXTRA_CLIENTS --ntasks=$EXTRA_CLIENTS --nodelist=$NODES_WITH_EXTRA \
             bash -c "./test_multiclient_fullAsync 1 $MASTER_NODE $((BASE_CLIENTS + 1))" &
        CLIENT_PID_1=$!
    fi

    if [ $((NUM_CLIENT_NODES - EXTRA_CLIENTS)) -gt 0 ]; then
        srun --nodes=$((NUM_CLIENT_NODES - EXTRA_CLIENTS)) --ntasks=$((NUM_CLIENT_NODES - EXTRA_CLIENTS)) --nodelist=$NODES_WITH_BASE \
             bash -c "./test_multiclient_fullAsync 1 $MASTER_NODE $BASE_CLIENTS" &
        CLIENT_PID_2=$!
    fi

    # Waiting for clients
    wait $CLIENT_PID_1 $CLIENT_PID_2
done

echo "=========================================================="
echo "-> Benchmark terminated."
echo "=== JOB COMPLETED ==="
date
