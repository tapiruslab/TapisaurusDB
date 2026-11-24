use tapisaurus_core::cluster::{DeterministicOp, DeterministicTxSpec};
use tapisaurus_sim::{run_chaos_suite, FaultProfile, SimCluster, SimClusterConfig};

#[tokio::test]
async fn test_dst_seed_reproducibility_bit_for_bit() {
    let report_a = run_chaos_suite(0x2026_1124_CAFE_0001, 2_000)
        .await
        .expect("chaos run A must succeed cleanly");
    let report_b = run_chaos_suite(0x2026_1124_CAFE_0001, 2_000)
        .await
        .expect("chaos run B must succeed cleanly");

    assert_eq!(report_a.ops_submitted, 2_000);
    assert_eq!(report_a.total_supply, 20_000);
    assert_eq!(report_a.final_state_crc32, report_b.final_state_crc32);
    assert_eq!(report_a.transfers_committed, report_b.transfers_committed);
    assert_eq!(report_a.transfers_aborted, report_b.transfers_aborted);
    assert_eq!(report_a.network_stats, report_b.network_stats);
}

#[tokio::test]
async fn test_dst_10k_chaos_ops_zero_double_spend_and_convergence() {
    let report = run_chaos_suite(0x5441_5049_5255_5303, 10_000)
        .await
        .expect("10,000-op chaos storm must preserve linearizability and zero double-spend");

    assert_eq!(report.ops_submitted, 10_000);
    assert_eq!(report.total_supply, 20_000);
    assert!(report.transfers_committed > 0);
    assert!(report.transfers_aborted > 0);
    assert!(report.torn_wal_injections > 0);
    assert!(report.crash_restarts > 0);
}

#[tokio::test]
async fn test_dst_split_brain_minority_rejected_and_majority_progresses() {
    let mut cluster = SimCluster::new(SimClusterConfig {
        seed: 0xDEAD_BEEF_55AA_1001,
        node_count: 5,
        faults: FaultProfile::default(),
    })
    .unwrap();

    // Seed accounts under healthy quorum
    let init_tx = DeterministicTxSpec::new(
        "tx_init",
        vec![
            DeterministicOp::Put {
                key: "acct:alice".into(),
                value: 1_000,
            },
            DeterministicOp::Put {
                key: "acct:bob".into(),
                value: 1_000,
            },
        ],
    );
    cluster.commit_calvin_epoch(0, vec![init_tx]).await.unwrap();

    // Partition old leader (node 0) + node 1 into a minority side (2 vs 3)
    cluster.partition_minority(&[0, 1]);

    // Stale minority leader MUST NOT commit a double-spend epoch
    let rogue_tx = DeterministicTxSpec::new(
        "tx_rogue_minority",
        vec![DeterministicOp::Transfer {
            from_key: "acct:alice".into(),
            to_key: "acct:bob".into(),
            amount: 500,
            min_balance: 0,
        }],
    );
    let minority_res = cluster.commit_calvin_epoch(0, vec![rogue_tx]).await;
    assert!(
        minority_res.is_err(),
        "Minority stale leader must be blocked from reaching quorum commit"
    );

    // Majority side (nodes 2, 3, 4) elects node 2 cleanly and executes valid transfers
    let won = cluster.run_election(2).await;
    assert!(won, "Majority quorum node 2 must win election cleanly");

    let valid_tx = DeterministicTxSpec::new(
        "tx_majority_transfer",
        vec![DeterministicOp::Transfer {
            from_key: "acct:alice".into(),
            to_key: "acct:bob".into(),
            amount: 750,
            min_balance: 0,
        }],
    );
    let (_epoch, receipts) = cluster
        .commit_calvin_epoch(2, vec![valid_tx])
        .await
        .unwrap();
    assert!(receipts[0].committed);

    // Heal partition and ensure lagging minority nodes converge cleanly without double-spend
    cluster.heal_partitions();
    cluster.catch_up_cluster().await.unwrap();
    cluster.assert_single_leader_per_term().unwrap();
    let (_crc, supply) = cluster
        .assert_zero_double_spend_and_convergence(2_000, 0)
        .unwrap();
    assert_eq!(supply, 2_000);
}

#[tokio::test]
async fn test_dst_torn_wal_power_cut_and_delayed_out_of_order_packets() {
    let mut cluster = SimCluster::new(SimClusterConfig {
        seed: 0x0FAC_ED99_1234_5678,
        node_count: 3,
        faults: FaultProfile::default(),
    })
    .unwrap();

    cluster
        .commit_calvin_epoch(
            0,
            vec![DeterministicTxSpec::new(
                "tx_seed",
                vec![
                    DeterministicOp::Put {
                        key: "acct:x".into(),
                        value: 5_000,
                    },
                    DeterministicOp::Put {
                        key: "acct:y".into(),
                        value: 5_000,
                    },
                ],
            )],
        )
        .await
        .unwrap();

    // Enqueue delayed/out-of-order AppendEntries heartbeat packets
    let hb = cluster.node_info(0);
    assert!(hb.is_leader);
    cluster.transport().set_fault_profile(FaultProfile {
        packet_drop_rate: 0.0,
        packet_delay_rate: 1.0,
        min_delay_ms: 10,
        max_delay_ms: 40,
        duplicate_rate: 0.5,
    });

    // Inject torn WAL tail write on follower node 1 and recover
    cluster.inject_torn_wal_tail(1).unwrap();
    cluster.restart_node(1);

    // Snapshot leader and commit one more epoch after compaction
    cluster.snapshot_leader().unwrap();
    cluster
        .commit_calvin_epoch(
            0,
            vec![DeterministicTxSpec::new(
                "tx_post_snapshot",
                vec![DeterministicOp::Transfer {
                    from_key: "acct:x".into(),
                    to_key: "acct:y".into(),
                    amount: 1_250,
                    min_balance: 0,
                }],
            )],
        )
        .await
        .unwrap();

    cluster.transport().advance_clock(100);
    let _ = cluster.transport().drain_due_delayed();
    cluster.catch_up_cluster().await.unwrap();

    cluster.assert_single_leader_per_term().unwrap();
    let (_crc, total) = cluster
        .assert_zero_double_spend_and_convergence(10_000, 0)
        .unwrap();
    assert_eq!(total, 10_000);
}
