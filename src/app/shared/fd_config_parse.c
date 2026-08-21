#include "../platform/fd_config_extract.h"
#include "../platform/fd_config_macros.c"
#include "fd_config_private.h"

static void
fd_config_check_configf( fd_config_t *  config,
                         fd_configf_t * config_f ) {
  (void)config_f;
  if( FD_UNLIKELY( strlen( config->paths.snapshots )>PATH_MAX-1UL ) ) {
    FD_LOG_ERR(( "[config->paths.snapshots] is too long (max %lu)", PATH_MAX-1UL ));
  }
  if( FD_UNLIKELY( config->paths.snapshots[ 0 ]!='\0' && config->paths.snapshots[ 0 ]!='/' ) ) {
    FD_LOG_ERR(( "[config->paths.snapshots] must be an absolute path and hence start with a '/'"));
  }
}

fd_configf_t *
fd_config_extract_podf( uchar *        pod,
                        fd_configf_t * config ) {
  CFG_POP_ARRAY( cstr,   paths.authorized_voter_paths                        );

  CFG_POP      ( cstr,   gossip.host                                         );

  CFG_POP      ( bool,   layout.enable_block_production                      );
  CFG_POP      ( uint,   layout.execrp_tile_count                            );
  CFG_POP      ( uint,   layout.sign_tile_count                              );
  CFG_POP      ( uint,   layout.resolv_tile_count                            );
  CFG_POP      ( uint,   layout.execle_tile_count                            );
  CFG_POP      ( uint,   layout.gossvf_tile_count                            );

  CFG_POP      ( ulong,  accounts.max_accounts                               );
  CFG_POP      ( ulong,  accounts.cache_size_gib                             );

  CFG_POP      ( ulong,  runtime.max_live_slots                              );
  CFG_POP      ( ulong,  runtime.max_fork_width                              );

  CFG_POP      ( ulong,  runtime.program_cache.heap_size_mib                 );
  CFG_POP      ( ulong,  runtime.program_cache.mean_cache_entry_size         );

  CFG_POP      ( cstr,   consensus.wait_for_supermajority_with_bank_hash     );

  CFG_POP      ( uint,   snapshots.sources.max_local_full_effective_age      );
  CFG_POP      ( uint,   snapshots.sources.max_local_incremental_age         );
  CFG_POP      ( bool,   snapshots.sources.gossip.allow_any                  );
  CFG_POP_ARRAY( cstr,   snapshots.sources.gossip.allow_list                 );
  CFG_POP_ARRAY( cstr,   snapshots.sources.gossip.block_list                 );
  CFG_POP_ARRAY( cstr,   snapshots.sources.servers                           );
  CFG_POP      ( bool,   snapshots.incremental_snapshots                     );
  CFG_POP      ( bool,   snapshots.genesis_download                          );
  CFG_POP      ( uint,   snapshots.max_full_snapshots_to_keep                );
  CFG_POP      ( uint,   snapshots.max_incremental_snapshots_to_keep         );
  CFG_POP      ( uint,   snapshots.max_retry_abort                           );
  CFG_POP      ( uint,   snapshots.min_download_speed_mibs                   );
  CFG_POP      ( ulong,  snapshots.wait_for_peers_timeout_seconds            );

  CFG_POP      ( bool,   development.hard_fork_fatal                         );
  CFG_POP      ( bool,   development.fixed_fec_sets                          );

  CFG_POP      ( bool,   development.genesis.validate_genesis_hash           );

  CFG_POP      ( cstr,   development.ledger_input.format                     );
  CFG_POP      ( cstr,   development.ledger_input.path                       );
  CFG_POP      ( ulong,  development.ledger_input.end_slot                   );

  CFG_POP      ( cstr,   development.backtest.affinity                       );
  CFG_POP      ( ulong,  development.backtest.root_distance                  );

  CFG_POP      ( cstr,   development.forktest.affinity                       );

  return config;
}

fd_config_t *
fd_config_extract_pod( uchar *       pod,
                       fd_config_t * config ) {
  CFG_POP      ( cstr,   name                                             );
  CFG_POP      ( cstr,   user                                             );

  CFG_POP      ( bool,   telemetry                                        );

  CFG_POP      ( cstr,   log.path                                         );
  CFG_POP      ( cstr,   log.colorize                                     );
  CFG_POP      ( cstr,   log.level_logfile                                );
  CFG_POP      ( cstr,   log.level_stderr                                 );
  CFG_POP      ( cstr,   log.level_flush                                  );

  CFG_POP      ( cstr,   paths.base                                       );
  CFG_POP      ( cstr,   paths.identity_key                               );
  CFG_POP      ( cstr,   paths.vote_account                               );
  CFG_POP      ( cstr,   paths.snapshots                                  );
  CFG_POP      ( cstr,   paths.genesis                                    );
  CFG_POP      ( cstr,   paths.accounts                                   );
  CFG_POP      ( cstr,   paths.shredb                                     );

  CFG_POP_ARRAY( cstr,   gossip.entrypoints                               );
  CFG_POP      ( ushort, gossip.port                                      );

  CFG_POP      ( ushort, consensus.expected_shred_version                 );
  CFG_POP      ( cstr,   consensus.expected_genesis_hash                  );
  CFG_POP      ( bool,   consensus.wait_for_vote_to_start_leader          );

  CFG_POP      ( cstr,   layout.affinity                                  );
  CFG_POP      ( cstr,   layout.blocklist_cores                           );
  CFG_POP      ( uint,   layout.net_tile_count                            );
  CFG_POP      ( uint,   layout.quic_tile_count                           );
  CFG_POP      ( uint,   layout.verify_tile_count                         );
  CFG_POP      ( uint,   layout.shred_tile_count                          );

  CFG_POP      ( cstr,   hugetlbfs.mount_path                             );
  CFG_POP      ( cstr,   hugetlbfs.max_page_size                          );
  CFG_POP      ( ulong,  hugetlbfs.gigantic_page_threshold_mib            );

  CFG_POP      ( cstr,   net.interface                                    );
  CFG_POP      ( cstr,   net.bind_address                                 );
  CFG_POP      ( cstr,   net.provider                                     );
  CFG_POP      ( uint,   net.ingress_buffer_size                          );
  CFG_POP      ( cstr,   net.xdp.xdp_mode                                 );
  CFG_POP      ( bool,   net.xdp.xdp_zero_copy                            );
  CFG_POP      ( cstr,   net.xdp.poll_mode                                );
  CFG_POP      ( uint,   net.xdp.xdp_rx_queue_size                        );
  CFG_POP      ( uint,   net.xdp.xdp_tx_queue_size                        );
  CFG_POP      ( uint,   net.xdp.flush_timeout_micros                     );
  CFG_POP      ( cstr,   net.xdp.rss_queue_mode                           );
  CFG_POP      ( bool,   net.xdp.listen_gre                               );
  CFG_POP      ( bool,   net.xdp.native_bond                              );
  CFG_POP      ( uint,   net.socket.receive_buffer_size                   );
  CFG_POP      ( uint,   net.socket.send_buffer_size                      );

  CFG_POP      ( ulong,  tiles.netlink.max_routes                         );
  CFG_POP      ( ulong,  tiles.netlink.max_peer_routes                    );
  CFG_POP      ( ulong,  tiles.netlink.max_neighbors                      );

  CFG_POP      ( ulong,  tiles.gossip.max_entries                         );

  CFG_POP      ( ushort, tiles.quic.regular_transaction_listen_port       );
  CFG_POP      ( ushort, tiles.quic.quic_transaction_listen_port          );
  CFG_POP      ( uint,   tiles.quic.txn_reassembly_count                  );
  CFG_POP      ( uint,   tiles.quic.max_concurrent_connections            );
  CFG_POP      ( uint,   tiles.quic.max_concurrent_handshakes             );
  CFG_POP      ( uint,   tiles.quic.idle_timeout_millis                   );
  CFG_POP      ( uint,   tiles.quic.ack_delay_millis                      );
  CFG_POP      ( bool,   tiles.quic.retry                                 );
  CFG_POP      ( cstr,   tiles.quic.ssl_key_log_file                      );

  CFG_POP      ( uint,   tiles.verify.signature_cache_size                );
  CFG_POP      ( uint,   tiles.verify.receive_buffer_size                 );
  CFG_POP      ( uint,   tiles.verify.mtu                                 );

  CFG_POP      ( uint,   tiles.dedup.signature_cache_size                 );

  CFG_POP      ( bool,   tiles.bundle.enabled                             );
  CFG_POP      ( cstr,   tiles.bundle.url                                 );
  CFG_POP      ( cstr,   tiles.bundle.tls_domain_name                     );
  CFG_POP      ( cstr,   tiles.bundle.tpu_url                             );
  CFG_POP      ( cstr,   tiles.bundle.tpu_tls_domain_name                 );
  CFG_POP      ( cstr,   tiles.bundle.tip_distribution_program_addr       );
  CFG_POP      ( cstr,   tiles.bundle.tip_payment_program_addr            );
  CFG_POP      ( cstr,   tiles.bundle.tip_distribution_authority          );
  CFG_POP      ( uint,   tiles.bundle.commission_bps                      );
  CFG_POP      ( ulong,  tiles.bundle.keepalive_interval_millis           );
  CFG_POP      ( bool,   tiles.bundle.tls_cert_verify                     );
  CFG_POP      ( bool,   tiles.bundle.inertia_block_mode                 );
  CFG_POP      ( cstr,   tiles.bundle.strategy                            );

  CFG_POP      ( uint,   tiles.pack.max_pending_transactions              );
  CFG_POP      ( bool,   tiles.pack.use_consumed_cus                      );
  CFG_POP      ( cstr,   tiles.pack.schedule_strategy                     );
  CFG_POP_ARRAY( cstr,   tiles.pack.account_blocklist                     );

  CFG_POP      ( ulong,  tiles.replay.max_transaction_lookahead_buffer_size );
  CFG_POP_ARRAY( cstr,   tiles.replay.enable_features                       );

  CFG_POP      ( bool,   tiles.pohh.lagged_consecutive_leader_start       );

  CFG_POP      ( uint,   tiles.shred.max_pending_shred_sets                   );
  CFG_POP      ( ushort, tiles.shred.shred_listen_port                        );
  CFG_POP_ARRAY( cstr,   tiles.shred.additional_shred_destinations_retransmit );
  CFG_POP_ARRAY( cstr,   tiles.shred.additional_shred_destinations_leader     );

  CFG_POP      ( cstr,   tiles.metric.prometheus_listen_address           );
  CFG_POP      ( ushort, tiles.metric.prometheus_listen_port              );

  CFG_POP      ( cstr,   tiles.event.url                                  );

  CFG_POP      ( bool,   tiles.gui.enabled                                );
  CFG_POP      ( cstr,   tiles.gui.gui_listen_address                     );
  CFG_POP      ( ushort, tiles.gui.gui_listen_port                        );
  CFG_POP      ( ulong,  tiles.gui.max_http_connections                   );
  CFG_POP      ( ulong,  tiles.gui.max_websocket_connections              );
  CFG_POP      ( ulong,  tiles.gui.max_http_request_length                );
  CFG_POP      ( ulong,  tiles.gui.send_buffer_size_mb                    );

  CFG_POP      ( bool,   tiles.rpc.enabled                                );
  CFG_POP      ( cstr,   tiles.rpc.rpc_listen_address                     );
  CFG_POP      ( ushort, tiles.rpc.rpc_listen_port                        );
  CFG_POP      ( ulong,  tiles.rpc.max_http_connections                   );
  CFG_POP      ( ulong,  tiles.rpc.max_websocket_connections              );
  CFG_POP      ( ulong,  tiles.rpc.max_http_request_length                );
  CFG_POP      ( ulong,  tiles.rpc.send_buffer_size_mb                    );
  CFG_POP      ( bool,   tiles.rpc.delay_startup                          );

  CFG_POP      ( bool,   tiles.grpc.enabled                               );
  CFG_POP      ( cstr,   tiles.grpc.listen_address                        );
  CFG_POP      ( ushort, tiles.grpc.listen_port                           );
  CFG_POP      ( ulong,  tiles.grpc.max_subscriptions                     );
  CFG_POP      ( ulong,  tiles.grpc.tx_depth                              );
  CFG_POP      ( ulong,  tiles.grpc.send_buffer_size_mb                   );

  CFG_POP      ( ushort, tiles.repair.repair_client_listen_port           );
  CFG_POP      ( ulong,  tiles.repair.slot_max                            );

  CFG_POP      ( bool,   tiles.rserve.enabled                             );
  CFG_POP      ( ushort, tiles.rserve.repair_serve_listen_port            );
  CFG_POP      ( ulong,  tiles.rserve.shred_storage_limit_gib             );

  CFG_POP      ( ulong,  capture.capture_start_slot                       );
  CFG_POP      ( cstr,   capture.solcap_capture                           );
  CFG_POP      ( bool,   capture.recent_only                              );
  CFG_POP      ( ulong,  capture.recent_slots_per_file                    );
  CFG_POP      ( cstr,   capture.dump_proto_dir                           );
  CFG_POP      ( cstr,   capture.dump_syscall_name_filter                 );
  CFG_POP      ( cstr,   capture.dump_instr_program_id_filter             );
  CFG_POP      ( bool,   capture.dump_syscall_to_pb                       );
  CFG_POP      ( bool,   capture.dump_instr_to_pb                         );
  CFG_POP      ( bool,   capture.dump_txn_to_pb                           );
  CFG_POP      ( bool,   capture.dump_txn_as_fixture                      );
  CFG_POP      ( bool,   capture.dump_block_to_pb                         );

  CFG_POP      ( ushort, tiles.txsend.txsend_src_port                     );

  CFG_POP      ( bool,   development.sandbox                              );
  CFG_POP      ( bool,   development.no_clone                             );
  CFG_POP      ( cstr,   development.core_dump                            );
  CFG_POP      ( bool,   development.no_agave                             );
  CFG_POP      ( bool,   development.bootstrap                            );

  CFG_POP      ( bool,   development.gossip.allow_private_address         );

  CFG_POP      ( ulong,  development.genesis.hashes_per_tick              );
  CFG_POP      ( ulong,  development.genesis.target_tick_duration_micros  );
  CFG_POP      ( ulong,  development.genesis.ticks_per_slot               );
  CFG_POP      ( ulong,  development.genesis.fund_initial_accounts        );
  CFG_POP      ( ulong,  development.genesis.fund_initial_amount_lamports );
  CFG_POP      ( ulong,  development.genesis.vote_account_stake_lamports  );
  CFG_POP      ( bool,   development.genesis.warmup_epochs                );

  CFG_POP      ( uint,   development.bench.benchg_tile_count              );
  CFG_POP      ( uint,   development.bench.benchs_tile_count              );
  CFG_POP      ( cstr,   development.bench.affinity                       );
  CFG_POP      ( bool,   development.bench.larger_max_cost_per_block      );
  CFG_POP      ( bool,   development.bench.larger_shred_limits_per_block  );
  CFG_POP      ( ulong,  development.bench.disable_blockstore_from_slot   );
  CFG_POP      ( bool,   development.bench.disable_status_cache           );

  CFG_POP      ( cstr,   development.bundle.ssl_key_log_file              );
  CFG_POP      ( uint,   development.bundle.buffer_size_kib               );
  CFG_POP      ( uint,   development.bundle.ssl_heap_size_mib             );

  CFG_POP      ( bool,   development.event.report_shreds                  );
  CFG_POP      ( bool,   development.event.report_transactions            );
  CFG_POP      ( bool,   development.event.report_transaction_diffs       );

  CFG_POP      ( cstr,   development.pktgen.affinity                      );
  CFG_POP      ( cstr,   development.pktgen.fake_dst_ip                   );

  CFG_POP      ( cstr,   development.udpecho.affinity                     );

  CFG_POP      ( ulong,  development.accdb.partition_size_gib             );

  CFG_POP      ( bool,   development.hugetlbfs.min_size                   );

  if( FD_UNLIKELY( !fd_config_extract_podf( pod, &config->firedancer ) ) ) return NULL;
  fd_config_check_configf( config, &config->firedancer );

  /* Renamed config options */

# define CFG_RENAMED( old_path, new_path )                             \
  do {                                                                 \
    char const * key = #old_path;                                      \
    fd_pod_info_t info[1];                                             \
    if( FD_UNLIKELY( !fd_pod_query( pod, key, info ) ) ) {             \
      FD_LOG_WARNING(( "Config option `%s` was renamed to `%s`. "      \
                       "Please update your config file.",              \
                       #old_path, #new_path ));                        \
      return NULL;                                                     \
    }                                                                  \
    (void)config->new_path; /* assert new path exists */               \
  } while(0)

  CFG_RENAMED( tiles.net.interface,            net.interface                );
  CFG_RENAMED( tiles.net.bind_address,         net.bind_address             );
  CFG_RENAMED( tiles.net.provider,             net.provider                 );
  CFG_RENAMED( tiles.net.xdp_mode,             net.xdp.xdp_mode             );
  CFG_RENAMED( tiles.net.xdp_zero_copy,        net.xdp.xdp_zero_copy        );
  CFG_RENAMED( tiles.net.xdp_rx_queue_size,    net.xdp.xdp_rx_queue_size    );
  CFG_RENAMED( tiles.net.xdp_tx_queue_size,    net.xdp.xdp_tx_queue_size    );
  CFG_RENAMED( tiles.net.flush_timeout_micros, net.xdp.flush_timeout_micros );
  CFG_RENAMED( tiles.net.send_buffer_size,     net.ingress_buffer_size      );

  CFG_RENAMED( development.net.provider,                 net.provider                   );
  CFG_RENAMED( development.net.sock_receive_buffer_size, net.socket.receive_buffer_size );
  CFG_RENAMED( development.net.sock_send_buffer_size,    net.socket.send_buffer_size    );

  CFG_RENAMED( tiles.repair.repair_intake_listen_port,   tiles.repair.repair_client_listen_port );

# undef CFG_RENAMED

  if( FD_UNLIKELY( !fdctl_pod_find_leftover( pod ) ) ) return NULL;
  return config;
}

#undef CFG_POP
#undef CFG_ARRAY
