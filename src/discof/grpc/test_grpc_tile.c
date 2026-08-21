#include "fd_grpc_tile.c"

#include "../../ballet/base64/fd_base64.h"
#include "../../flamenco/runtime/fd_runtime_err.h"

#include <stdlib.h>

FD_IMPORT_BINARY( transaction1, "src/ballet/txn/fixtures/transaction1.bin" );
FD_IMPORT_BINARY( transaction2, "src/ballet/txn/fixtures/transaction2.bin" );
FD_IMPORT_BINARY( transaction3, "src/ballet/txn/fixtures/transaction3.bin" );
FD_IMPORT_BINARY( transaction4, "src/ballet/txn/fixtures/transaction4.bin" );
FD_IMPORT_BINARY( transaction5, "src/ballet/txn/fixtures/transaction5.bin" );
FD_IMPORT_BINARY( transaction6, "src/ballet/txn/fixtures/transaction6.bin" );

#define FIXTURE_CNT (6UL)

struct fixture {
  uchar const * payload;
  ulong         payload_sz;
};

static void
print_accounts( fd_txn_p_t const * txnp,
                ulong              idx ) {
  fd_txn_t const * txn   = (fd_txn_t const *)txnp->_;
  uchar const *    accts = (uchar const *)fd_txn_get_acct_addrs( txn, txnp->payload );

  for( ulong i=0UL; i<txn->acct_addr_cnt; i++ ) {
    char b64[ FD_BASE64_ENC_SZ( 32UL )+1UL ];
    ulong n = fd_base64_encode( b64, accts+32UL*i, 32UL );
    b64[ n ] = '\0';
    FD_LOG_NOTICE(( "  txn[%lu] account[%lu] %s", idx, i, b64 ));
  }
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  char const * listen_addr_cstr = fd_env_strip_cmdline_cstr ( &argc, &argv, "--listen", NULL, "127.0.0.1" );
  ulong        listen_port      = fd_env_strip_cmdline_ulong( &argc, &argv, "--port",   NULL, 10800UL     );
  ulong        max_sub          = fd_env_strip_cmdline_ulong( &argc, &argv, "--subs",   NULL, 64UL        );
  ulong        tx_depth         = fd_env_strip_cmdline_ulong( &argc, &argv, "--depth",  NULL, 4096UL      );
  ulong        ring_mb          = fd_env_strip_cmdline_ulong( &argc, &argv, "--ring-mb",NULL, 32UL        );
  ulong        rate             = fd_env_strip_cmdline_ulong( &argc, &argv, "--rate",   NULL, 100UL       );

  uint listen_addr;
  if( FD_UNLIKELY( !fd_cstr_to_ip4_addr( listen_addr_cstr, &listen_addr ) ) )
    FD_LOG_ERR(( "invalid --listen `%s`", listen_addr_cstr ));

  fd_topo_tile_t * tile = aligned_alloc( alignof(fd_topo_tile_t), sizeof(fd_topo_tile_t) );
  FD_TEST( tile );
  memset( tile, 0, sizeof(fd_topo_tile_t) );
  tile->grpc.listen_addr         = listen_addr;
  tile->grpc.listen_port         = (ushort)listen_port;
  tile->grpc.max_subscriptions   = max_sub;
  tile->grpc.tx_depth            = tx_depth;
  tile->grpc.send_buffer_size_mb = ring_mb;

  ulong  fp      = fd_ulong_align_up( scratch_footprint( tile ), scratch_align() );
  void * scratch = aligned_alloc( scratch_align(), fp );
  FD_TEST( scratch );
  memset( scratch, 0, fp );

  tile_server_init( scratch, tile );
  fd_grpc_tile_t * ctx = tile_ctx_init( scratch, tile );

  ctx->last_slot = 300000000UL;

  struct fixture fixtures[ FIXTURE_CNT ] = {
    { transaction1, transaction1_sz }, { transaction2, transaction2_sz },
    { transaction3, transaction3_sz }, { transaction4, transaction4_sz },
    { transaction5, transaction5_sz }, { transaction6, transaction6_sz }
  };

  fd_replay_txn_executed_t * execs = aligned_alloc( alignof(fd_replay_txn_executed_t), FIXTURE_CNT*sizeof(fd_replay_txn_executed_t) );
  FD_TEST( execs );
  memset( execs, 0, FIXTURE_CNT*sizeof(fd_replay_txn_executed_t) );

  ulong txn_cnt = 0UL;
  for( ulong i=0UL; i<FIXTURE_CNT; i++ ) {
    fd_txn_p_t * txnp = execs[ txn_cnt ].txn;
    if( FD_UNLIKELY( fixtures[ i ].payload_sz>FD_TPU_MTU ) ) continue;
    memcpy( txnp->payload, fixtures[ i ].payload, fixtures[ i ].payload_sz );
    txnp->payload_sz = fixtures[ i ].payload_sz;
    if( FD_UNLIKELY( !fd_txn_parse( txnp->payload, txnp->payload_sz, txnp->_, NULL ) ) ) {
      FD_LOG_NOTICE(( "fixture %lu rejected by fd_txn_parse, skipping", i+1UL ));
      continue;
    }
    /* Alternate success and failure so include_failed is exercised. */
    execs[ txn_cnt ].txn_err = ( txn_cnt & 1UL ) ? FD_RUNTIME_TXN_ERR_ACCOUNT_LOADED_TWICE : 0;
    txn_cnt++;
  }
  FD_TEST( txn_cnt );

  FD_LOG_NOTICE(( "listening at " FD_IP4_ADDR_FMT ":%lu with %lu transactions, %lu updates/sec",
                  FD_IP4_ADDR_FMT_ARGS( listen_addr ), listen_port, txn_cnt, rate ));
  FD_LOG_NOTICE(( "subscribe to any of these account keys (base64, as grpcurl expects):" ));
  for( ulong i=0UL; i<txn_cnt; i++ ) print_accounts( execs[ i ].txn, i );

  ulong base_slot = ctx->last_slot;
  long interval = rate ? (long)(1000000000UL/rate) : 1000000000L;
  long next     = fd_log_wallclock();
  ulong seq     = 0UL;

  for(;;) {
    fd_grpc_server_poll( ctx->server );

    long now = fd_log_wallclock();
    if( now<next ) continue;
    next += interval;

    if( seq%32UL==0UL ) {
      uchar block_hash[ 32 ];
      memset( block_hash, (int)(ctx->last_slot & 0xffUL), 32UL );
      emit_slot( ctx, ctx->last_slot, ctx->last_slot-1UL, ctx->last_slot/2UL, block_hash,
                 (uint)inertia_v1_CommitmentLevel_COMMITMENT_LEVEL_PROCESSED );

      if( ctx->last_slot>=base_slot+2UL )
        emit_slot( ctx, ctx->last_slot-2UL, 0UL, 0UL, NULL,
                   (uint)inertia_v1_CommitmentLevel_COMMITMENT_LEVEL_CONFIRMED );
      if( ctx->last_slot>=base_slot+32UL )
        emit_slot( ctx, ctx->last_slot-32UL, 0UL, 0UL, NULL,
                   (uint)inertia_v1_CommitmentLevel_COMMITMENT_LEVEL_FINALIZED );

      ctx->last_slot++;
    }

    fd_replay_txn_executed_t * exec = &execs[ seq%txn_cnt ];
    exec->slot = ctx->last_slot;
    emit_txn( ctx, exec );
    seq++;
  }

  return 0;
}