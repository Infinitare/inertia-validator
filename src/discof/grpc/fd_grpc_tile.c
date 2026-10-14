#include "proto/stream.pb.h"
#include "../replay/fd_replay_tile.h"

#include "../../third_party/nanopb/pb_decode.h"
#include "../../ballet/pb/fd_pb_encode.h"
#include "../../disco/tiles.h"
#include "../../disco/metrics/fd_metrics.h"
#include "../../disco/topo/fd_topo.h"
#include "../../util/io/fd_io.h"
#include "../../util/net/fd_ip4.h"
#include "../../waltz/grpc/fd_grpc_server.h"

#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "generated/fd_grpc_tile_seccomp.h"

#define SUBSCRIBE_PATH "/inertia.v1.Stream/Subscribe"

#define SUB_ACCT_MAX (256UL)

#define MSG_MAX (16384UL)

#define GRPC_STATUS_INVALID_ARGUMENT (3U)

#define UPDATE_FIELD_TXN  (1U)
#define UPDATE_FIELD_GAP  (2U)

#define IN_KIND_REPLAY (0)
#define IN_KIND_EXECLE (1)

#define BLOOM_BITS  (1UL<<18)
#define BLOOM_WORDS (BLOOM_BITS>>6)
#define BLOOM_MASK  (BLOOM_BITS-1UL)

struct __attribute__((aligned(64))) fd_grpc_acct_entry {
  uchar key[ 32 ];
  ulong mask;
  ulong gen;
};

typedef struct fd_grpc_acct_entry fd_grpc_acct_entry_t;

struct fd_grpc_sub {
  ulong acct_cnt;
  uchar include_votes;
  uchar include_failed;
  uchar accts[ SUB_ACCT_MAX ][ 32 ];
};

typedef struct fd_grpc_sub fd_grpc_sub_t;

struct fd_grpc_in {
  int         kind;
  fd_wksp_t * mem;
  ulong       chunk0;
  ulong       wmark;
  ulong       mtu;
  ulong       next_seq;
  int         seq_valid;
};

typedef struct fd_grpc_in fd_grpc_in_t;

struct fd_grpc_tile {
  fd_grpc_server_t * server;

  fd_grpc_sub_t *        sub;
  fd_grpc_acct_entry_t * acct_map;
  ulong                  acct_map_cnt;
  ulong                  acct_map_mask;
  ulong                  acct_gen;
  ulong *                bloom;
  ulong                  max_sub;

  ulong sub_mask;
  ulong txn_mask;
  ulong vote_mask;
  ulong failed_mask;

  ulong last_slot;
  ulong txn_oversz;
  ulong gap_cnt;
  ulong gap_total;

  long next_poll_deadline;
  long poll_interval_ticks;

  union {
    fd_replay_message_t replay;
    fd_txn_p_t          txns[ MAX_TXN_PER_MICROBLOCK ];
    uchar               raw[ FD_EXECLE_POH_MTU ];
  } buf[ 1 ];
  inertia_v1_SubscribeRequest req[ 1 ];

  fd_grpc_in_t in[ FD_TOPO_MAX_TILE_IN_LINKS ];
};

typedef struct fd_grpc_tile fd_grpc_tile_t;

static inline fd_grpc_server_params_t
derive_params( fd_topo_tile_t const * tile ) {
  ulong max_sub = tile->grpc.max_subscriptions;
  return (fd_grpc_server_params_t){
    .max_conn_cnt         = max_sub,
    .max_stream_cnt       = max_sub,
    .max_streams_per_conn = fd_ulong_min( max_sub, 4UL ),
    .max_req_msg_sz       = 16384UL,
    .tx_desc_cnt          = tile->grpc.tx_depth,
    .oring_sz             = tile->grpc.send_buffer_size_mb<<20,
    .conn_rx_bufsz        = 32768UL,
    .conn_tx_bufsz        = 32768UL,
    .initial_window_size  = 1U<<20
  };
}

FD_FN_PURE static inline ulong
acct_map_cnt( fd_topo_tile_t const * tile ) {
  return fd_ulong_pow2_up( 4UL*tile->grpc.max_subscriptions*SUB_ACCT_MAX );
}

FD_FN_CONST static inline ulong
scratch_align( void ) {
  ulong a = alignof( fd_grpc_tile_t );
  a = fd_ulong_max( a, fd_grpc_server_align()          );
  a = fd_ulong_max( a, alignof( fd_grpc_sub_t )        );
  a = fd_ulong_max( a, alignof( fd_grpc_acct_entry_t ) );
  return a;
}

static inline ulong
scratch_footprint( fd_topo_tile_t const * tile ) {
  fd_grpc_server_params_t params = derive_params( tile );
  ulong server_fp = fd_grpc_server_footprint( &params );
  if( FD_UNLIKELY( !server_fp ) ) FD_LOG_ERR(( "Invalid [tiles.grpc] config parameters" ));

  ulong l = FD_LAYOUT_INIT;
  l = FD_LAYOUT_APPEND( l, alignof( fd_grpc_tile_t ),        sizeof( fd_grpc_tile_t )                              );
  l = FD_LAYOUT_APPEND( l, fd_grpc_server_align(),           server_fp                                             );
  l = FD_LAYOUT_APPEND( l, alignof( fd_grpc_sub_t ),         tile->grpc.max_subscriptions*sizeof( fd_grpc_sub_t )  );
  l = FD_LAYOUT_APPEND( l, alignof( fd_grpc_acct_entry_t ),  acct_map_cnt( tile )*sizeof( fd_grpc_acct_entry_t )   );
  l = FD_LAYOUT_APPEND( l, 64UL,                             BLOOM_WORDS*sizeof( ulong )                           );
  return FD_LAYOUT_FINI( l, scratch_align() );
}

static inline ulong
acct_hash0( uchar const * key ) { return fd_ulong_hash( FD_LOAD( ulong, key     ) ); }

static inline ulong
acct_hash1( uchar const * key ) { return fd_ulong_hash( FD_LOAD( ulong, key+8UL ) ); }

static void
acct_map_insert( fd_grpc_tile_t * ctx,
                 uchar const *    key,
                 ulong            bit ) {
  ulong h0 = acct_hash0( key );
  ulong h1 = acct_hash1( key );

  ctx->bloom[ ( h0 & BLOOM_MASK )>>6 ] |= 1UL<<( h0 & 63UL );
  ctx->bloom[ ( h1 & BLOOM_MASK )>>6 ] |= 1UL<<( h1 & 63UL );

  ulong i = h0 & ctx->acct_map_mask;
  for( ulong probe=0UL; probe<ctx->acct_map_cnt; probe++ ) {
    fd_grpc_acct_entry_t * e = &ctx->acct_map[ i ];
    if( e->gen!=ctx->acct_gen ) {
      memcpy( e->key, key, 32UL );
      e->mask = bit;
      e->gen  = ctx->acct_gen;
      return;
    }
    if( !memcmp( e->key, key, 32UL ) ) {
      e->mask |= bit;
      return;
    }
    i = (i+1UL) & ctx->acct_map_mask;
  }
  FD_LOG_ERR(( "grpc account index full (%lu entries)", ctx->acct_map_cnt ));
}

FD_FN_PURE static inline ulong
acct_map_query( fd_grpc_tile_t const * ctx,
                uchar const *          key ) {
  ulong h0 = acct_hash0( key );
  ulong h1 = acct_hash1( key );

  if( FD_LIKELY( !( ( ctx->bloom[ ( h0 & BLOOM_MASK )>>6 ]>>( h0 & 63UL ) ) &
                    ( ctx->bloom[ ( h1 & BLOOM_MASK )>>6 ]>>( h1 & 63UL ) ) & 1UL ) ) ) return 0UL;

  ulong i = h0 & ctx->acct_map_mask;
  for( ulong probe=0UL; probe<ctx->acct_map_cnt; probe++ ) {
    fd_grpc_acct_entry_t const * e = &ctx->acct_map[ i ];
    if( e->gen!=ctx->acct_gen ) return 0UL;
    if( !memcmp( e->key, key, 32UL ) ) return e->mask;
    i = (i+1UL) & ctx->acct_map_mask;
  }
  return 0UL;
}

static void
index_rebuild( fd_grpc_tile_t * ctx ) {
  if( FD_UNLIKELY( !++ctx->acct_gen ) ) {
    memset( ctx->acct_map, 0, ctx->acct_map_cnt*sizeof( fd_grpc_acct_entry_t ) );
    ctx->acct_gen = 1UL;
  }
  memset( ctx->bloom, 0, BLOOM_WORDS*sizeof(ulong) );

  ctx->txn_mask    = 0UL;
  ctx->vote_mask   = 0UL;
  ctx->failed_mask = 0UL;

  for( ulong m=ctx->sub_mask; m; m &= m-1UL ) {
    ulong           idx = (ulong)fd_ulong_find_lsb( m );
    fd_grpc_sub_t * sub = &ctx->sub[ idx ];
    ulong           bit = 1UL<<idx;

    if( sub->include_votes  ) ctx->vote_mask   |= bit;
    if( sub->include_failed ) ctx->failed_mask |= bit;

    if( sub->acct_cnt ) ctx->txn_mask |= bit;
    for( ulong j=0UL; j<sub->acct_cnt; j++ ) acct_map_insert( ctx, sub->accts[ j ], bit );
  }
}

static void
emit_gap( fd_grpc_tile_t * ctx ) {
  uchar * out = fd_grpc_server_msg_stage( ctx->server, MSG_MAX );
  if( FD_UNLIKELY( !out ) ) return;

  fd_pb_encoder_t enc[ 1 ];
  fd_pb_encoder_init( enc, out, MSG_MAX );

  fd_pb_submsg_open ( enc, UPDATE_FIELD_GAP );
  fd_pb_push_uint64 ( enc, 1U, ctx->gap_cnt   );
  fd_pb_push_uint64 ( enc, 2U, ctx->last_slot );
  fd_pb_submsg_close( enc );

  fd_grpc_server_msg_broadcast( ctx->server, fd_pb_encoder_out_sz( enc ), ctx->sub_mask );
  ctx->gap_cnt = 0UL;
}

static void
emit_txn( fd_grpc_tile_t *   ctx,
          ulong              slot,
          fd_txn_p_t const * txnp,
          int                txn_err ) {
  if( FD_LIKELY( !ctx->txn_mask ) ) return;

  fd_txn_t const *   txn     = (fd_txn_t const *)txnp->_;
  uchar const *      payload = txnp->payload;

  ulong acct_cnt  = txn->acct_addr_cnt;
  ulong instr_cnt = txn->instr_cnt;
  if( FD_UNLIKELY( acct_cnt>FD_TXN_ACCT_ADDR_MAX || instr_cnt>FD_TXN_INSTR_MAX || txnp->payload_sz>FD_TXN_MTU ) ) return;

  uchar const * accts = (uchar const *)fd_txn_get_acct_addrs( txn, payload );

  ulong mask = 0UL;
  for( ulong i=0UL; i<acct_cnt; i++ ) mask |= acct_map_query( ctx, accts + 32UL*i );
  if( FD_LIKELY( !mask ) ) return;

  int is_vote = fd_txn_is_simple_vote_transaction( txn, payload );
  if( is_vote ) mask &= ctx->vote_mask;

  int success = ( txn_err==0 );
  if( !success ) mask &= ctx->failed_mask;

  mask &= ctx->sub_mask;
  if( FD_UNLIKELY( !mask ) ) return;

  /* Upper bound on the encoding.  fd_pb_encode reserves a worst-case
     5 byte length prefix for every submessage and bytes field, so this
     is well above the typical encoded size. */

  ulong bound = 128UL + acct_cnt*38UL + instr_cnt*30UL + txnp->payload_sz + 32UL;
  if( FD_UNLIKELY( bound>MSG_MAX ) ) { ctx->txn_oversz++; return; }

  if( FD_UNLIKELY( ctx->gap_cnt ) ) emit_gap( ctx );

  uchar * out = fd_grpc_server_msg_stage( ctx->server, MSG_MAX );
  if( FD_UNLIKELY( !out ) ) return;

  fd_pb_encoder_t enc[ 1 ];
  fd_pb_encoder_init( enc, out, MSG_MAX );

  fd_pb_submsg_open( enc, UPDATE_FIELD_TXN );
  fd_pb_push_uint64( enc, 1U, slot );
  fd_pb_push_bytes ( enc, 2U, fd_txn_get_signatures( txn, payload ), 64UL );
  fd_pb_push_bool  ( enc, 3U, success );
  fd_pb_push_int64 ( enc, 4U, (long)txn_err );
  fd_pb_push_bool  ( enc, 5U, is_vote );

  for( ulong i=0UL; i<acct_cnt; i++ ) fd_pb_push_bytes( enc, 6U, accts + 32UL*i, 32UL );

  for( ulong i=0UL; i<instr_cnt; i++ ) {
    fd_txn_instr_t const * instr = &txn->instr[ i ];
    fd_pb_submsg_open( enc, 7U );
    fd_pb_push_uint32( enc, 1U, instr->program_id );
    fd_pb_push_bytes ( enc, 2U, fd_txn_get_instr_accts( instr, payload ), instr->acct_cnt );
    fd_pb_push_bytes ( enc, 3U, fd_txn_get_instr_data ( instr, payload ), instr->data_sz  );
    fd_pb_submsg_close( enc );
  }
  fd_pb_submsg_close( enc );

  fd_grpc_server_msg_broadcast( ctx->server, fd_pb_encoder_out_sz( enc ), mask );
}

static int
cb_request_start( void *       _ctx,
                  ulong        stream_idx,
                  char const * path,
                  ulong        path_len ) {
  fd_grpc_tile_t * ctx = _ctx;
  if( FD_UNLIKELY( stream_idx>=ctx->max_sub ) ) return 0;
  if( FD_UNLIKELY( path_len!=sizeof(SUBSCRIBE_PATH)-1UL ) ) return 0;
  if( FD_UNLIKELY( memcmp( path, SUBSCRIBE_PATH, path_len ) ) ) return 0;
  return 1;
}

static void
cb_request_msg( void *        _ctx,
                ulong         stream_idx,
                uchar const * msg,
                ulong         msg_sz ) {
  fd_grpc_tile_t * ctx = _ctx;
  ulong            bit = 1UL<<stream_idx;

  if( FD_UNLIKELY( ctx->sub_mask & bit ) ) {
    fd_grpc_server_stream_close( ctx->server, stream_idx, GRPC_STATUS_INVALID_ARGUMENT, "only one request permitted", 26UL );
    return;
  }

  inertia_v1_SubscribeRequest * req = ctx->req;
  *req = (inertia_v1_SubscribeRequest)inertia_v1_SubscribeRequest_init_zero;
  pb_istream_t stream = pb_istream_from_buffer( msg, msg_sz );
  if( FD_UNLIKELY( !pb_decode( &stream, inertia_v1_SubscribeRequest_fields, req ) ) ) {
    fd_grpc_server_stream_close( ctx->server, stream_idx, GRPC_STATUS_INVALID_ARGUMENT, "malformed SubscribeRequest", 26UL );
    return;
  }

  fd_grpc_sub_t * sub = &ctx->sub[ stream_idx ];
  sub->acct_cnt       = 0UL;
  sub->include_votes  = (uchar)!!req->include_votes;
  sub->include_failed = (uchar)!!req->include_failed;

  for( ulong i=0UL; i<req->accounts_count; i++ ) {
    if( FD_UNLIKELY( req->accounts[ i ].size!=32UL ) ) {
      fd_grpc_server_stream_close( ctx->server, stream_idx, GRPC_STATUS_INVALID_ARGUMENT, "account key must be 32 bytes", 28UL );
      return;
    }
    memcpy( sub->accts[ sub->acct_cnt++ ], req->accounts[ i ].bytes, 32UL );
  }

  ctx->sub_mask |= bit;
  index_rebuild( ctx );
}

static void
cb_request_fin( void * _ctx,
                ulong  stream_idx ) {
  fd_grpc_tile_t * ctx = _ctx;
  if( FD_UNLIKELY( !( ctx->sub_mask & (1UL<<stream_idx) ) ) ) {
    fd_grpc_server_stream_close( ctx->server, stream_idx, GRPC_STATUS_INVALID_ARGUMENT, "missing SubscribeRequest", 24UL );
  }
}

static void
cb_stream_close( void * _ctx,
                 ulong  stream_idx,
                 int    reason ) {
  (void)reason;
  fd_grpc_tile_t * ctx = _ctx;
  ulong            bit = 1UL<<stream_idx;
  if( FD_LIKELY( ctx->sub_mask & bit ) ) {
    ctx->sub_mask &= ~bit;
    if( FD_UNLIKELY( !ctx->sub_mask ) ) ctx->gap_cnt = 0UL;
    index_rebuild( ctx );
  }
}

static fd_grpc_tile_t *
tile_server_init( void *                 scratch,
                  fd_topo_tile_t const * tile ) {
  fd_grpc_server_params_t params = derive_params( tile );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_grpc_tile_t * ctx     = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_grpc_tile_t ), sizeof( fd_grpc_tile_t )              );
  void *           _server = FD_SCRATCH_ALLOC_APPEND( l, fd_grpc_server_align(),    fd_grpc_server_footprint( &params )   );

  fd_grpc_server_callbacks_t cb = {
    .request_start = cb_request_start,
    .request_msg   = cb_request_msg,
    .request_fin   = cb_request_fin,
    .stream_close  = cb_stream_close
  };

  ctx->server = fd_grpc_server_join( fd_grpc_server_new( _server, &params, &cb, ctx, 0UL ) );
  FD_TEST( ctx->server );

  int err = fd_grpc_server_listen( ctx->server, tile->grpc.listen_addr, tile->grpc.listen_port );
  if( FD_UNLIKELY( err ) ) FD_LOG_ERR(( "grpc listen failed (%i-%s)", err, fd_io_strerror( err ) ));

  return ctx;
}

static fd_grpc_tile_t *
tile_ctx_init( void *                 scratch,
               fd_topo_tile_t const * tile ) {
  fd_grpc_server_params_t params = derive_params( tile );

  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_grpc_tile_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_grpc_tile_t ),       sizeof( fd_grpc_tile_t )                             );
  /**/                   FD_SCRATCH_ALLOC_APPEND( l, fd_grpc_server_align(),          fd_grpc_server_footprint( &params )                  );
  void * _sub          = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_grpc_sub_t ),        tile->grpc.max_subscriptions*sizeof( fd_grpc_sub_t ) );
  void * _acct_map     = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_grpc_acct_entry_t ), acct_map_cnt( tile )*sizeof( fd_grpc_acct_entry_t )  );
  void * _bloom        = FD_SCRATCH_ALLOC_APPEND( l, 64UL,                            BLOOM_WORDS*sizeof( ulong )                          );
  ulong  scratch_top   = FD_SCRATCH_ALLOC_FINI( l, scratch_align() );

  if( FD_UNLIKELY( scratch_top > (ulong)scratch + scratch_footprint( tile ) ) )
    FD_LOG_ERR(( "scratch overflow %lu %lu %lu", scratch_top - (ulong)scratch - scratch_footprint( tile ), scratch_top, (ulong)scratch + scratch_footprint( tile ) ));

  ctx->sub           = _sub;
  ctx->acct_map      = _acct_map;
  ctx->acct_map_cnt  = acct_map_cnt( tile );
  ctx->acct_map_mask = ctx->acct_map_cnt-1UL;
  ctx->acct_gen      = 0UL;
  ctx->bloom         = _bloom;
  ctx->max_sub       = tile->grpc.max_subscriptions;

  ctx->sub_mask    = 0UL;
  ctx->txn_mask    = 0UL;
  ctx->vote_mask   = 0UL;
  ctx->failed_mask = 0UL;

  ctx->last_slot  = 0UL;
  ctx->txn_oversz = 0UL;
  ctx->gap_cnt    = 0UL;
  ctx->gap_total  = 0UL;

  memset( ctx->acct_map, 0, ctx->acct_map_cnt*sizeof( fd_grpc_acct_entry_t ) );
  memset( ctx->bloom,    0, BLOOM_WORDS*sizeof( ulong )                      );

  ctx->next_poll_deadline  = fd_tickcount();
  ctx->poll_interval_ticks = (long)( fd_tempo_tick_per_ns( NULL )*128.0*1000.0 );

  return ctx;
}

static void
privileged_init( fd_topo_t const *      topo,
                 fd_topo_tile_t const * tile ) {
  tile_server_init( fd_topo_obj_laddr( topo, tile->tile_obj_id ), tile );

  FD_LOG_NOTICE(( "grpc server listening at " FD_IP4_ADDR_FMT ":%u", FD_IP4_ADDR_FMT_ARGS( tile->grpc.listen_addr ), tile->grpc.listen_port ));
}

static void
unprivileged_init( fd_topo_t const *      topo,
                   fd_topo_tile_t const * tile ) {
  fd_grpc_tile_t * ctx = tile_ctx_init( fd_topo_obj_laddr( topo, tile->tile_obj_id ), tile );

  FD_TEST( tile->in_cnt<=sizeof( ctx->in )/sizeof( ctx->in[ 0 ] ) );
  for( ulong i=0UL; i<tile->in_cnt; i++ ) {
    fd_topo_link_t const * link      = &topo->links[ tile->in_link_id[ i ] ];
    fd_topo_wksp_t const * link_wksp = &topo->workspaces[ topo->objs[ link->dcache_obj_id ].wksp_id ];

    if     ( !strcmp( link->name, "replay_out" ) ) ctx->in[ i ].kind = IN_KIND_REPLAY;
    else if( !strcmp( link->name, "execle_poh" ) ) ctx->in[ i ].kind = IN_KIND_EXECLE;
    else FD_LOG_ERR(( "unexpected link name %s", link->name ));

    FD_TEST( link->mtu<=sizeof( ctx->buf ) );
    ctx->in[ i ].mem       = link_wksp->wksp;
    ctx->in[ i ].chunk0    = fd_dcache_compact_chunk0( ctx->in[ i ].mem, link->dcache );
    ctx->in[ i ].wmark     = fd_dcache_compact_wmark ( ctx->in[ i ].mem, link->dcache, link->mtu );
    ctx->in[ i ].mtu       = link->mtu;
    ctx->in[ i ].next_seq  = 0UL;
    ctx->in[ i ].seq_valid = 0;
  }
}

static void
metrics_write( fd_grpc_tile_t * ctx ) {
  fd_grpc_server_metrics_t const * m = fd_grpc_server_metrics( ctx->server );

  FD_MGAUGE_SET( GRPC, SUBSCRIPTION_COUNT, (ulong)fd_ulong_popcnt( ctx->sub_mask ) );
  FD_MCNT_SET(   GRPC, CONN_ACCEPTED,      m->conn_accepted   );
  FD_MCNT_SET(   GRPC, CONN_REJECTED,      m->conn_rejected   );
  FD_MCNT_SET(   GRPC, CONN_ERR,           m->conn_err        );
  FD_MCNT_SET(   GRPC, STREAM_OPENED,      m->stream_opened   );
  FD_MCNT_SET(   GRPC, STREAM_REJECTED,    m->stream_rejected );
  FD_MCNT_SET(   GRPC, STREAM_EVICTED,     m->stream_evicted  );
  FD_MCNT_SET(   GRPC, MSG_STAGED,         m->msg_staged      );
  FD_MCNT_SET(   GRPC, MSG_QUEUED,         m->msg_queued      );
  FD_MCNT_SET(   GRPC, MSG_DROPPED,        m->msg_dropped     );
  FD_MCNT_SET(   GRPC, MSG_ABANDONED,      m->msg_abandoned   );
  FD_MCNT_SET(   GRPC, TXN_OVERSZ,         ctx->txn_oversz    );
  FD_MCNT_SET(   GRPC, BYTES_TX,           m->bytes_tx        );
  FD_MCNT_SET(   GRPC, FRAGS_DROPPED,      ctx->gap_total     );
}

static void
before_credit( fd_grpc_tile_t *    ctx,
               fd_stem_context_t * stem,
               int *               charge_busy ) {
  (void)stem;

  long now = fd_tickcount();
  if( FD_UNLIKELY( fd_grpc_server_tx_urgent( ctx->server ) || now>=ctx->next_poll_deadline ) ) {
    *charge_busy = fd_grpc_server_poll( ctx->server );
    ctx->next_poll_deadline = fd_tickcount() + ctx->poll_interval_ticks;
  }
}

static int
before_frag( fd_grpc_tile_t * ctx,
             ulong            in_idx,
             ulong            seq,
             ulong            sig ) {
  fd_grpc_in_t * in = &ctx->in[ in_idx ];
  if( FD_UNLIKELY( !in->seq_valid ) ) {
    in->seq_valid = 1;
  } else if( FD_UNLIKELY( seq!=in->next_seq ) ) {
    ulong missed = (ulong)fd_seq_diff( seq, in->next_seq );
    if( FD_LIKELY( ctx->sub_mask ) ) ctx->gap_cnt += missed;
    ctx->gap_total += missed;
  }
  in->next_seq = fd_seq_inc( seq, 1UL );

  if( FD_LIKELY( !ctx->txn_mask ) ) return 1;
  return in->kind==IN_KIND_REPLAY && sig!=REPLAY_SIG_TXN_EXECUTED;
}

static void
during_frag( fd_grpc_tile_t * ctx,
             ulong            in_idx,
             ulong            seq FD_PARAM_UNUSED,
             ulong            sig FD_PARAM_UNUSED,
             ulong            chunk,
             ulong            sz,
             ulong            ctl FD_PARAM_UNUSED ) {
  fd_grpc_in_t const * in = &ctx->in[ in_idx ];
  if( FD_UNLIKELY( chunk<in->chunk0 || chunk>in->wmark || sz>in->mtu ||
                   ( in->kind==IN_KIND_EXECLE && sz<sizeof(fd_microblock_trailer_t) ) ) ) {
    FD_LOG_ERR(( "chunk %lu sz %lu out of bounds [%lu,%lu]", chunk, sz, in->chunk0, in->wmark ));
  }
  memcpy( ctx->buf, fd_chunk_to_laddr_const( in->mem, chunk ), sz );
}

static void
after_frag( fd_grpc_tile_t *    ctx,
            ulong               in_idx,
            ulong               seq    FD_PARAM_UNUSED,
            ulong               sig,
            ulong               sz,
            ulong               tsorig FD_PARAM_UNUSED,
            ulong               tspub  FD_PARAM_UNUSED,
            fd_stem_context_t * stem   FD_PARAM_UNUSED ) {
  switch( ctx->in[ in_idx ].kind ) {
    case IN_KIND_REPLAY: {
      if( FD_UNLIKELY( sig!=REPLAY_SIG_TXN_EXECUTED ) ) break;
      fd_replay_txn_executed_t const * exec = &ctx->buf->replay.txn_executed;
      ctx->last_slot = exec->slot;
      emit_txn( ctx, exec->slot, exec->txn, exec->txn_err );
      break;
    }
    case IN_KIND_EXECLE: {
      ctx->last_slot = fd_disco_execle_sig_slot( sig );
      ulong txn_cnt = ( sz-sizeof(fd_microblock_trailer_t) )/sizeof(fd_txn_p_t);
      for( ulong i=0UL; i<txn_cnt; i++ ) {
        fd_txn_p_t const * txnp = &ctx->buf->txns[ i ];
        if( !( txnp->flags & FD_TXN_P_FLAGS_EXECUTE_SUCCESS ) ) continue;
        emit_txn( ctx, ctx->last_slot, txnp, -(int)( txnp->flags>>24 ) );
      }
      break;
    }
    default:
      break;
  }
}

static ulong
populate_allowed_seccomp( fd_topo_t const *      topo,
                          fd_topo_tile_t const * tile,
                          ulong                  out_cnt,
                          struct sock_filter *   out ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );
  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_grpc_tile_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_grpc_tile_t ), sizeof( fd_grpc_tile_t ) );

  populate_sock_filter_policy_fd_grpc_tile( out_cnt, out, (uint)fd_log_private_logfile_fd(), (uint)fd_grpc_server_fd( ctx->server ) );
  return sock_filter_policy_fd_grpc_tile_instr_cnt;
}

static ulong
populate_allowed_fds( fd_topo_t const *      topo,
                      fd_topo_tile_t const * tile,
                      ulong                  out_fds_cnt,
                      int *                  out_fds ) {
  void * scratch = fd_topo_obj_laddr( topo, tile->tile_obj_id );
  FD_SCRATCH_ALLOC_INIT( l, scratch );
  fd_grpc_tile_t * ctx = FD_SCRATCH_ALLOC_APPEND( l, alignof( fd_grpc_tile_t ), sizeof( fd_grpc_tile_t ) );

  if( FD_UNLIKELY( out_fds_cnt<3UL ) ) FD_LOG_ERR(( "out_fds_cnt %lu", out_fds_cnt ));

  ulong out_cnt = 0UL;
  out_fds[ out_cnt++ ] = 2; /* stderr */
  if( FD_LIKELY( -1!=fd_log_private_logfile_fd() ) )
    out_fds[ out_cnt++ ] = fd_log_private_logfile_fd();
  out_fds[ out_cnt++ ] = fd_grpc_server_fd( ctx->server );
  return out_cnt;
}

static ulong
rlimit_file_cnt( fd_topo_t const *      topo FD_PARAM_UNUSED,
                 fd_topo_tile_t const * tile ) {
  /* pipefd, listen socket, stderr, logfile, and one spare for accept() */
  return 5UL+tile->grpc.max_subscriptions;
}

#define STEM_BURST (1UL)
#define STEM_LAZY  (128L*3000L)

#define STEM_CALLBACK_CONTEXT_TYPE  fd_grpc_tile_t
#define STEM_CALLBACK_CONTEXT_ALIGN alignof(fd_grpc_tile_t)

#define STEM_CALLBACK_METRICS_WRITE metrics_write
#define STEM_CALLBACK_BEFORE_CREDIT before_credit
#define STEM_CALLBACK_BEFORE_FRAG   before_frag
#define STEM_CALLBACK_DURING_FRAG   during_frag
#define STEM_CALLBACK_AFTER_FRAG    after_frag

#include "../../disco/stem/fd_stem.c"

#ifndef FD_TILE_TEST
fd_topo_run_tile_t fd_tile_grpc = {
  .name                     = "grpc",
  .rlimit_file_cnt_fn       = rlimit_file_cnt,
  .populate_allowed_seccomp = populate_allowed_seccomp,
  .populate_allowed_fds     = populate_allowed_fds,
  .scratch_align            = scratch_align,
  .scratch_footprint        = scratch_footprint,
  .privileged_init          = privileged_init,
  .unprivileged_init        = unprivileged_init,
  .run                      = stem_run,
};
#endif
