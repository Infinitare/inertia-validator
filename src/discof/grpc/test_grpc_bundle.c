#define fd_grpc_server_msg_stage     test_msg_stage
#define fd_grpc_server_msg_broadcast test_msg_broadcast
#include "fd_grpc_tile.c"

#include "../../ballet/pb/fd_pb_tokenize.h"
#include "../../flamenco/runtime/fd_executor_err.h"
#include "../../flamenco/runtime/fd_runtime_err.h"
#include "../../util/tmpl/fd_unit_test.c"

#include <stdlib.h>

#define TEST_OUT_MAX (8UL)
#define TEST_SLOT    (300000000UL)

struct test_out {
  uchar buf[ MSG_MAX ];
  ulong sz;
  ulong mask;
};

typedef struct test_out test_out_t;

static uchar      test_stage[ MSG_MAX ];
static test_out_t test_out[ TEST_OUT_MAX ];
static ulong      test_out_cnt;

uchar *
test_msg_stage( fd_grpc_server_t * server,
                ulong              msg_max ) {
  (void)server;
  FD_TEST( msg_max<=sizeof(test_stage) );
  return test_stage;
}

void
test_msg_broadcast( fd_grpc_server_t * server,
                    ulong              msg_sz,
                    ulong              stream_mask ) {
  (void)server;
  FD_TEST( test_out_cnt<TEST_OUT_MAX );
  FD_TEST( msg_sz<=sizeof(test_stage) );
  test_out_t * out = &test_out[ test_out_cnt++ ];
  memcpy( out->buf, test_stage, msg_sz );
  out->sz   = msg_sz;
  out->mask = stream_mask;
}

struct test_member {
  uchar tag;
  int   landed;
  int   txn_err;
  int   exec_err;
  uint  exec_err_idx;
  uint  custom_err;
};

typedef struct test_member test_member_t;

struct test_update {
  uint  kind;
  ulong gap_dropped;
  ulong slot;
  int   landed;
  ulong sig_cnt;
  uchar sigs[ MAX_TXN_PER_MICROBLOCK ][ 64 ];
  ulong err_cnt;
  int   txn_err     [ MAX_TXN_PER_MICROBLOCK ];
  int   exec_err    [ MAX_TXN_PER_MICROBLOCK ];
  uint  exec_err_idx[ MAX_TXN_PER_MICROBLOCK ];
  uint  custom_err  [ MAX_TXN_PER_MICROBLOCK ];
};

typedef struct test_update test_update_t;

static fd_grpc_tile_t *
test_ctx_new( void ** scratch_out ) {
  fd_topo_tile_t * tile = aligned_alloc( alignof(fd_topo_tile_t), sizeof(fd_topo_tile_t) );
  FD_TEST( tile );
  memset( tile, 0, sizeof(fd_topo_tile_t) );
  tile->grpc.max_subscriptions   = 4UL;
  tile->grpc.tx_depth            = 64UL;
  tile->grpc.send_buffer_size_mb = 1UL;

  ulong  fp      = fd_ulong_align_up( scratch_footprint( tile ), scratch_align() );
  void * scratch = aligned_alloc( scratch_align(), fp );
  FD_TEST( scratch );
  memset( scratch, 0, fp );

  fd_grpc_tile_t * ctx = tile_ctx_init( scratch, tile );
  free( tile );

  ctx->in[ 0 ].kind = IN_KIND_EXECLE;
  ctx->in[ 1 ].kind = IN_KIND_EXECLE;
  ctx->in[ 2 ].kind = IN_KIND_REPLAY;

  test_out_cnt = 0UL;
  *scratch_out = scratch;
  return ctx;
}

static void
test_subscribe( fd_grpc_tile_t * ctx,
                ulong            stream_idx,
                int              include_bundles ) {
  uchar const req[] = { 0x20, 0x01 };
  cb_request_msg( ctx, stream_idx, req, include_bundles ? sizeof(req) : 0UL );
  FD_TEST( ctx->sub_mask & (1UL<<stream_idx) );
}

static void
test_make_txn( fd_txn_p_t *          txnp,
               test_member_t const * m ) {
  memset( txnp, 0, sizeof(fd_txn_p_t) );
  fd_txn_t * txn = TXN( txnp );
  txn->signature_cnt = 1;
  txn->signature_off = 1;
  txnp->payload[ 0 ] = 1;
  for( ulong j=0UL; j<64UL; j++ ) txnp->payload[ 1UL+j ] = (uchar)( m->tag + j );
  txnp->payload_sz = (ushort)65;
  txnp->flags = ( (uint)(-m->txn_err)<<24 ) | fd_uint_if( m->landed, FD_TXN_P_FLAGS_EXECUTE_SUCCESS, 0U );
}

static void
test_feed( fd_grpc_tile_t *                ctx,
           ulong                           in_idx,
           ulong                           seq,
           fd_txn_p_t const *              txns,
           ulong                           txn_cnt,
           fd_microblock_trailer_t const * trailer ) {
  ulong sig = fd_disco_execle_sig( TEST_SLOT, 0UL );
  ulong sz  = txn_cnt*sizeof(fd_txn_p_t) + sizeof(fd_microblock_trailer_t);
  if( before_frag( ctx, in_idx, seq, sig ) ) return;
  memcpy( ctx->buf->txns, txns, txn_cnt*sizeof(fd_txn_p_t) );
  memcpy( ctx->buf->raw + txn_cnt*sizeof(fd_txn_p_t), trailer, sizeof(fd_microblock_trailer_t) );
  after_frag( ctx, in_idx, seq, sig, sz, 0UL, 0UL, NULL );
}

static ulong
test_feed_bundle( fd_grpc_tile_t *      ctx,
                  ulong                 in_idx,
                  ulong                 seq,
                  ulong                 pack_txn_idx0,
                  test_member_t const * members,
                  ulong                 cnt,
                  ulong                 lo,
                  ulong                 hi ) {
  for( ulong i=lo; i<hi; i++ ) {
    fd_txn_p_t txn[ 1 ];
    test_make_txn( txn, &members[ i ] );
    fd_microblock_trailer_t trailer = {
      .pack_txn_idx = pack_txn_idx0+i,
      .bundle_idx   = (uchar)i,
      .bundle_cnt   = (uchar)cnt,
      .exec_err     = (schar)members[ i ].exec_err,
      .exec_err_idx = (uchar)members[ i ].exec_err_idx,
      .custom_err   = members[ i ].custom_err
    };
    test_feed( ctx, in_idx, seq++, txn, 1UL, &trailer );
  }
  return seq;
}

static void
test_decode_txn_error( fd_pb_inbuf_t * in,
                       test_update_t * u ) {
  FD_TEST( u->err_cnt<MAX_TXN_PER_MICROBLOCK );
  ulong i = u->err_cnt++;
  fd_pb_tlv_t tlv[ 1 ];
  while( fd_pb_inbuf_sz( in ) ) {
    FD_TEST( fd_pb_read_tlv( in, tlv ) );
    FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_VARINT );
    switch( tlv->field_id ) {
    case 1U: u->txn_err     [ i ] = (int)(uint)tlv->varint; break;
    case 2U: u->exec_err    [ i ] = (int)(uint)tlv->varint; break;
    case 3U: u->exec_err_idx[ i ] = (uint)tlv->varint;      break;
    case 4U: u->custom_err  [ i ] = (uint)tlv->varint;      break;
    default: FD_LOG_ERR(( "unexpected TxnError field %u", tlv->field_id ));
    }
  }
}

static void
test_decode_bundle( fd_pb_inbuf_t * in,
                    test_update_t * u ) {
  fd_pb_tlv_t tlv[ 1 ];
  while( fd_pb_inbuf_sz( in ) ) {
    FD_TEST( fd_pb_read_tlv( in, tlv ) );
    switch( tlv->field_id ) {
    case 1U:
      FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_VARINT );
      u->slot = tlv->varint;
      break;
    case 2U:
      FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_VARINT );
      u->landed = !!tlv->varint;
      break;
    case 3U:
      FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_LEN && tlv->len==64UL );
      FD_TEST( fd_pb_inbuf_sz( in )>=64UL );
      FD_TEST( u->sig_cnt<MAX_TXN_PER_MICROBLOCK );
      memcpy( u->sigs[ u->sig_cnt++ ], in->cur, 64UL );
      fd_pb_inbuf_skip( in, 64UL );
      break;
    case 4U: {
      FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_LEN );
      fd_pb_inbuf_t sub[ 1 ];
      fd_pb_inbuf_init( sub, in->cur, tlv->len );
      fd_pb_inbuf_skip( in, tlv->len );
      test_decode_txn_error( sub, u );
      break;
    }
    default:
      FD_LOG_ERR(( "unexpected BundleOutcome field %u", tlv->field_id ));
    }
  }
}

static void
test_decode_gap( fd_pb_inbuf_t * in,
                 test_update_t * u ) {
  fd_pb_tlv_t tlv[ 1 ];
  while( fd_pb_inbuf_sz( in ) ) {
    FD_TEST( fd_pb_read_tlv( in, tlv ) );
    FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_VARINT );
    if     ( tlv->field_id==1U ) u->gap_dropped = tlv->varint;
    else if( tlv->field_id==2U ) u->slot        = tlv->varint;
    else FD_LOG_ERR(( "unexpected Gap field %u", tlv->field_id ));
  }
}

static void
test_decode( ulong           msg_idx,
             test_update_t * u ) {
  FD_TEST( msg_idx<test_out_cnt );
  memset( u, 0, sizeof(test_update_t) );

  fd_pb_inbuf_t in[ 1 ];
  fd_pb_inbuf_init( in, test_out[ msg_idx ].buf, test_out[ msg_idx ].sz );
  fd_pb_tlv_t tlv[ 1 ];
  FD_TEST( fd_pb_read_tlv( in, tlv ) );
  FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_LEN );
  FD_TEST( tlv->len==fd_pb_inbuf_sz( in ) );
  u->kind = tlv->field_id;

  switch( u->kind ) {
  case UPDATE_FIELD_BUNDLE: test_decode_bundle( in, u ); break;
  case UPDATE_FIELD_GAP:    test_decode_gap   ( in, u ); break;
  default: FD_LOG_ERR(( "unexpected Update field %u", u->kind ));
  }
}

static void
test_check_bundle( ulong                 msg_idx,
                   ulong                 mask,
                   int                   landed,
                   test_member_t const * members,
                   ulong                 cnt ) {
  FD_TEST( test_out[ msg_idx ].mask==mask );
  test_update_t u[ 1 ];
  test_decode( msg_idx, u );
  FD_TEST( u->kind==UPDATE_FIELD_BUNDLE );
  FD_TEST( u->slot==TEST_SLOT );
  FD_TEST( u->landed==landed );
  FD_TEST( u->sig_cnt==cnt );
  FD_TEST( u->err_cnt==cnt );
  for( ulong i=0UL; i<cnt; i++ ) {
    for( ulong j=0UL; j<64UL; j++ ) FD_TEST( u->sigs[ i ][ j ]==(uchar)( members[ i ].tag + j ) );
    FD_TEST( u->txn_err     [ i ]==members[ i ].txn_err      );
    FD_TEST( u->exec_err    [ i ]==members[ i ].exec_err     );
    FD_TEST( u->exec_err_idx[ i ]==members[ i ].exec_err_idx );
    FD_TEST( u->custom_err  [ i ]==members[ i ].custom_err   );
  }
}

static void
test_check_gap( ulong msg_idx,
                ulong dropped ) {
  test_update_t u[ 1 ];
  test_decode( msg_idx, u );
  FD_TEST( u->kind==UPDATE_FIELD_GAP );
  FD_TEST( u->gap_dropped==dropped );
}

static test_member_t const landed3[ 3 ] = {
  { .tag=0x10, .landed=1 },
  { .tag=0x20, .landed=1 },
  { .tag=0x30, .landed=1 }
};

static test_member_t const other3[ 3 ] = {
  { .tag=0x40, .landed=1 },
  { .tag=0x50, .landed=1 },
  { .tag=0x60, .landed=1 }
};

static test_member_t const next3[ 3 ] = {
  { .tag=0x70, .landed=1 },
  { .tag=0x80, .landed=1 },
  { .tag=0x90, .landed=1 }
};

FD_UNIT_TEST( grpc_bundle_landed ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe( ctx, 0UL, 1 );
  test_subscribe( ctx, 1UL, 0 );
  FD_TEST( ctx->bundle_mask==1UL );

  test_feed_bundle( ctx, 0UL, 0UL, 40UL, landed3, 3UL, 0UL, 2UL );
  FD_TEST( test_out_cnt==0UL );
  test_feed_bundle( ctx, 0UL, 2UL, 40UL, landed3, 3UL, 2UL, 3UL );
  FD_TEST( test_out_cnt==1UL );
  test_check_bundle( 0UL, 1UL, 1, landed3, 3UL );
  free( scratch );
}

FD_UNIT_TEST( grpc_bundle_failed ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe( ctx, 2UL, 1 );

  test_member_t const failed[ 3 ] = {
    { .tag=0x11, .txn_err=FD_RUNTIME_TXN_ERR_BUNDLE_PEER },
    { .tag=0x22, .txn_err=FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR,
      .exec_err=FD_EXECUTOR_INSTR_ERR_CUSTOM_ERR, .exec_err_idx=3U, .custom_err=6001U },
    { .tag=0x33, .txn_err=FD_RUNTIME_TXN_ERR_BUNDLE_PEER }
  };
  test_feed_bundle( ctx, 0UL, 0UL, 7UL, failed, 3UL, 0UL, 3UL );
  FD_TEST( test_out_cnt==1UL );
  test_check_bundle( 0UL, 1UL<<2, 0, failed, 3UL );
  free( scratch );
}

FD_UNIT_TEST( grpc_bundle_single ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe( ctx, 0UL, 1 );

  test_feed_bundle( ctx, 0UL, 0UL, 5UL, landed3, 1UL, 0UL, 1UL );
  FD_TEST( test_out_cnt==1UL );
  test_check_bundle( 0UL, 1UL, 1, landed3, 1UL );
  free( scratch );
}

FD_UNIT_TEST( grpc_bundle_ignores_microblocks ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe( ctx, 0UL, 1 );

  fd_txn_p_t txns[ 3 ];
  for( ulong i=0UL; i<3UL; i++ ) test_make_txn( &txns[ i ], &landed3[ i ] );
  fd_microblock_trailer_t trailer = { .pack_txn_idx=1UL };
  test_feed( ctx, 0UL, 0UL, txns, 1UL, &trailer );
  test_feed( ctx, 0UL, 1UL, txns, 3UL, &trailer );
  FD_TEST( test_out_cnt==0UL );

  test_feed_bundle( ctx, 0UL, 2UL, 4UL, landed3, 3UL, 0UL, 3UL );
  FD_TEST( test_out_cnt==1UL );
  test_check_bundle( 0UL, 1UL, 1, landed3, 3UL );
  free( scratch );
}

FD_UNIT_TEST( grpc_bundle_requires_subscriber ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  FD_TEST( before_frag( ctx, 0UL, 0UL, fd_disco_execle_sig( TEST_SLOT, 0UL ) )==1 );

  test_subscribe( ctx, 0UL, 0 );
  test_feed_bundle( ctx, 0UL, 1UL, 4UL, landed3, 3UL, 0UL, 3UL );
  FD_TEST( test_out_cnt==0UL );

  test_subscribe( ctx, 1UL, 1 );
  FD_TEST( before_frag( ctx, 0UL, 4UL, fd_disco_execle_sig( TEST_SLOT, 0UL ) )==0 );
  FD_TEST( before_frag( ctx, 2UL, 0UL, REPLAY_SIG_TXN_EXECUTED )==1 );
  free( scratch );
}

FD_UNIT_TEST( grpc_bundle_overrun_no_splice ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe( ctx, 0UL, 1 );

  test_feed_bundle( ctx, 0UL, 0UL, 10UL, landed3, 3UL, 0UL, 2UL );
  test_feed_bundle( ctx, 0UL, 5UL, 20UL, other3,  3UL, 2UL, 3UL );
  FD_TEST( test_out_cnt==0UL );

  test_feed_bundle( ctx, 0UL, 6UL, 30UL, next3, 3UL, 0UL, 3UL );
  FD_TEST( test_out_cnt==2UL );
  test_check_gap( 0UL, 3UL );
  test_check_bundle( 1UL, 1UL, 1, next3, 3UL );
  free( scratch );
}

FD_UNIT_TEST( grpc_bundle_resubscribe_no_splice ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe( ctx, 0UL, 1 );

  ulong seq = test_feed_bundle( ctx, 0UL, 0UL, 10UL, landed3, 3UL, 0UL, 2UL );
  cb_stream_close( ctx, 0UL, 0 );
  FD_TEST( !ctx->bundle_mask );
  seq = test_feed_bundle( ctx, 0UL, seq, 10UL, landed3, 3UL, 2UL, 3UL );
  seq = test_feed_bundle( ctx, 0UL, seq, 20UL, other3,  3UL, 0UL, 2UL );

  test_subscribe( ctx, 1UL, 1 );
  seq = test_feed_bundle( ctx, 0UL, seq, 20UL, other3, 3UL, 2UL, 3UL );
  FD_TEST( test_out_cnt==0UL );

  test_feed_bundle( ctx, 0UL, seq, 30UL, next3, 3UL, 0UL, 3UL );
  FD_TEST( test_out_cnt==1UL );
  test_check_bundle( 0UL, 1UL<<1, 1, next3, 3UL );
  free( scratch );
}

FD_UNIT_TEST( grpc_bundle_per_link ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe( ctx, 0UL, 1 );

  test_feed_bundle( ctx, 0UL, 0UL, 10UL, landed3, 2UL, 0UL, 1UL );
  test_feed_bundle( ctx, 1UL, 0UL, 20UL, other3,  2UL, 0UL, 1UL );
  test_feed_bundle( ctx, 0UL, 1UL, 10UL, landed3, 2UL, 1UL, 2UL );
  test_feed_bundle( ctx, 1UL, 1UL, 20UL, other3,  2UL, 1UL, 2UL );
  FD_TEST( test_out_cnt==2UL );
  test_check_bundle( 0UL, 1UL, 1, landed3, 2UL );
  test_check_bundle( 1UL, 1UL, 1, other3,  2UL );
  free( scratch );
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_unit_tests( argc, argv );
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}