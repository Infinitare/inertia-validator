#define fd_grpc_server_msg_stage     test_msg_stage
#define fd_grpc_server_msg_broadcast test_msg_broadcast
#define fd_grpc_server_stream_close  test_stream_close
#include "fd_grpc_tile.c"

#include "../../ballet/pb/fd_pb_tokenize.h"
#include "../../flamenco/runtime/fd_executor_err.h"
#include "../../flamenco/runtime/fd_runtime_err.h"
#include "../../util/tmpl/fd_unit_test.c"

#include <stdlib.h>

#define TEST_OUT_MAX (8UL)
#define TEST_SLOT    (300000000UL)
#define TEST_LOADED  (8UL)

struct test_out {
  uchar buf[ MSG_MAX ];
  ulong sz;
  ulong mask;
};

typedef struct test_out test_out_t;

static uchar      test_stage[ MSG_MAX ];
static test_out_t test_out[ TEST_OUT_MAX ];
static ulong      test_out_cnt;
static ulong      test_close_cnt;

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

void
test_stream_close( fd_grpc_server_t * server,
                   ulong              stream_idx,
                   uint               grpc_status,
                   char const *       grpc_msg,
                   ulong              grpc_msg_len ) {
  (void)server; (void)stream_idx; (void)grpc_status; (void)grpc_msg; (void)grpc_msg_len;
  test_close_cnt++;
}

struct test_txn {
  uint  kind;
  ulong slot;
  uchar sig[ 64 ];
  int   success;
  ulong index;
  uchar payload[ FD_TPU_MTU ];
  ulong payload_sz;
  ulong wr_cnt;
  uchar wr[ TEST_LOADED ][ 32 ];
  ulong ro_cnt;
  uchar ro[ TEST_LOADED ][ 32 ];
  int   has_err;
  int   txn_err;
  int   exec_err;
  uint  exec_err_idx;
  uint  custom_err;
  ulong gap_dropped;
};

typedef struct test_txn test_txn_t;

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
  ctx->in[ 1 ].kind = IN_KIND_PACK;
  ctx->in[ 2 ].kind = IN_KIND_REPLAY;

  test_out_cnt   = 0UL;
  test_close_cnt = 0UL;
  *scratch_out = scratch;
  return ctx;
}

static void
test_key( uchar * key,
          uchar   tag ) {
  for( ulong j=0UL; j<32UL; j++ ) key[ j ] = (uchar)( tag + j );
}

static void
test_subscribe_wallet( fd_grpc_tile_t * ctx,
                       ulong            stream_idx,
                       uchar            acct_tag ) {
  uchar req[ 34 ] = { 0x0a, 32 };
  test_key( req+2, acct_tag );
  FD_TEST( cb_request_start( ctx, stream_idx, SUBSCRIBE_PATH, sizeof(SUBSCRIBE_PATH)-1UL ) );
  cb_request_msg( ctx, stream_idx, req, sizeof(req) );
  FD_TEST( ctx->sub_mask & (1UL<<stream_idx) );
}

static void
test_subscribe_leader( fd_grpc_tile_t * ctx,
                       ulong            stream_idx ) {
  FD_TEST( cb_request_start( ctx, stream_idx, SUBSCRIBE_LEADER_PATH, sizeof(SUBSCRIBE_LEADER_PATH)-1UL ) );
  cb_request_msg( ctx, stream_idx, NULL, 0UL );
  FD_TEST( ctx->leader_mask & (1UL<<stream_idx) );
}

static void
test_make_txn( fd_txn_p_t * txnp,
               uchar        sig_tag,
               uchar        acct_tag,
               ulong        adtl_cnt,
               ulong        adtl_wr_cnt,
               int          landed,
               int          txn_err ) {
  memset( txnp, 0, sizeof(fd_txn_p_t) );
  fd_txn_t * txn = TXN( txnp );
  txn->signature_cnt                = 1;
  txn->signature_off                = 1;
  txn->acct_addr_cnt                = 2;
  txn->acct_addr_off                = 65;
  txn->addr_table_adtl_cnt          = (uchar)adtl_cnt;
  txn->addr_table_adtl_writable_cnt = (uchar)adtl_wr_cnt;
  txnp->payload[ 0 ] = 1;
  for( ulong j=0UL; j<64UL; j++ ) txnp->payload[ 1UL+j ] = (uchar)( sig_tag + j );
  test_key( txnp->payload+65UL, acct_tag         );
  test_key( txnp->payload+97UL, (uchar)0xEE      );
  txnp->payload_sz = (ushort)129;
  txnp->flags = ( (uint)(-txn_err)<<24 ) | fd_uint_if( landed, FD_TXN_P_FLAGS_EXECUTE_SUCCESS, 0U );
}

static void
test_feed_execle( fd_grpc_tile_t *                ctx,
                  ulong                           seq,
                  fd_txn_p_t const *              txns,
                  ulong                           txn_cnt,
                  fd_microblock_trailer_t const * trailer ) {
  ulong sig = fd_disco_execle_sig( TEST_SLOT, 0UL );
  ulong sz  = txn_cnt*sizeof(fd_txn_p_t) + sizeof(fd_microblock_trailer_t);
  if( before_frag( ctx, 0UL, seq, sig ) ) return;
  memcpy( ctx->buf->txns, txns, txn_cnt*sizeof(fd_txn_p_t) );
  memcpy( ctx->buf->raw + txn_cnt*sizeof(fd_txn_p_t), trailer, sizeof(fd_microblock_trailer_t) );
  after_frag( ctx, 0UL, seq, sig, sz, 0UL, 0UL, NULL );
}

static uchar test_pack_buf[ FD_PACK_EXECLE_MTU ] __attribute__((aligned(64)));

static void
test_feed_pack( fd_grpc_tile_t *   ctx,
                ulong              seq,
                fd_txn_p_t const * txns,
                ulong              txn_cnt,
                ulong              pack_txn_idx,
                int                commit ) {
  memset( test_pack_buf, 0, sizeof(test_pack_buf) );
  fd_txn_e_t * txne = (fd_txn_e_t *)test_pack_buf;
  for( ulong i=0UL; i<txn_cnt; i++ ) {
    txne[ i ].txnp[ 0 ] = txns[ i ];
    ulong cnt = TXN( &txns[ i ] )->addr_table_adtl_cnt;
    for( ulong j=0UL; j<cnt; j++ ) test_key( txne[ i ].alt_accts[ j ].b, (uchar)( 0x80 + 0x10*i + j ) );
  }
  ulong sz = txn_cnt*sizeof(fd_txn_e_t) + sizeof(fd_microblock_execle_trailer_t);
  fd_microblock_execle_trailer_t * trailer = (fd_microblock_execle_trailer_t *)( test_pack_buf + txn_cnt*sizeof(fd_txn_e_t) );
  trailer->pack_txn_idx = pack_txn_idx;

  ulong sig = fd_disco_poh_sig( TEST_SLOT, POH_PKT_TYPE_MICROBLOCK, 0UL );
  if( before_frag( ctx, 1UL, seq, sig ) ) return;
  alt_stage( ctx, test_pack_buf, sz );
  if( commit ) after_frag( ctx, 1UL, seq, sig, sz, 0UL, 0UL, NULL );
}

static void
test_feed_replay( fd_grpc_tile_t *   ctx,
                  ulong              seq,
                  fd_txn_p_t const * txnp,
                  ulong              index_in_slot,
                  int                txn_err ) {
  if( before_frag( ctx, 2UL, seq, REPLAY_SIG_TXN_EXECUTED ) ) return;
  fd_replay_txn_executed_t * exec = &ctx->buf->replay.txn_executed;
  memset( exec, 0, sizeof(fd_replay_txn_executed_t) );
  exec->txn[ 0 ]      = *txnp;
  exec->slot          = TEST_SLOT+1UL;
  exec->index_in_slot = index_in_slot;
  exec->txn_err       = txn_err;
  after_frag( ctx, 2UL, seq, REPLAY_SIG_TXN_EXECUTED, sizeof(fd_replay_txn_executed_t), 0UL, 0UL, NULL );
}

static void
test_decode_err( fd_pb_inbuf_t * in,
                 test_txn_t *    t ) {
  t->has_err = 1;
  fd_pb_tlv_t tlv[ 1 ];
  while( fd_pb_inbuf_sz( in ) ) {
    FD_TEST( fd_pb_read_tlv( in, tlv ) );
    FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_VARINT );
    switch( tlv->field_id ) {
    case 1U: t->txn_err      = (int)(uint)tlv->varint; break;
    case 2U: t->exec_err     = (int)(uint)tlv->varint; break;
    case 3U: t->exec_err_idx = (uint)tlv->varint;      break;
    case 4U: t->custom_err   = (uint)tlv->varint;      break;
    default: FD_LOG_ERR(( "unexpected TxnError field %u", tlv->field_id ));
    }
  }
}

static void
test_decode_bytes( fd_pb_inbuf_t *     in,
                   fd_pb_tlv_t const * tlv,
                   uchar *             out,
                   ulong               max ) {
  FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_LEN );
  FD_TEST( tlv->len<=max && fd_pb_inbuf_sz( in )>=tlv->len );
  memcpy( out, in->cur, tlv->len );
  fd_pb_inbuf_skip( in, tlv->len );
}

static void
test_decode( ulong        msg_idx,
             test_txn_t * t ) {
  FD_TEST( msg_idx<test_out_cnt );
  memset( t, 0, sizeof(test_txn_t) );

  fd_pb_inbuf_t in[ 1 ];
  fd_pb_inbuf_init( in, test_out[ msg_idx ].buf, test_out[ msg_idx ].sz );
  fd_pb_tlv_t tlv[ 1 ];
  FD_TEST( fd_pb_read_tlv( in, tlv ) );
  FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_LEN );
  FD_TEST( tlv->len==fd_pb_inbuf_sz( in ) );
  t->kind = tlv->field_id;

  while( fd_pb_inbuf_sz( in ) ) {
    FD_TEST( fd_pb_read_tlv( in, tlv ) );
    if( t->kind==UPDATE_FIELD_GAP ) {
      FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_VARINT );
      if     ( tlv->field_id==1U ) t->gap_dropped = tlv->varint;
      else if( tlv->field_id==2U ) t->slot        = tlv->varint;
      else FD_LOG_ERR(( "unexpected Gap field %u", tlv->field_id ));
      continue;
    }
    FD_TEST( t->kind==UPDATE_FIELD_TXN );
    switch( tlv->field_id ) {
    case 1U: t->slot    = tlv->varint;    break;
    case 2U: FD_TEST( tlv->len==64UL ); test_decode_bytes( in, tlv, t->sig, 64UL ); break;
    case 3U: t->success = !!tlv->varint;  break;
    case 4U: t->index   = tlv->varint;    break;
    case 5U: t->payload_sz = tlv->len; test_decode_bytes( in, tlv, t->payload, FD_TPU_MTU ); break;
    case 6U: FD_TEST( tlv->len==32UL && t->wr_cnt<TEST_LOADED ); test_decode_bytes( in, tlv, t->wr[ t->wr_cnt++ ], 32UL ); break;
    case 7U: FD_TEST( tlv->len==32UL && t->ro_cnt<TEST_LOADED ); test_decode_bytes( in, tlv, t->ro[ t->ro_cnt++ ], 32UL ); break;
    case 8U: {
      FD_TEST( tlv->wire_type==FD_PB_WIRE_TYPE_LEN );
      fd_pb_inbuf_t sub[ 1 ];
      fd_pb_inbuf_init( sub, in->cur, tlv->len );
      fd_pb_inbuf_skip( in, tlv->len );
      test_decode_err( sub, t );
      break;
    }
    default: FD_LOG_ERR(( "unexpected TxnUpdate field %u", tlv->field_id ));
    }
  }
}

static void
test_check_txn( ulong              msg_idx,
                ulong              mask,
                ulong              slot,
                ulong              index,
                fd_txn_p_t const * txnp,
                int                txn_err,
                test_txn_t *       t ) {
  FD_TEST( test_out[ msg_idx ].mask==mask );
  test_decode( msg_idx, t );
  FD_TEST( t->kind==UPDATE_FIELD_TXN );
  FD_TEST( t->slot==slot );
  FD_TEST( t->index==index );
  FD_TEST( t->success==!txn_err );
  FD_TEST( !memcmp( t->sig, txnp->payload+1UL, 64UL ) );
  FD_TEST( t->payload_sz==txnp->payload_sz );
  FD_TEST( !memcmp( t->payload, txnp->payload, txnp->payload_sz ) );
  FD_TEST( t->has_err==!!txn_err );
  if( txn_err ) FD_TEST( t->txn_err==txn_err );
}

FD_UNIT_TEST( grpc_txn_wallet_and_leader ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe_wallet( ctx, 0UL, 0x10 );
  test_subscribe_leader( ctx, 1UL );

  fd_txn_p_t txns[ 3 ];
  test_make_txn( &txns[ 0 ], 0x01, 0x10, 0UL, 0UL, 1, 0 );
  test_make_txn( &txns[ 1 ], 0x02, 0x20, 0UL, 0UL, 1, FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  test_make_txn( &txns[ 2 ], 0x03, 0x10, 0UL, 0UL, 0, FD_RUNTIME_TXN_ERR_INSUFFICIENT_FUNDS_FOR_FEE );
  fd_microblock_trailer_t trailer = {
    .pack_txn_idx = 70UL,
    .exec_err     = { 0, FD_EXECUTOR_INSTR_ERR_CUSTOM_ERR, 0 },
    .exec_err_idx = { 0, 2U, 0 },
    .custom_err   = { 0U, 6001U, 0U }
  };
  test_feed_execle( ctx, 0UL, txns, 3UL, &trailer );
  FD_TEST( test_out_cnt==2UL );

  test_txn_t t[ 1 ];
  test_check_txn( 0UL, 3UL, TEST_SLOT, 70UL, &txns[ 0 ], 0, t );
  FD_TEST( !t->wr_cnt && !t->ro_cnt );

  test_check_txn( 1UL, 2UL, TEST_SLOT, 71UL, &txns[ 1 ], FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR, t );
  FD_TEST( t->exec_err==FD_EXECUTOR_INSTR_ERR_CUSTOM_ERR );
  FD_TEST( t->exec_err_idx==2U );
  FD_TEST( t->custom_err==6001U );
  free( scratch );
}

FD_UNIT_TEST( grpc_txn_wallet_failed ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe_wallet( ctx, 2UL, 0x10 );

  fd_txn_p_t txns[ 1 ];
  test_make_txn( &txns[ 0 ], 0x04, 0x10, 0UL, 0UL, 1, FD_RUNTIME_TXN_ERR_ACCOUNT_NOT_FOUND );
  fd_microblock_trailer_t trailer = { .pack_txn_idx = 9UL };
  test_feed_execle( ctx, 0UL, txns, 1UL, &trailer );
  FD_TEST( test_out_cnt==1UL );

  test_txn_t t[ 1 ];
  test_check_txn( 0UL, 1UL<<2, TEST_SLOT, 9UL, &txns[ 0 ], FD_RUNTIME_TXN_ERR_ACCOUNT_NOT_FOUND, t );
  FD_TEST( t->exec_err==0 && t->exec_err_idx==0U && t->custom_err==0U );
  free( scratch );
}

FD_UNIT_TEST( grpc_txn_replay_wallet_only ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe_leader( ctx, 1UL );
  FD_TEST( before_frag( ctx, 2UL, 0UL, REPLAY_SIG_TXN_EXECUTED )==1 );

  test_subscribe_wallet( ctx, 0UL, 0x10 );
  FD_TEST( before_frag( ctx, 2UL, 1UL, REPLAY_SIG_TXN_EXECUTED )==0 );
  FD_TEST( before_frag( ctx, 2UL, 2UL, REPLAY_SIG_SLOT_COMPLETED )==1 );

  fd_txn_p_t txn[ 1 ];
  test_make_txn( txn, 0x05, 0x10, 3UL, 1UL, 1, 0 );
  test_feed_replay( ctx, 3UL, txn, 42UL, 0 );
  test_make_txn( txn, 0x06, 0x10, 0UL, 0UL, 1, FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  test_feed_replay( ctx, 4UL, txn, 43UL, FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  FD_TEST( test_out_cnt==2UL );

  test_txn_t t[ 1 ];
  test_make_txn( txn, 0x05, 0x10, 3UL, 1UL, 1, 0 );
  test_check_txn( 0UL, 1UL, TEST_SLOT+1UL, 42UL, txn, 0, t );
  FD_TEST( !t->wr_cnt && !t->ro_cnt );

  test_make_txn( txn, 0x06, 0x10, 0UL, 0UL, 1, FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR );
  test_check_txn( 1UL, 1UL, TEST_SLOT+1UL, 43UL, txn, FD_RUNTIME_TXN_ERR_INSTRUCTION_ERROR, t );
  FD_TEST( t->exec_err==0 );
  FD_TEST( !ctx->alt_miss );
  free( scratch );
}

FD_UNIT_TEST( grpc_txn_alt_loaded ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe_leader( ctx, 0UL );

  fd_txn_p_t txns[ 2 ];
  test_make_txn( &txns[ 0 ], 0x07, 0x30, 0UL, 0UL, 1, 0 );
  test_make_txn( &txns[ 1 ], 0x08, 0x31, 3UL, 2UL, 1, 0 );
  test_feed_pack( ctx, 0UL, txns, 2UL, 100UL, 1 );
  FD_TEST( test_out_cnt==0UL );

  fd_microblock_trailer_t trailer = { .pack_txn_idx = 100UL };
  test_feed_execle( ctx, 0UL, txns, 2UL, &trailer );
  FD_TEST( test_out_cnt==2UL );

  test_txn_t t[ 1 ];
  test_check_txn( 0UL, 1UL, TEST_SLOT, 100UL, &txns[ 0 ], 0, t );
  FD_TEST( !t->wr_cnt && !t->ro_cnt );

  test_check_txn( 1UL, 1UL, TEST_SLOT, 101UL, &txns[ 1 ], 0, t );
  FD_TEST( t->wr_cnt==2UL && t->ro_cnt==1UL );
  uchar key[ 32 ];
  test_key( key, 0x90 ); FD_TEST( !memcmp( t->wr[ 0 ], key, 32UL ) );
  test_key( key, 0x91 ); FD_TEST( !memcmp( t->wr[ 1 ], key, 32UL ) );
  test_key( key, 0x92 ); FD_TEST( !memcmp( t->ro[ 0 ], key, 32UL ) );
  FD_TEST( !ctx->alt_miss );
  free( scratch );
}

FD_UNIT_TEST( grpc_txn_alt_miss ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe_leader( ctx, 0UL );

  fd_txn_p_t txns[ 1 ];
  test_make_txn( &txns[ 0 ], 0x09, 0x30, 2UL, 1UL, 1, 0 );
  fd_microblock_trailer_t trailer = { .pack_txn_idx = 200UL };
  test_feed_execle( ctx, 0UL, txns, 1UL, &trailer );

  test_feed_pack( ctx, 0UL, txns, 1UL, 201UL, 0 );
  trailer.pack_txn_idx = 201UL;
  test_feed_execle( ctx, 1UL, txns, 1UL, &trailer );

  test_feed_pack( ctx, 1UL, txns, 1UL, 202UL+ALT_CACHE_CNT, 1 );
  trailer.pack_txn_idx = 202UL;
  test_feed_execle( ctx, 2UL, txns, 1UL, &trailer );

  FD_TEST( test_out_cnt==3UL );
  for( ulong i=0UL; i<3UL; i++ ) {
    test_txn_t t[ 1 ];
    test_check_txn( i, 1UL, TEST_SLOT, 200UL+i, &txns[ 0 ], 0, t );
    FD_TEST( !t->wr_cnt && !t->ro_cnt );
  }
  FD_TEST( ctx->alt_miss==3UL );
  free( scratch );
}

FD_UNIT_TEST( grpc_txn_gaps_split ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );
  test_subscribe_wallet( ctx, 0UL, 0x10 );
  test_subscribe_leader( ctx, 1UL );

  fd_txn_p_t txn[ 1 ];
  test_make_txn( txn, 0x0a, 0x20, 0UL, 0UL, 1, 0 );

  FD_TEST( before_frag( ctx, 2UL, 0UL, REPLAY_SIG_SLOT_COMPLETED )==1 );
  FD_TEST( before_frag( ctx, 2UL, 5UL, REPLAY_SIG_SLOT_COMPLETED )==1 );
  FD_TEST( ctx->gap_cnt==4UL && !ctx->leader_gap_cnt );

  test_feed_pack( ctx, 0UL, txn, 1UL, 1UL, 1 );
  test_feed_pack( ctx, 3UL, txn, 1UL, 2UL, 1 );
  FD_TEST( ctx->gap_cnt==4UL && !ctx->leader_gap_cnt );
  FD_TEST( ctx->gap_total==6UL );

  fd_microblock_trailer_t trailer = { .pack_txn_idx = 2UL };
  test_feed_execle( ctx, 0UL, txn, 1UL, &trailer );
  test_feed_execle( ctx, 3UL, txn, 1UL, &trailer );
  FD_TEST( ctx->gap_cnt==6UL );
  FD_TEST( test_out_cnt==3UL );

  test_txn_t t[ 1 ];
  test_check_txn( 0UL, 2UL, TEST_SLOT, 2UL, txn, 0, t );
  test_decode( 1UL, t );
  FD_TEST( t->kind==UPDATE_FIELD_GAP && t->gap_dropped==2UL && test_out[ 1 ].mask==2UL );
  test_check_txn( 2UL, 2UL, TEST_SLOT, 2UL, txn, 0, t );
  FD_TEST( !ctx->leader_gap_cnt );

  cb_stream_close( ctx, 1UL, 0 );
  FD_TEST( !ctx->leader_mask );
  cb_stream_close( ctx, 0UL, 0 );
  FD_TEST( !ctx->sub_mask && !ctx->gap_cnt );
  free( scratch );
}

FD_UNIT_TEST( grpc_txn_paths ) {
  void * scratch;
  fd_grpc_tile_t * ctx = test_ctx_new( &scratch );

  FD_TEST( !cb_request_start( ctx, 0UL, "/inertia.v1.Stream/Nope", 23UL ) );
  FD_TEST( !cb_request_start( ctx, 4UL, SUBSCRIBE_LEADER_PATH, sizeof(SUBSCRIBE_LEADER_PATH)-1UL ) );

  test_subscribe_leader( ctx, 0UL );
  FD_TEST( !ctx->sub_mask );
  cb_request_msg( ctx, 0UL, NULL, 0UL );
  FD_TEST( test_close_cnt==1UL );
  cb_request_fin( ctx, 0UL );
  FD_TEST( test_close_cnt==1UL );

  FD_TEST( cb_request_start( ctx, 1UL, SUBSCRIBE_LEADER_PATH, sizeof(SUBSCRIBE_LEADER_PATH)-1UL ) );
  cb_stream_close( ctx, 1UL, 0 );
  FD_TEST( cb_request_start( ctx, 1UL, SUBSCRIBE_PATH, sizeof(SUBSCRIBE_PATH)-1UL ) );
  cb_request_msg( ctx, 1UL, NULL, 0UL );
  FD_TEST( ctx->sub_mask==2UL && ctx->leader_mask==1UL );

  FD_TEST( cb_request_start( ctx, 2UL, SUBSCRIBE_LEADER_PATH, sizeof(SUBSCRIBE_LEADER_PATH)-1UL ) );
  cb_request_fin( ctx, 2UL );
  FD_TEST( test_close_cnt==2UL );
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