#define _GNU_SOURCE
#include "fd_grpc_server.h"
#include "../h2/fd_h2_rbuf_sock.h"
#include "../../util/fd_util.h"
#include "../../util/net/fd_ip4.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#define FD_GRPC_SERVER_MAGIC (0xf17eda2593c9b100UL)

/* Bytes of rbuf_tx headroom required before emitting a HEADERS frame.
   Derived from what fd_grpc_h2_gen_response_{hdrs,trailers} can emit so
   that adding a header or raising GRPC_MSG_MAX cannot silently overrun
   the reserve.  A literal with a literal name costs 1B marker + a 7-bit
   prefix varint per length; lengths up to 128 take 2B. */

#define FD_GRPC_SERVER_HDR_RESERVE                                       \
  (  1UL + 24UL                                     /* :status, content-type */ \
   + 1UL + 1UL + 11UL + 1UL + 2UL                   /* grpc-status  */          \
   + 1UL + 1UL + 12UL + 2UL + FD_GRPC_SERVER_GRPC_MSG_MAX ) /* grpc-message */

/* Number of pump/flush rounds conn_service does per poll.  Bounds the
   work one connection can do at the expense of its peers, while letting
   a backlogged subscriber drain more than one bufferful per visit. */

#define FD_GRPC_SERVER_DRAIN_MAX (4UL)

struct __attribute__((aligned(64))) fd_grpc_server_stream {
  fd_h2_stream_t h2[1];

  ulong conn_idx;

  uint in_use    : 1;
  uint accepted  : 1;
  uint hdrs_sent : 1;
  uint req_eos   : 1; /* peer signalled END_STREAM */
  uint req_fin   : 1; /* request_fin was delivered */
  uint closing   : 1;

  uchar   rx_hdr[5];
  ulong   rx_hdr_off;
  ulong   rx_msg_sz;
  ulong   rx_msg_off;
  uchar * rx_msg;

  uchar * hdr_buf;
  ulong   hdr_len;

  fd_grpc_req_hdrs_rx_t * req;

  ulong * tx_desc_off;
  ulong * tx_desc_len;
  ulong   tx_desc_head;
  ulong   tx_desc_tail;

  fd_h2_tx_op_t tx_op[1];

  char * close_msg;
  ulong  close_msg_len;
  uint   close_status;
  int    close_reason;
};

typedef struct fd_grpc_server_stream fd_grpc_server_stream_t;

struct fd_grpc_server_conn {
  fd_h2_conn_t h2[1];

  fd_h2_rbuf_t rbuf_rx[1];
  fd_h2_rbuf_t rbuf_tx[1];
  uchar *      rx_buf;
  uchar *      tx_buf;

  ulong stream_mask; /* stream slots owned by this conn */
  ulong active_idx;  /* index into server->conn_active */
  ulong query_memo;  /* last slot cb_stream_query hit, or ULONG_MAX */

  int   sock;
  uint  in_use     : 1;
  uint  preface_ok : 1;
  uint  tx_blocked : 1; /* sendmsg hit EAGAIN, wait for POLLOUT */
  uint  tx_pending : 1; /* a stream has queued or unfinished tx work */
  ulong preface_sz;
  uchar preface[24];
};

typedef struct fd_grpc_server_conn fd_grpc_server_conn_t;

struct fd_grpc_server {
  fd_grpc_server_params_t    params;
  fd_grpc_server_callbacks_t cb;
  void *                     ctx;

  int    listen_sock;
  ushort listen_port;
  int    tx_urgent;

  fd_grpc_server_conn_t   * conns;
  fd_grpc_server_stream_t * streams;
  ulong                     stream_used;  /* slots in use */
  ulong                     stream_valid; /* slots that exist */

  uchar * oring;
  ulong   oring_sz;
  ulong   oring_mask;
  ulong   stage_off;
  ulong   stage_len;
  ulong   oldest_min; /* lower bound on any stream's oldest queued offset */

  uchar * scratch;
  ulong   scratch_sz;

  struct pollfd * pollfds;
  ulong *         poll_conn_idx;
  ulong *         conn_free;
  ulong           conn_free_cnt;
  ulong *         conn_active;
  ulong           conn_active_cnt;

  /* stream_close callbacks deferred out of the ring allocator */
  ulong defer_mask;
  uint  defer_depth;
  schar defer_reason[ FD_GRPC_SERVER_STREAM_MAX ];

  fd_h2_hdr_matcher_t matcher[1];

  fd_grpc_server_metrics_t metrics;

  ulong magic;
};

static fd_h2_callbacks_t fd_grpc_server_h2_cb;
static int               fd_grpc_server_h2_cb_init;

static inline fd_grpc_server_conn_t *
conn_upcast( fd_h2_conn_t * h2 ) {
  return (fd_grpc_server_conn_t *)( (ulong)h2 - offsetof(fd_grpc_server_conn_t, h2) );
}

static inline fd_grpc_server_stream_t *
stream_upcast( fd_h2_stream_t * h2 ) {
  return (fd_grpc_server_stream_t *)( (ulong)h2 - offsetof(fd_grpc_server_stream_t, h2) );
}

FD_FN_CONST ulong
fd_grpc_server_align( void ) {
  return FD_GRPC_SERVER_ALIGN;
}

FD_FN_PURE ulong
fd_grpc_server_footprint( fd_grpc_server_params_t const * params ) {
  if( FD_UNLIKELY( !params                                                 ) ) return 0UL;
  if( FD_UNLIKELY( !params->max_conn_cnt                                   ) ) return 0UL;
  if( FD_UNLIKELY( !params->max_stream_cnt                                 ) ) return 0UL;
  if( FD_UNLIKELY( params->max_stream_cnt>FD_GRPC_SERVER_STREAM_MAX        ) ) return 0UL;
  if( FD_UNLIKELY( !params->max_streams_per_conn                           ) ) return 0UL;
  if( FD_UNLIKELY( params->max_streams_per_conn>params->max_stream_cnt     ) ) return 0UL;
  if( FD_UNLIKELY( !fd_ulong_is_pow2( params->tx_desc_cnt )                ) ) return 0UL;
  if( FD_UNLIKELY( !fd_ulong_is_pow2( params->oring_sz )                   ) ) return 0UL;
  if( FD_UNLIKELY( !params->max_req_msg_sz                                 ) ) return 0UL;
  if( FD_UNLIKELY( params->conn_rx_bufsz<FD_GRPC_SERVER_RX_BUFSZ_MIN       ) ) return 0UL;
  if( FD_UNLIKELY( params->conn_tx_bufsz<FD_GRPC_SERVER_TX_BUFSZ_MIN       ) ) return 0UL;

  ulong scratch_sz = params->conn_rx_bufsz;

  FD_SCRATCH_ALLOC_INIT( l, 0UL );
  FD_SCRATCH_ALLOC_APPEND( l, FD_GRPC_SERVER_ALIGN, sizeof(fd_grpc_server_t) );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_grpc_server_conn_t),   params->max_conn_cnt  *sizeof(fd_grpc_server_conn_t)   );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_grpc_server_stream_t), params->max_stream_cnt*sizeof(fd_grpc_server_stream_t) );
  FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_conn_cnt*params->conn_rx_bufsz );
  FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_conn_cnt*params->conn_tx_bufsz );
  FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_stream_cnt*params->max_req_msg_sz );
  FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_stream_cnt*FD_GRPC_SERVER_HDR_BLOCK_MAX );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_grpc_req_hdrs_rx_t), params->max_stream_cnt*sizeof(fd_grpc_req_hdrs_rx_t) );
  FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_stream_cnt*FD_GRPC_SERVER_GRPC_MSG_MAX );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(ulong), params->max_stream_cnt*params->tx_desc_cnt*sizeof(ulong) );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(ulong), params->max_stream_cnt*params->tx_desc_cnt*sizeof(ulong) );
  FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->oring_sz );
  FD_SCRATCH_ALLOC_APPEND( l, 1UL, scratch_sz );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(struct pollfd), (params->max_conn_cnt+1UL)*sizeof(struct pollfd) );
  FD_SCRATCH_ALLOC_APPEND( l, alignof(ulong), 3UL*params->max_conn_cnt*sizeof(ulong) );
  return FD_SCRATCH_ALLOC_FINI( l, FD_GRPC_SERVER_ALIGN );
}

static void
fd_grpc_server_h2_cb_bootstrap( void );

void *
fd_grpc_server_new( void *                             mem,
                    fd_grpc_server_params_t const *    params,
                    fd_grpc_server_callbacks_t const * cb,
                    void *                             ctx,
                    ulong                              seed ) {
  if( FD_UNLIKELY( !mem ) ) { FD_LOG_WARNING(( "NULL mem" )); return NULL; }
  if( FD_UNLIKELY( !fd_ulong_is_aligned( (ulong)mem, FD_GRPC_SERVER_ALIGN ) ) ) {
    FD_LOG_WARNING(( "misaligned mem" ));
    return NULL;
  }
  ulong footprint = fd_grpc_server_footprint( params );
  if( FD_UNLIKELY( !footprint ) ) {
    FD_LOG_WARNING(( "invalid params" ));
    return NULL;
  }
  if( FD_UNLIKELY( !cb || !cb->request_start || !cb->request_msg ||
                   !cb->request_fin || !cb->stream_close ) ) {
    FD_LOG_WARNING(( "invalid callbacks" ));
    return NULL;
  }

  ulong scratch_sz = params->conn_rx_bufsz;

  FD_SCRATCH_ALLOC_INIT( l, mem );
  fd_grpc_server_t * server =
    FD_SCRATCH_ALLOC_APPEND( l, FD_GRPC_SERVER_ALIGN, sizeof(fd_grpc_server_t) );
  void * conns_mem   = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_grpc_server_conn_t),   params->max_conn_cnt  *sizeof(fd_grpc_server_conn_t)   );
  void * streams_mem = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_grpc_server_stream_t), params->max_stream_cnt*sizeof(fd_grpc_server_stream_t) );
  uchar * rx_bufs    = FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_conn_cnt*params->conn_rx_bufsz );
  uchar * tx_bufs    = FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_conn_cnt*params->conn_tx_bufsz );
  uchar * rx_msgs    = FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_stream_cnt*params->max_req_msg_sz );
  uchar * hdr_bufs   = FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_stream_cnt*FD_GRPC_SERVER_HDR_BLOCK_MAX );
  void *  reqs       = FD_SCRATCH_ALLOC_APPEND( l, alignof(fd_grpc_req_hdrs_rx_t), params->max_stream_cnt*sizeof(fd_grpc_req_hdrs_rx_t) );
  char *  close_msgs = FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->max_stream_cnt*FD_GRPC_SERVER_GRPC_MSG_MAX );
  ulong * desc_offs  = FD_SCRATCH_ALLOC_APPEND( l, alignof(ulong), params->max_stream_cnt*params->tx_desc_cnt*sizeof(ulong) );
  ulong * desc_lens  = FD_SCRATCH_ALLOC_APPEND( l, alignof(ulong), params->max_stream_cnt*params->tx_desc_cnt*sizeof(ulong) );
  uchar * oring      = FD_SCRATCH_ALLOC_APPEND( l, 1UL, params->oring_sz );
  uchar * scratch    = FD_SCRATCH_ALLOC_APPEND( l, 1UL, scratch_sz );
  void *  pollfds    = FD_SCRATCH_ALLOC_APPEND( l, alignof(struct pollfd), (params->max_conn_cnt+1UL)*sizeof(struct pollfd) );
  ulong * conn_idxs  = FD_SCRATCH_ALLOC_APPEND( l, alignof(ulong), 3UL*params->max_conn_cnt*sizeof(ulong) );
  ulong scratch_top = FD_SCRATCH_ALLOC_FINI( l, FD_GRPC_SERVER_ALIGN );
  FD_TEST( scratch_top==(ulong)mem+footprint );

  memset( server, 0, sizeof(fd_grpc_server_t) );
  server->params        = *params;
  server->cb            = *cb;
  server->ctx           = ctx;
  server->listen_sock   = -1;
  server->listen_port   = (ushort)0;
  server->conns         = conns_mem;
  server->streams       = streams_mem;
  server->stream_valid  = fd_ulong_mask_lsb( (int)params->max_stream_cnt );
  server->oring         = oring;
  server->oring_sz      = params->oring_sz;
  server->oring_mask    = params->oring_sz-1UL;
  server->oldest_min    = ULONG_MAX;
  server->scratch       = scratch;
  server->scratch_sz    = scratch_sz;
  server->pollfds       = pollfds;
  server->poll_conn_idx = conn_idxs;
  server->conn_free     = conn_idxs +      params->max_conn_cnt;
  server->conn_active   = conn_idxs + 2UL*params->max_conn_cnt;

  memset( conns_mem,   0, params->max_conn_cnt  *sizeof(fd_grpc_server_conn_t)   );
  memset( streams_mem, 0, params->max_stream_cnt*sizeof(fd_grpc_server_stream_t) );

  for( ulong i=0UL; i<params->max_conn_cnt; i++ ) {
    fd_grpc_server_conn_t * conn = &server->conns[ i ];
    conn->sock   = -1;
    conn->rx_buf = rx_bufs + i*params->conn_rx_bufsz;
    conn->tx_buf = tx_bufs + i*params->conn_tx_bufsz;
    server->conn_free[ i ] = params->max_conn_cnt-1UL-i;
  }
  server->conn_free_cnt = params->max_conn_cnt;

  for( ulong i=0UL; i<params->max_stream_cnt; i++ ) {
    fd_grpc_server_stream_t * s = &server->streams[ i ];
    s->rx_msg      = rx_msgs    + i*params->max_req_msg_sz;
    s->hdr_buf     = hdr_bufs   + i*FD_GRPC_SERVER_HDR_BLOCK_MAX;
    s->req         = (fd_grpc_req_hdrs_rx_t *)reqs + i;
    s->close_msg   = close_msgs + i*FD_GRPC_SERVER_GRPC_MSG_MAX;
    s->tx_desc_off = desc_offs  + i*params->tx_desc_cnt;
    s->tx_desc_len = desc_lens  + i*params->tx_desc_cnt;
  }

  if( FD_UNLIKELY( !fd_h2_hdr_matcher_init( server->matcher, seed ) ) ) {
    FD_LOG_WARNING(( "fd_h2_hdr_matcher_init failed" ));
    return NULL;
  }

  fd_grpc_server_h2_cb_bootstrap();

  FD_COMPILER_MFENCE();
  server->magic = FD_GRPC_SERVER_MAGIC;
  FD_COMPILER_MFENCE();
  return mem;
}

fd_grpc_server_t *
fd_grpc_server_join( void * mem ) {
  if( FD_UNLIKELY( !mem ) ) { FD_LOG_WARNING(( "NULL mem" )); return NULL; }
  fd_grpc_server_t * server = mem;
  if( FD_UNLIKELY( server->magic!=FD_GRPC_SERVER_MAGIC ) ) {
    FD_LOG_WARNING(( "bad magic" ));
    return NULL;
  }
  return server;
}

void *
fd_grpc_server_leave( fd_grpc_server_t * server ) {
  return server;
}

void *
fd_grpc_server_delete( void * mem ) {
  fd_grpc_server_t * server = mem;
  if( FD_UNLIKELY( !server || server->magic!=FD_GRPC_SERVER_MAGIC ) ) return NULL;

  fd_grpc_server_shutdown( server );

  FD_COMPILER_MFENCE();
  server->magic = 0UL;
  FD_COMPILER_MFENCE();
  return mem;
}

FD_FN_CONST fd_grpc_server_metrics_t const *
fd_grpc_server_metrics( fd_grpc_server_t const * server ) {
  return &server->metrics;
}

FD_FN_PURE ushort
fd_grpc_server_listen_port( fd_grpc_server_t const * server ) {
  return server->listen_port;
}

FD_FN_PURE int
fd_grpc_server_tx_urgent( fd_grpc_server_t const * server ) {
  return server->tx_urgent;
}

FD_FN_PURE ulong
fd_grpc_server_active_mask( fd_grpc_server_t const * server ) {
  ulong mask = 0UL;
  for( ulong m=server->stream_used; m; m &= m-1UL ) {
    ulong i = (ulong)fd_ulong_find_lsb( m );
    fd_grpc_server_stream_t const * s = &server->streams[ i ];
    if( s->accepted && !s->closing ) mask |= (1UL<<i);
  }
  return mask;
}

static ulong
stream_idx( fd_grpc_server_t const * server, fd_grpc_server_stream_t const * s ) {
  return (ulong)( s - server->streams );
}

static ulong
conn_index( fd_grpc_server_t const * server, fd_grpc_server_conn_t const * conn ) {
  return (ulong)( conn - server->conns );
}

static ulong
stream_oldest_off( fd_grpc_server_t const *        server,
                   fd_grpc_server_stream_t const * s ) {
  if( s->tx_desc_tail==s->tx_desc_head ) return ULONG_MAX;
  return s->tx_desc_off[ s->tx_desc_tail & (server->params.tx_desc_cnt-1UL) ];
}

static void
stream_close_flush( fd_grpc_server_t * server ) {
  ulong mask = server->defer_mask;
  if( FD_LIKELY( !mask ) ) return;
  server->defer_mask = 0UL;
  server->defer_depth++;
  for( ; mask; mask &= mask-1UL ) {
    ulong idx = (ulong)fd_ulong_find_lsb( mask );
    server->cb.stream_close( server->ctx, idx, (int)server->defer_reason[ idx ] );
  }
  server->defer_depth--;
}

static void
stream_release( fd_grpc_server_t *        server,
                fd_grpc_server_stream_t * s,
                int                       reason ) {
  if( FD_UNLIKELY( !s->in_use ) ) return;
  ulong idx = stream_idx( server, s );
  fd_grpc_server_conn_t * conn = &server->conns[ s->conn_idx ];
  conn->stream_mask   &= ~(1UL<<idx);
  server->stream_used &= ~(1UL<<idx);

  int was_accepted = !!s->accepted;

  memset( s->tx_op, 0, sizeof(fd_h2_tx_op_t) );
  s->in_use        = 0;
  s->accepted      = 0;
  s->hdrs_sent     = 0;
  s->req_eos       = 0;
  s->req_fin       = 0;
  s->closing       = 0;
  s->rx_hdr_off    = 0UL;
  s->rx_msg_sz     = 0UL;
  s->rx_msg_off    = 0UL;
  s->hdr_len       = 0UL;
  s->tx_desc_head  = 0UL;
  s->tx_desc_tail  = 0UL;
  s->close_msg_len = 0UL;
  memset( s->h2, 0, sizeof(fd_h2_stream_t) );

  if( !was_accepted ) return;
  if( FD_UNLIKELY( server->defer_depth ) ) {
    server->defer_mask         |= 1UL<<idx;
    server->defer_reason[ idx ] = (schar)reason;
    return;
  }
  server->cb.stream_close( server->ctx, idx, reason );
}

static void
stream_error( fd_grpc_server_t *        server,
              fd_grpc_server_conn_t *   conn,
              fd_grpc_server_stream_t * s,
              uint                      h2_err,
              int                       reason ) {
  if( FD_LIKELY( fd_h2_rbuf_free_sz( conn->rbuf_tx )>=sizeof(fd_h2_rst_stream_t) ) ) {
    fd_h2_stream_error( s->h2, conn->h2, conn->rbuf_tx, h2_err );
  } else {
    fd_h2_stream_reset( s->h2, conn->h2 );
  }
  stream_release( server, s, reason );
}

static void
stream_detach( fd_grpc_server_t *        server,
               fd_grpc_server_conn_t *   conn,
               fd_grpc_server_stream_t * s,
               int                       reason ) {
  fd_h2_stream_close_tx( s->h2, conn->h2 );

  if( s->h2->state==FD_H2_STREAM_STATE_CLOSING_TX ) {
    fd_h2_stream_close_rx( s->h2, conn->h2 );
  }

  if( FD_UNLIKELY( s->h2->state!=FD_H2_STREAM_STATE_CLOSED ) ) {
    stream_error( server, conn, s, FD_H2_ERR_INTERNAL, reason );
    return;
  }
  stream_release( server, s, reason );
}

static void
stream_evict( fd_grpc_server_t *        server,
              fd_grpc_server_stream_t * s ) {
  fd_grpc_server_conn_t * conn = &server->conns[ s->conn_idx ];
  server->metrics.stream_evicted++;
  stream_error( server, conn, s, FD_H2_ERR_ENHANCE_YOUR_CALM, FD_GRPC_SERVER_CLOSE_TOO_SLOW );
}

static void
stream_finish( fd_grpc_server_t *        server,
               fd_grpc_server_stream_t * s ) {
  fd_grpc_server_conn_t * conn = &server->conns[ s->conn_idx ];
  if( FD_UNLIKELY( !fd_h2_tx_check_sz( conn->rbuf_tx, FD_GRPC_SERVER_HDR_RESERVE ) ) ) return;

  uint flags = FD_H2_FLAG_END_HEADERS | FD_H2_FLAG_END_STREAM;
  fd_h2_tx_prepare( conn->h2, conn->rbuf_tx, FD_H2_FRAME_TYPE_HEADERS, flags, s->h2->stream_id );
  int ok = 1;
  if( !s->hdrs_sent ) ok &= fd_grpc_h2_gen_response_hdrs( conn->rbuf_tx );
  ok &= fd_grpc_h2_gen_response_trailers( conn->rbuf_tx, s->close_status, s->close_msg, s->close_msg_len );
  fd_h2_tx_commit( conn->h2, conn->rbuf_tx );

  if( FD_UNLIKELY( !ok ) ) {
    stream_error( server, conn, s, FD_H2_ERR_INTERNAL, s->close_reason );
    return;
  }
  stream_detach( server, conn, s, s->close_reason );
}

static void
stream_tx_pump( fd_grpc_server_t *        server,
                fd_grpc_server_stream_t * s ) {
  if( FD_UNLIKELY( !s->in_use || !s->accepted ) ) return;
  fd_grpc_server_conn_t * conn = &server->conns[ s->conn_idx ];
  ulong desc_mask  = server->params.tx_desc_cnt-1UL;
  ulong oring_mask = server->oring_mask;

  int blocked = 0;
  for(;;) {
    if( !s->tx_op->chunk_sz ) {
      if( s->tx_desc_tail==s->tx_desc_head ) break;
      ulong slot = s->tx_desc_tail & desc_mask;
      fd_h2_tx_op_init( s->tx_op,
                        server->oring + ( s->tx_desc_off[ slot ] & oring_mask ),
                        s->tx_desc_len[ slot ], 0U );
    }
    ulong before = s->tx_op->chunk_sz;
    fd_h2_tx_op_copy( conn->h2, s->h2, conn->rbuf_tx, s->tx_op );
    server->metrics.bytes_tx += before - s->tx_op->chunk_sz;
    if( s->tx_op->chunk_sz ) { blocked = 1; break; }
    s->tx_desc_tail++;
  }

  if( !blocked && s->closing ) stream_finish( server, s );

  if( s->in_use && ( s->tx_desc_head!=s->tx_desc_tail || s->tx_op->chunk_sz || s->closing ) ) {
    conn->tx_pending = 1;
  }
}

uchar *
fd_grpc_server_msg_stage( fd_grpc_server_t * server,
                          ulong              msg_max ) {
  stream_close_flush( server );

  if( FD_UNLIKELY( server->stage_len ) ) server->metrics.msg_abandoned++;
  server->stage_len = 0UL;

  ulong tot = 5UL + msg_max;
  if( FD_UNLIKELY( tot>server->oring_sz ) ) {
    server->metrics.msg_dropped++;
    return NULL;
  }

  /* Keep every staged message contiguous so a tx_op can point straight
     into the ring.  Skip the tail when one would straddle the wrap. */
  ulong phys = server->stage_off & server->oring_mask;
  if( phys+tot > server->oring_sz ) {
    server->stage_off += server->oring_sz - phys;
  }

  ulong horizon = server->stage_off + tot;
  if( horizon>server->oring_sz ) {
    ulong floor_off = horizon - server->oring_sz;
    if( server->oldest_min<floor_off ) {
      ulong new_min = ULONG_MAX;
      server->defer_depth++;
      for( ulong m=server->stream_used; m; m &= m-1UL ) {
        fd_grpc_server_stream_t * s = &server->streams[ fd_ulong_find_lsb( m ) ];
        ulong oldest = stream_oldest_off( server, s );
        if( oldest==ULONG_MAX ) continue;
        if( FD_UNLIKELY( oldest<floor_off ) ) { stream_evict( server, s ); continue; }
        new_min = fd_ulong_min( new_min, oldest );
      }
      server->defer_depth--;
      server->oldest_min = new_min;
    }
  }

  server->stage_len = tot;
  server->metrics.msg_staged++;
  return server->oring + ( ( server->stage_off+5UL ) & server->oring_mask );
}

void
fd_grpc_server_msg_broadcast( fd_grpc_server_t * server,
                              ulong              msg_sz,
                              ulong              stream_mask ) {
  if( FD_UNLIKELY( !server->stage_len ) ) return;
  if( FD_UNLIKELY( 5UL+msg_sz>server->stage_len ) ) {
    FD_LOG_CRIT(( "fd_grpc_server_msg_broadcast: committed %lu > staged %lu", msg_sz, server->stage_len-5UL ));
  }

  ulong off = server->stage_off;
  ulong len = 5UL + msg_sz;
  fd_grpc_msg_hdr_encode( server->oring + ( off & server->oring_mask ), msg_sz );

  server->stage_off += len;
  server->stage_len  = 0UL;

  ulong desc_cnt  = server->params.tx_desc_cnt;
  ulong desc_mask = desc_cnt-1UL;

  server->defer_depth++;
  for( ulong m=stream_mask & server->stream_used; m; m &= m-1UL ) {
    fd_grpc_server_stream_t * s = &server->streams[ fd_ulong_find_lsb( m ) ];
    if( FD_UNLIKELY( !s->in_use || !s->accepted || s->closing ) ) continue;

    if( FD_UNLIKELY( s->tx_desc_head-s->tx_desc_tail >= desc_cnt ) ) {
      stream_evict( server, s );
      continue;
    }
    if( s->tx_desc_head==s->tx_desc_tail ) server->oldest_min = fd_ulong_min( server->oldest_min, off );
    ulong slot = s->tx_desc_head & desc_mask;
    s->tx_desc_off[ slot ] = off;
    s->tx_desc_len[ slot ] = len;
    s->tx_desc_head++;
    server->metrics.msg_queued++;

    fd_grpc_server_conn_t * conn = &server->conns[ s->conn_idx ];
    stream_tx_pump( server, s );

    if( fd_h2_rbuf_used_sz( conn->rbuf_tx )*2UL >= server->params.conn_tx_bufsz ) server->tx_urgent = 1;
  }
  server->defer_depth--;

  stream_close_flush( server );
}

void
fd_grpc_server_stream_close( fd_grpc_server_t * server,
                             ulong              idx,
                             uint               grpc_status,
                             char const *       grpc_msg,
                             ulong              grpc_msg_len ) {
  if( FD_UNLIKELY( idx>=server->params.max_stream_cnt ) ) return;
  fd_grpc_server_stream_t * s = &server->streams[ idx ];
  if( FD_UNLIKELY( !s->in_use || s->closing ) ) return;

  ulong msg_len = 0UL;
  if( grpc_msg && grpc_msg_len ) {
    static char const hex[] = "0123456789ABCDEF";
    for( ulong i=0UL; i<grpc_msg_len; i++ ) {
      uint c = (uint)(uchar)grpc_msg[ i ];
      if( FD_LIKELY( c>=0x20U && c<=0x7eU && c!='%' ) ) {
        if( FD_UNLIKELY( msg_len+1UL>FD_GRPC_SERVER_GRPC_MSG_MAX ) ) break;
        s->close_msg[ msg_len++ ] = (char)c;
      } else {
        if( FD_UNLIKELY( msg_len+3UL>FD_GRPC_SERVER_GRPC_MSG_MAX ) ) break;
        s->close_msg[ msg_len++ ] = '%';
        s->close_msg[ msg_len++ ] = hex[ c>>4    ];
        s->close_msg[ msg_len++ ] = hex[ c&0x0fU ];
      }
    }
  }

  s->close_msg_len = msg_len;
  s->closing       = 1;
  s->close_status  = grpc_status;
  s->close_reason  = FD_GRPC_SERVER_CLOSE_OK;
  stream_tx_pump( server, s );
}

static void
stream_reject( fd_grpc_server_t *        server,
               fd_grpc_server_conn_t *   conn,
               fd_grpc_server_stream_t * s,
               uint                      grpc_status ) {
  server->metrics.stream_rejected++;
  if( FD_LIKELY( fd_h2_tx_check_sz( conn->rbuf_tx, FD_GRPC_SERVER_HDR_RESERVE ) ) ) {
    uint flags = FD_H2_FLAG_END_HEADERS | FD_H2_FLAG_END_STREAM;
    fd_h2_tx_prepare( conn->h2, conn->rbuf_tx, FD_H2_FRAME_TYPE_HEADERS, flags, s->h2->stream_id );
    int ok = fd_grpc_h2_gen_response_hdrs( conn->rbuf_tx );
    ok &= fd_grpc_h2_gen_response_trailers( conn->rbuf_tx, grpc_status, NULL, 0UL );
    fd_h2_tx_commit( conn->h2, conn->rbuf_tx );
    if( FD_UNLIKELY( !ok ) ) {
      stream_error( server, conn, s, FD_H2_ERR_INTERNAL, FD_GRPC_SERVER_CLOSE_OK );
      return;
    }
    stream_detach( server, conn, s, FD_GRPC_SERVER_CLOSE_OK );
  } else {
    stream_error( server, conn, s, FD_H2_ERR_REFUSED_STREAM, FD_GRPC_SERVER_CLOSE_OK );
  }
}

static fd_h2_stream_t *
cb_stream_create( fd_h2_conn_t * h2_conn,
                  uint           stream_id ) {
  (void)stream_id;
  fd_grpc_server_t *      server = h2_conn->ctx;
  fd_grpc_server_conn_t * conn   = conn_upcast( h2_conn );

  ulong avail = server->stream_valid & ~server->stream_used;
  if( FD_UNLIKELY( ( (ulong)fd_ulong_popcnt( conn->stream_mask )>=server->params.max_streams_per_conn ) |
                   ( !avail                                                                           ) ) ) {
    server->metrics.stream_rejected++;
    return NULL;
  }

  ulong                     idx = (ulong)fd_ulong_find_lsb( avail );
  fd_grpc_server_stream_t * s   = &server->streams[ idx ];
  fd_h2_stream_init( s->h2 );
  s->in_use        = 1;
  s->accepted      = 0;
  s->hdrs_sent     = 0;
  s->req_eos       = 0;
  s->req_fin       = 0;
  s->closing       = 0;
  s->conn_idx      = conn_index( server, conn );
  s->rx_hdr_off    = 0UL;
  s->rx_msg_sz     = 0UL;
  s->rx_msg_off    = 0UL;
  s->hdr_len       = 0UL;
  s->tx_desc_head  = 0UL;
  s->tx_desc_tail  = 0UL;
  s->close_status  = FD_GRPC_STATUS_OK;
  s->close_reason  = FD_GRPC_SERVER_CLOSE_OK;
  s->close_msg_len = 0UL;
  memset( s->tx_op, 0, sizeof(fd_h2_tx_op_t) );
  memset( s->req,   0, sizeof(fd_grpc_req_hdrs_rx_t) );

  conn->stream_mask   |= 1UL<<idx;
  server->stream_used |= 1UL<<idx;
  server->metrics.stream_opened++;
  return s->h2;
}

static fd_h2_stream_t *
cb_stream_query( fd_h2_conn_t * h2_conn,
                 uint           stream_id ) {
  fd_grpc_server_t *      server = h2_conn->ctx;
  fd_grpc_server_conn_t * conn   = conn_upcast( h2_conn );

  ulong memo = conn->query_memo;
  if( FD_LIKELY( memo<server->params.max_stream_cnt && ( conn->stream_mask & (1UL<<memo) ) ) ) {
    fd_grpc_server_stream_t * s = &server->streams[ memo ];
    if( FD_LIKELY( s->h2->stream_id==stream_id ) ) return s->h2;
  }

  for( ulong m=conn->stream_mask; m; m &= m-1UL ) {
    ulong                     idx = (ulong)fd_ulong_find_lsb( m );
    fd_grpc_server_stream_t * s   = &server->streams[ idx ];
    if( s->h2->stream_id==stream_id ) {
      conn->query_memo = idx;
      return s->h2;
    }
  }
  return NULL;
}

static void
cb_conn_established( fd_h2_conn_t * h2_conn ) {
  (void)h2_conn;
}

static void
cb_conn_final( fd_h2_conn_t * h2_conn,
               uint           h2_err,
               int            closed_by ) {
  (void)h2_err; (void)closed_by;
  fd_grpc_server_t *      server = h2_conn->ctx;
  fd_grpc_server_conn_t * conn   = conn_upcast( h2_conn );

  for( ulong m=conn->stream_mask; m; m &= m-1UL ) {
    stream_release( server, &server->streams[ fd_ulong_find_lsb( m ) ], FD_GRPC_SERVER_CLOSE_CONN_DEAD );
  }
}

static void
cb_headers( fd_h2_conn_t *   h2_conn,
            fd_h2_stream_t * h2_stream,
            void const *     data,
            ulong            data_sz,
            ulong            flags ) {
  fd_grpc_server_t *        server = h2_conn->ctx;
  fd_grpc_server_conn_t *   conn   = conn_upcast( h2_conn );
  fd_grpc_server_stream_t * s      = stream_upcast( h2_stream );

  if( FD_UNLIKELY( !s->in_use ) ) return;
  if( flags & FD_H2_FLAG_END_STREAM ) s->req_eos = 1;

  /* Trailing HEADERS block on an accepted stream */
  if( FD_UNLIKELY( s->accepted ) ) {
    if( s->req_eos && !s->req_fin ) {
      s->req_fin = 1;
      server->cb.request_fin( server->ctx, stream_idx( server, s ) );
    }
    return;
  }

  if( FD_UNLIKELY( data_sz > FD_GRPC_SERVER_HDR_BLOCK_MAX - s->hdr_len ) ) {
    stream_reject( server, conn, s, FD_GRPC_STATUS_RESOURCE_EXHAUSTED );
    return;
  }
  fd_memcpy( s->hdr_buf + s->hdr_len, data, data_sz );
  s->hdr_len += data_sz;

  if( !( flags & FD_H2_FLAG_END_HEADERS ) ) return;

  int err = fd_grpc_h2_read_request_hdrs( s->req, server->matcher, s->hdr_buf, s->hdr_len );
  s->hdr_len = 0UL;
  if( FD_UNLIKELY( err!=FD_H2_SUCCESS ) ) {
    stream_error( server, conn, s, FD_H2_ERR_COMPRESSION, FD_GRPC_SERVER_CLOSE_PEER_RESET );
    return;
  }

  fd_grpc_req_hdrs_rx_t const * req = s->req;
  if( FD_UNLIKELY( !req->is_post || !req->is_grpc_proto || req->path_oversz || !req->path_len ) ) {
    stream_reject( server, conn, s, FD_GRPC_STATUS_UNIMPLEMENTED );
    return;
  }

  if( FD_UNLIKELY( !fd_h2_tx_check_sz( conn->rbuf_tx, FD_GRPC_SERVER_HDR_RESERVE ) ) ) {
    stream_reject( server, conn, s, FD_GRPC_STATUS_RESOURCE_EXHAUSTED );
    return;
  }

  ulong idx = stream_idx( server, s );
  if( FD_UNLIKELY( !server->cb.request_start( server->ctx, idx, req->path, req->path_len ) ) ) {
    stream_reject( server, conn, s, FD_GRPC_STATUS_UNIMPLEMENTED );
    return;
  }
  if( FD_UNLIKELY( !s->in_use ) ) return;

  fd_h2_tx_prepare( conn->h2, conn->rbuf_tx, FD_H2_FRAME_TYPE_HEADERS, FD_H2_FLAG_END_HEADERS, s->h2->stream_id );
  fd_grpc_h2_gen_response_hdrs( conn->rbuf_tx );
  fd_h2_tx_commit( conn->h2, conn->rbuf_tx );

  s->accepted  = 1;
  s->hdrs_sent = 1;

  if( s->req_eos ) {
    s->req_fin = 1;
    server->cb.request_fin( server->ctx, idx );
  }
}

static void
cb_data( fd_h2_conn_t *   h2_conn,
         fd_h2_stream_t * h2_stream,
         void const *     data,
         ulong            data_sz,
         ulong            flags ) {
  fd_grpc_server_t *        server = h2_conn->ctx;
  fd_grpc_server_conn_t *   conn   = conn_upcast( h2_conn );
  fd_grpc_server_stream_t * s      = stream_upcast( h2_stream );

  if( FD_UNLIKELY( !s->in_use || !s->accepted || s->closing ) ) return;

  uchar const * p   = data;
  ulong         rem = data_sz;

  while( rem ) {
    if( s->rx_hdr_off<5UL ) {
      ulong take = fd_ulong_min( 5UL-s->rx_hdr_off, rem );
      fd_memcpy( s->rx_hdr+s->rx_hdr_off, p, take );
      s->rx_hdr_off += take;
      p             += take;
      rem           -= take;
      if( s->rx_hdr_off<5UL ) break;

      uint msg_sz_be;
      fd_memcpy( &msg_sz_be, s->rx_hdr+1, 4UL );
      s->rx_msg_sz  = fd_uint_bswap( msg_sz_be );
      s->rx_msg_off = 0UL;

      if( FD_UNLIKELY( s->rx_hdr[0] || s->rx_msg_sz>server->params.max_req_msg_sz ) ) {
        stream_error( server, conn, s, FD_H2_ERR_PROTOCOL, FD_GRPC_SERVER_CLOSE_PEER_RESET );
        return;
      }
    }

    ulong take = fd_ulong_min( s->rx_msg_sz-s->rx_msg_off, rem );
    if( take ) {
      fd_memcpy( s->rx_msg+s->rx_msg_off, p, take );
      s->rx_msg_off += take;
      p             += take;
      rem           -= take;
    }

    if( s->rx_msg_off>=s->rx_msg_sz ) {
      server->cb.request_msg( server->ctx, stream_idx( server, s ), s->rx_msg, s->rx_msg_sz );
      s->rx_hdr_off = 0UL;
      s->rx_msg_sz  = 0UL;
      s->rx_msg_off = 0UL;
      if( FD_UNLIKELY( !s->in_use || s->closing ) ) return;
    }
  }

  if( ( flags & FD_H2_FLAG_END_STREAM ) ) {
    s->req_eos = 1;
    if( !s->req_fin ) {
      s->req_fin = 1;
      server->cb.request_fin( server->ctx, stream_idx( server, s ) );
    }
  }
}

static void
cb_rst_stream( fd_h2_conn_t *   h2_conn,
               fd_h2_stream_t * h2_stream,
               uint             error_code,
               int              closed_by ) {
  (void)error_code; (void)closed_by;
  fd_grpc_server_t *        server = h2_conn->ctx;
  fd_grpc_server_stream_t * s      = stream_upcast( h2_stream );
  stream_release( server, s, FD_GRPC_SERVER_CLOSE_PEER_RESET );
}

static void
cb_window_update( fd_h2_conn_t * h2_conn,
                  uint           increment ) {
  (void)increment;
  fd_grpc_server_t *      server = h2_conn->ctx;
  fd_grpc_server_conn_t * conn   = conn_upcast( h2_conn );
  for( ulong m=conn->stream_mask; m; m &= m-1UL ) {
    stream_tx_pump( server, &server->streams[ fd_ulong_find_lsb( m ) ] );
  }
}

static void
cb_stream_window_update( fd_h2_conn_t *   h2_conn,
                         fd_h2_stream_t * h2_stream,
                         uint             increment ) {
  (void)increment;
  fd_grpc_server_t *        server = h2_conn->ctx;
  fd_grpc_server_stream_t * s      = stream_upcast( h2_stream );
  stream_tx_pump( server, s );
}

static void
cb_ping_ack( fd_h2_conn_t * h2_conn ) {
  (void)h2_conn;
}

static void
fd_grpc_server_h2_cb_bootstrap( void ) {
  if( FD_LIKELY( fd_grpc_server_h2_cb_init ) ) return;
  fd_h2_callbacks_init( &fd_grpc_server_h2_cb );
  fd_grpc_server_h2_cb.stream_create        = cb_stream_create;
  fd_grpc_server_h2_cb.stream_query         = cb_stream_query;
  fd_grpc_server_h2_cb.conn_established     = cb_conn_established;
  fd_grpc_server_h2_cb.conn_final           = cb_conn_final;
  fd_grpc_server_h2_cb.headers              = cb_headers;
  fd_grpc_server_h2_cb.data                 = cb_data;
  fd_grpc_server_h2_cb.rst_stream           = cb_rst_stream;
  fd_grpc_server_h2_cb.window_update        = cb_window_update;
  fd_grpc_server_h2_cb.stream_window_update = cb_stream_window_update;
  fd_grpc_server_h2_cb.ping_ack             = cb_ping_ack;
  fd_grpc_server_h2_cb_init = 1;
}

int
fd_grpc_server_listen( fd_grpc_server_t * server,
                       uint               ip4_addr,
                       ushort             port ) {
  if( FD_UNLIKELY( server->listen_sock>=0 ) ) return EALREADY;

  int sock = socket( AF_INET, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC, IPPROTO_TCP );
  if( FD_UNLIKELY( sock<0 ) ) return errno;

  int one = 1;
  if( FD_UNLIKELY( 0!=setsockopt( sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(int) ) ) ) {
    int err = errno; close( sock ); return err;
  }

  struct sockaddr_in addr = {0};
  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = ip4_addr;
  addr.sin_port        = fd_ushort_bswap( port );
  if( FD_UNLIKELY( 0!=bind( sock, fd_type_pun_const( &addr ), sizeof(struct sockaddr_in) ) ) ) {
    int err = errno; close( sock ); return err;
  }
  if( FD_UNLIKELY( 0!=listen( sock, 64 ) ) ) {
    int err = errno; close( sock ); return err;
  }

  server->listen_sock = sock;
  server->listen_port = port;
  if( FD_UNLIKELY( !port ) ) {
    struct sockaddr_in bound = {0};
    socklen_t          len   = sizeof(struct sockaddr_in);
    if( FD_LIKELY( 0==getsockname( sock, fd_type_pun( &bound ), &len ) ) ) {
      server->listen_port = fd_ushort_bswap( bound.sin_port );
    }
  }
  FD_LOG_NOTICE(( "gRPC server listening at " FD_IP4_ADDR_FMT ":%hu",
                  FD_IP4_ADDR_FMT_ARGS( ip4_addr ), server->listen_port ));
  return 0;
}

int
fd_grpc_server_fd( fd_grpc_server_t const * server ) {
  return server->listen_sock;
}

static fd_grpc_server_conn_t *
conn_alloc( fd_grpc_server_t * server ) {
  if( FD_UNLIKELY( !server->conn_free_cnt ) ) return NULL;
  ulong                   idx  = server->conn_free[ --server->conn_free_cnt ];
  fd_grpc_server_conn_t * conn = &server->conns[ idx ];
  conn->in_use      = 1;
  conn->preface_ok  = 0;
  conn->tx_blocked  = 0;
  conn->tx_pending  = 0;
  conn->preface_sz  = 0UL;
  conn->stream_mask = 0UL;
  conn->query_memo  = ULONG_MAX;
  conn->active_idx  = server->conn_active_cnt;
  server->conn_active[ server->conn_active_cnt++ ] = idx;
  return conn;
}

static void
conn_close( fd_grpc_server_t *      server,
            fd_grpc_server_conn_t * conn,
            int                     reason ) {
  if( FD_UNLIKELY( !conn->in_use ) ) return;
  for( ulong m=conn->stream_mask; m; m &= m-1UL ) {
    stream_release( server, &server->streams[ fd_ulong_find_lsb( m ) ], reason );
  }
  if( conn->sock>=0 ) close( conn->sock );
  conn->sock        = -1;
  conn->in_use      = 0;
  conn->preface_ok  = 0;
  conn->preface_sz  = 0UL;
  conn->stream_mask = 0UL;

  ulong slot  = conn->active_idx;
  ulong last  = --server->conn_active_cnt;
  ulong moved = server->conn_active[ last ];
  server->conn_active[ slot ]        = moved;
  server->conns[ moved ].active_idx  = slot;
  server->conn_free[ server->conn_free_cnt++ ] = conn_index( server, conn );
}

static void
conn_goaway( fd_grpc_server_conn_t * conn ) {
  if( FD_UNLIKELY( !conn->preface_ok || conn->sock<0                       ) ) return;
  if( FD_UNLIKELY( conn->h2->flags & FD_H2_CONN_FLAGS_DEAD                 ) ) return;
  if( FD_UNLIKELY( fd_h2_rbuf_free_sz( conn->rbuf_tx )<sizeof(fd_h2_goaway_t) ) ) return;

  uint last = conn->h2->rx_stream_next>=3U ? conn->h2->rx_stream_next-2U : 0U;
  fd_h2_goaway_t goaway = {
    .hdr            = { .typlen = fd_h2_frame_typlen( FD_H2_FRAME_TYPE_GOAWAY, 8UL ) },
    .last_stream_id = fd_uint_bswap( last ),
    .error_code     = fd_uint_bswap( FD_H2_SUCCESS )
  };
  fd_h2_rbuf_push( conn->rbuf_tx, &goaway, sizeof(fd_h2_goaway_t) );
  fd_h2_rbuf_sendmsg( conn->rbuf_tx, conn->sock, MSG_NOSIGNAL|MSG_DONTWAIT );
}

void
fd_grpc_server_shutdown( fd_grpc_server_t * server ) {
  while( server->conn_active_cnt ) {
    fd_grpc_server_conn_t * conn = &server->conns[ server->conn_active[ server->conn_active_cnt-1UL ] ];
    conn_goaway( conn );
    conn_close( server, conn, FD_GRPC_SERVER_CLOSE_SHUTDOWN );
  }
  stream_close_flush( server );
  if( server->listen_sock>=0 ) {
    close( server->listen_sock );
    server->listen_sock = -1;
  }
}

static int
conn_accept( fd_grpc_server_t * server ) {
  int charge_busy = 0;
  for(;;) {
    int sock = accept4( server->listen_sock, NULL, NULL, SOCK_NONBLOCK|SOCK_CLOEXEC );
    if( sock<0 ) {
      if( FD_LIKELY( errno==EAGAIN || errno==EWOULDBLOCK ) ) break;
      if( errno==EINTR || errno==ECONNABORTED ) continue;
      server->metrics.conn_err++;
      FD_LOG_WARNING(( "accept4 failed (%i-%s)", errno, fd_io_strerror( errno ) ));
      break;
    }
    charge_busy = 1;

    fd_grpc_server_conn_t * conn = conn_alloc( server );
    if( FD_UNLIKELY( !conn ) ) {
      server->metrics.conn_rejected++;
      close( sock );
      continue;
    }

    int one = 1;
    setsockopt( sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(int) );

    conn->sock = sock;
    server->metrics.conn_accepted++;
  }
  return charge_busy;
}

static int
conn_read_preface( fd_grpc_server_t *      server,
                   fd_grpc_server_conn_t * conn ) {
  while( conn->preface_sz<24UL ) {
    long res = read( conn->sock, conn->preface+conn->preface_sz, 24UL-conn->preface_sz );
    if( res<0L ) {
      if( FD_LIKELY( errno==EAGAIN || errno==EWOULDBLOCK ) ) return 1;
      if( FD_UNLIKELY( errno==EINTR ) ) continue;
      return 0;
    }
    if( FD_UNLIKELY( res==0L ) ) return 0;
    if( FD_UNLIKELY( !fd_memeq( conn->preface+conn->preface_sz,
                                fd_h2_client_preface+conn->preface_sz,
                                (ulong)res ) ) ) return 0;
    conn->preface_sz += (ulong)res;
  }

  fd_h2_conn_init_server( conn->h2 );
  conn->h2->ctx = server;
  conn->h2->self_settings.max_concurrent_streams = (uint)server->params.max_streams_per_conn;
  conn->h2->self_settings.max_header_list_size   = (uint)FD_GRPC_SERVER_HDR_BLOCK_MAX;
  if( server->params.initial_window_size ) {
    conn->h2->self_settings.initial_window_size = server->params.initial_window_size;
  }
  fd_h2_rbuf_init( conn->rbuf_rx, conn->rx_buf, server->params.conn_rx_bufsz );
  fd_h2_rbuf_init( conn->rbuf_tx, conn->tx_buf, server->params.conn_tx_bufsz );
  conn->preface_ok = 1;
  return 1;
}

static int
conn_service( fd_grpc_server_t *      server,
              fd_grpc_server_conn_t * conn,
              short                   revents ) {
  int charge_busy = 0;

  if( FD_UNLIKELY( revents & (POLLHUP|POLLERR|POLLNVAL) ) ) {
    conn_close( server, conn, FD_GRPC_SERVER_CLOSE_CONN_DEAD );
    return 1;
  }

  if( !conn->preface_ok ) {
    if( !( revents & POLLIN ) ) return 0;
    if( FD_UNLIKELY( !conn_read_preface( server, conn ) ) ) {
      conn_close( server, conn, FD_GRPC_SERVER_CLOSE_CONN_DEAD );
      return 1;
    }
    if( !conn->preface_ok ) return 1;
    charge_busy = 1;
  }

  fd_h2_tx_control( conn->h2, conn->rbuf_tx, &fd_grpc_server_h2_cb );

  if( FD_LIKELY( !revents && !conn->tx_pending && !fd_h2_rbuf_used_sz( conn->rbuf_tx ) ) ) {
    return charge_busy;
  }

  /* Draining rbuf_tx frees room for more queued messages, so re-pump
     between flushes and let a backlogged subscriber catch up within one
     visit instead of one bufferful per poll. */
  for( ulong iter=0UL; iter<FD_GRPC_SERVER_DRAIN_MAX; iter++ ) {
    conn->tx_pending = 0;
    for( ulong m=conn->stream_mask; m; m &= m-1UL ) {
      stream_tx_pump( server, &server->streams[ fd_ulong_find_lsb( m ) ] );
    }
    ulong before = fd_h2_rbuf_used_sz( conn->rbuf_tx );
    if( !before ) { conn->tx_blocked = 0; break; }
    if( conn->tx_blocked && !( revents & POLLOUT ) ) break;
    int err = fd_h2_rbuf_sendmsg( conn->rbuf_tx, conn->sock, MSG_NOSIGNAL|MSG_DONTWAIT );
    if( FD_UNLIKELY( err ) ) {
      if( FD_LIKELY( err==EAGAIN || err==EWOULDBLOCK ) ) { conn->tx_blocked = 1; break; }
      conn_close( server, conn, FD_GRPC_SERVER_CLOSE_CONN_DEAD );
      return 1;
    }
    if( fd_h2_rbuf_used_sz( conn->rbuf_tx )==before ) { conn->tx_blocked = 1; break; }
    conn->tx_blocked = 0;
    charge_busy = 1;
  }

  if( FD_UNLIKELY( conn->h2->flags & FD_H2_CONN_FLAGS_DEAD ) ) {
    conn_close( server, conn, FD_GRPC_SERVER_CLOSE_CONN_DEAD );
    return 1;
  }

  if( revents & POLLIN ) {
    int err = fd_h2_rbuf_recvmsg( conn->rbuf_rx, conn->sock, MSG_NOSIGNAL|MSG_DONTWAIT );
    if( FD_UNLIKELY( err==EPIPE ) ) {
      conn_close( server, conn, FD_GRPC_SERVER_CLOSE_CONN_DEAD );
      return 1;
    }
    if( FD_UNLIKELY( err && err!=EAGAIN && err!=EWOULDBLOCK ) ) {
      conn_close( server, conn, FD_GRPC_SERVER_CLOSE_CONN_DEAD );
      return 1;
    }
    if( fd_h2_rbuf_used_sz( conn->rbuf_rx ) ) {
      charge_busy = 1;
      fd_h2_rx( conn->h2, conn->rbuf_rx, conn->rbuf_tx,
                server->scratch, server->scratch_sz, &fd_grpc_server_h2_cb );
    }
  }

  return charge_busy;
}

int
fd_grpc_server_poll( fd_grpc_server_t * server ) {
  stream_close_flush( server );
  server->tx_urgent = 0;
  if( FD_UNLIKELY( server->listen_sock<0 ) ) return 0;

  struct pollfd * fds = server->pollfds;
  ulong nfds = 0UL;

  fds[ nfds ].fd      = server->listen_sock;
  fds[ nfds ].events  = POLLIN;
  fds[ nfds ].revents = 0;
  nfds++;

  ulong * conn_of_fd = server->poll_conn_idx;
  ulong   map_cnt    = 0UL;
  for( ulong j=0UL; j<server->conn_active_cnt; j++ ) {
    ulong                   idx  = server->conn_active[ j ];
    fd_grpc_server_conn_t * conn = &server->conns[ idx ];
    short events = POLLIN;
    if( conn->preface_ok && fd_h2_rbuf_used_sz( conn->rbuf_tx ) ) events = (short)( events | POLLOUT );
    fds[ nfds ].fd      = conn->sock;
    fds[ nfds ].events  = events;
    fds[ nfds ].revents = 0;
    conn_of_fd[ map_cnt++ ] = idx;
    nfds++;
  }

  int rc = fd_syscall_poll( fds, (uint)nfds, 0 );
  if( FD_UNLIKELY( rc<0 ) ) {
    if( FD_LIKELY( errno==EINTR ) ) return 0;
    FD_LOG_WARNING(( "poll failed (%i-%s)", errno, fd_io_strerror( errno ) ));
    return 0;
  }

  int charge_busy = 0;
  if( fds[ 0 ].revents & POLLIN ) charge_busy |= conn_accept( server );

  for( ulong j=0UL; j<map_cnt; j++ ) {
    fd_grpc_server_conn_t * conn = &server->conns[ conn_of_fd[ j ] ];
    if( !conn->in_use ) continue;
    charge_busy |= conn_service( server, conn, fds[ j+1UL ].revents );
  }

  return charge_busy;
}