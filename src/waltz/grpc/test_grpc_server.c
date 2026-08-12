#define _GNU_SOURCE
#include "fd_grpc_server.h"
#include "../h2/fd_h2_proto.h"
#include "../../util/fd_util.h"
#include "../../util/net/fd_ip4.h"

#include <errno.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#define TEST_PATH "/test.v1.Echo/Stream"
#define BAD_PATH  "/test.v1.Echo/Nope"

/* Callback trace.  Every scenario asserts against this. */

struct trace {
  ulong start_cnt;
  ulong msg_cnt;
  ulong fin_cnt;
  ulong close_cnt;

  ulong last_start_idx;
  char  last_path[ 128 ];
  ulong last_path_len;

  ulong last_msg_idx;
  uchar last_msg[ 1024 ];
  ulong last_msg_sz;

  int   last_close_reason;
  int   accept_all;
};

typedef struct trace trace_t;

static trace_t trace[1];

static int
cb_start( void *       ctx,
          ulong        stream_idx,
          char const * path,
          ulong        path_len ) {
  (void)ctx;
  trace->start_cnt++;
  trace->last_start_idx = stream_idx;
  trace->last_path_len  = fd_ulong_min( path_len, sizeof(trace->last_path) );
  fd_memcpy( trace->last_path, path, trace->last_path_len );
  if( trace->accept_all ) return 1;
  return path_len==sizeof(TEST_PATH)-1UL && !memcmp( path, TEST_PATH, path_len );
}

static void
cb_msg( void *        ctx,
        ulong         stream_idx,
        uchar const * msg,
        ulong         msg_sz ) {
  (void)ctx;
  trace->msg_cnt++;
  trace->last_msg_idx = stream_idx;
  trace->last_msg_sz  = fd_ulong_min( msg_sz, sizeof(trace->last_msg) );
  fd_memcpy( trace->last_msg, msg, trace->last_msg_sz );
}

static void
cb_fin( void * ctx,
        ulong  stream_idx ) {
  (void)ctx; (void)stream_idx;
  trace->fin_cnt++;
}

static void
cb_close( void * ctx,
          ulong  stream_idx,
          int    reason ) {
  (void)ctx; (void)stream_idx;
  trace->close_cnt++;
  trace->last_close_reason = reason;
}

static fd_grpc_server_callbacks_t const test_cb = {
  .request_start = cb_start,
  .request_msg   = cb_msg,
  .request_fin   = cb_fin,
  .stream_close  = cb_close
};

/* Server lifecycle helpers */

static uchar server_mem[ 16UL<<20 ] __attribute__((aligned(128)));

static fd_grpc_server_t *
server_start( fd_grpc_server_params_t const * params,
              ushort *                        out_port ) {
  memset( trace, 0, sizeof(trace_t) );

  ulong footprint = fd_grpc_server_footprint( params );
  FD_TEST( footprint );
  FD_TEST( footprint<=sizeof(server_mem) );

  FD_TEST( fd_grpc_server_new( server_mem, params, &test_cb, NULL, 1UL ) );
  fd_grpc_server_t * server = fd_grpc_server_join( server_mem );
  FD_TEST( server );

  FD_TEST( 0==fd_grpc_server_listen( server, FD_IP4_ADDR( 127,0,0,1 ), (ushort)0 ) );
  *out_port = fd_grpc_server_listen_port( server );
  FD_TEST( *out_port );
  return server;
}

static void
server_stop( fd_grpc_server_t * server ) {
  fd_grpc_server_shutdown( server );
  FD_TEST( fd_grpc_server_delete( fd_grpc_server_leave( server ) ) );
}

/* Client side: just enough HTTP/2 to drive one RPC. */

struct client {
  int   sock;
  uchar rx[ 65536 ];
  ulong rx_len;
};

typedef struct client client_t;

static void
client_connect( client_t * c,
                ushort     port ) {
  memset( c, 0, sizeof(client_t) );
  c->sock = socket( AF_INET, SOCK_STREAM|SOCK_CLOEXEC, IPPROTO_TCP );
  FD_TEST( c->sock>=0 );

  struct sockaddr_in addr = {0};
  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = FD_IP4_ADDR( 127,0,0,1 );
  addr.sin_port        = fd_ushort_bswap( port );
  FD_TEST( 0==connect( c->sock, fd_type_pun_const( &addr ), sizeof(struct sockaddr_in) ) );

  int one = 1;
  FD_TEST( 0==setsockopt( c->sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(int) ) );
}

static void
client_close( client_t * c ) {
  if( c->sock>=0 ) close( c->sock );
  c->sock = -1;
}

static void
client_send( client_t *    c,
             void const *  buf,
             ulong         sz ) {
  uchar const * p = buf;
  while( sz ) {
    long res = send( c->sock, p, sz, MSG_NOSIGNAL );
    if( res<0L && errno==EINTR ) continue;
    FD_TEST( res>0L );
    p  += (ulong)res;
    sz -= (ulong)res;
  }
}

/* client_drain moves whatever the kernel has for us into c->rx without
   blocking.  Returns bytes moved. */

static ulong
client_drain( client_t * c ) {
  ulong moved = 0UL;
  for(;;) {
    if( c->rx_len>=sizeof(c->rx) ) break;
    long res = recv( c->sock, c->rx+c->rx_len, sizeof(c->rx)-c->rx_len, MSG_DONTWAIT );
    if( res<0L ) {
      if( errno==EINTR ) continue;
      break; /* EAGAIN */
    }
    if( res==0L ) break;
    c->rx_len += (ulong)res;
    moved     += (ulong)res;
  }
  return moved;
}

/* pump runs the server and drains the client until cond holds or the
   budget runs out.  Everything here is non-blocking, so progress needs
   both sides serviced in the same loop. */

typedef int (* cond_fn_t)( void * arg );

static int
pump( fd_grpc_server_t * server,
      client_t *         c,
      cond_fn_t          cond,
      void *             arg ) {
  for( ulong i=0UL; i<20000UL; i++ ) {
    fd_grpc_server_poll( server );
    if( c ) client_drain( c );
    if( cond && cond( arg ) ) return 1;
    if( !(i%64UL) ) {
      struct timespec ts = { .tv_sec=0, .tv_nsec=50000L };
      nanosleep( &ts, NULL );
    }
  }
  return cond ? 0 : 1;
}

static void
pump_quiet( fd_grpc_server_t * server,
            client_t *         c,
            ulong              rounds ) {
  for( ulong i=0UL; i<rounds; i++ ) {
    fd_grpc_server_poll( server );
    if( c ) client_drain( c );
    struct timespec ts = { .tv_sec=0, .tv_nsec=50000L };
    nanosleep( &ts, NULL );
  }
}

/* Frame builders */

static ulong
put_frame_hdr( uchar * out,
               ulong   payload_sz,
               uchar   type,
               uchar   flags,
               uint    stream_id ) {
  out[0] = (uchar)( payload_sz>>16 );
  out[1] = (uchar)( payload_sz>>8  );
  out[2] = (uchar)( payload_sz     );
  out[3] = type;
  out[4] = flags;
  out[5] = (uchar)( stream_id>>24 );
  out[6] = (uchar)( stream_id>>16 );
  out[7] = (uchar)( stream_id>>8  );
  out[8] = (uchar)( stream_id     );
  return 9UL;
}

static void
client_preface( client_t * c,
                uint       initial_window_size ) {
  client_send( c, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", 24UL );

  uchar buf[ 9+6 ];
  ulong n = put_frame_hdr( buf, 6UL, FD_H2_FRAME_TYPE_SETTINGS, 0, 0U );
  buf[ n+0 ] = (uchar)( FD_H2_SETTINGS_INITIAL_WINDOW_SIZE>>8 );
  buf[ n+1 ] = (uchar)( FD_H2_SETTINGS_INITIAL_WINDOW_SIZE    );
  buf[ n+2 ] = (uchar)( initial_window_size>>24 );
  buf[ n+3 ] = (uchar)( initial_window_size>>16 );
  buf[ n+4 ] = (uchar)( initial_window_size>>8  );
  buf[ n+5 ] = (uchar)( initial_window_size     );
  client_send( c, buf, n+6UL );
}

/* Builds an HPACK block for a gRPC request.  All entries are static
   indexed or literal-with-indexed-name, since fd_hpack has no dynamic
   table. */

static ulong
build_hdr_block( uchar *      out,
                 char const * path,
                 ulong        path_len ) {
  ulong n = 0UL;
  out[ n++ ] = 0x83;                              /* :method: POST      */
  out[ n++ ] = 0x86;                              /* :scheme: http      */
  out[ n++ ] = 0x04;                              /* :path (literal)    */
  FD_TEST( path_len<127UL );
  out[ n++ ] = (uchar)path_len;
  fd_memcpy( out+n, path, path_len ); n += path_len;
  fd_memcpy( out+n, "\x5f\x16" "application/grpc+proto", 24UL ); n += 24UL;
  return n;
}

static void
client_headers( client_t *   c,
                uint         stream_id,
                char const * path,
                ulong        path_len,
                int          end_stream ) {
  uchar block[ 256 ];
  ulong block_sz = build_hdr_block( block, path, path_len );

  uchar flags = FD_H2_FLAG_END_HEADERS;
  if( end_stream ) flags = (uchar)( flags | FD_H2_FLAG_END_STREAM );

  uchar hdr[ 9 ];
  put_frame_hdr( hdr, block_sz, FD_H2_FRAME_TYPE_HEADERS, flags, stream_id );
  client_send( c, hdr,   9UL      );
  client_send( c, block, block_sz );
}

/* Sends a length-prefixed gRPC message, optionally split across two
   DATA frames at byte `split` of the 5+body wire form. */

static void
client_msg( client_t *    c,
            uint          stream_id,
            uchar const * body,
            ulong         body_sz,
            ulong         split,
            int           end_stream ) {
  uchar wire[ 1024+5 ];
  FD_TEST( body_sz+5UL<=sizeof(wire) );
  wire[0] = 0;
  wire[1] = (uchar)( body_sz>>24 );
  wire[2] = (uchar)( body_sz>>16 );
  wire[3] = (uchar)( body_sz>>8  );
  wire[4] = (uchar)( body_sz     );
  fd_memcpy( wire+5, body, body_sz );
  ulong wire_sz = body_sz+5UL;

  if( !split || split>=wire_sz ) split = wire_sz;

  uchar hdr[ 9 ];
  uchar flags = (uchar)( ( split==wire_sz && end_stream ) ? FD_H2_FLAG_END_STREAM : 0 );
  put_frame_hdr( hdr, split, FD_H2_FRAME_TYPE_DATA, flags, stream_id );
  client_send( c, hdr, 9UL );
  client_send( c, wire, split );

  if( split<wire_sz ) {
    flags = (uchar)( end_stream ? FD_H2_FLAG_END_STREAM : 0 );
    put_frame_hdr( hdr, wire_sz-split, FD_H2_FRAME_TYPE_DATA, flags, stream_id );
    client_send( c, hdr, 9UL );
    client_send( c, wire+split, wire_sz-split );
  }
}

/* Frame reader over the drained buffer */

struct frame {
  ulong         sz;
  uchar         type;
  uchar         flags;
  uint          stream_id;
  uchar const * payload;
};

typedef struct frame frame_t;

/* frame_at decodes the frame starting at *off, advancing it.  Returns 0
   if the buffer does not yet hold a whole frame. */

static int
frame_at( client_t * c,
          ulong *    off,
          frame_t *  out ) {
  if( *off+9UL > c->rx_len ) return 0;
  uchar const * p  = c->rx + *off;
  ulong         sz = ( (ulong)p[0]<<16 ) | ( (ulong)p[1]<<8 ) | (ulong)p[2];
  if( *off+9UL+sz > c->rx_len ) return 0;
  out->sz        = sz;
  out->type      = p[3];
  out->flags     = p[4];
  out->stream_id = ( (uint)p[5]<<24 ) | ( (uint)p[6]<<16 ) | ( (uint)p[7]<<8 ) | (uint)p[8];
  out->payload   = p+9;
  *off += 9UL+sz;
  return 1;
}

/* find_frame scans for the next frame of the given type. */

static int
find_frame( client_t * c,
            uchar      type,
            frame_t *  out ) {
  ulong   off = 0UL;
  frame_t f[1];
  while( frame_at( c, &off, f ) ) {
    if( f->type==type ) { *out = *f; return 1; }
  }
  return 0;
}

static ulong
count_frames( client_t * c,
              uchar      type ) {
  ulong   off = 0UL, cnt = 0UL;
  frame_t f[1];
  while( frame_at( c, &off, f ) ) {
    if( f->type==type ) cnt++;
  }
  return cnt;
}

/* any_hdr_contains searches every HEADERS payload for a byte string.
   Used to inspect trailers without decoding HPACK. */

static int
any_hdr_contains( client_t *   c,
                  char const * needle,
                  ulong        needle_len ) {
  ulong   off = 0UL;
  frame_t f[1];
  while( frame_at( c, &off, f ) ) {
    if( f->type!=FD_H2_FRAME_TYPE_HEADERS ) continue;
    if( f->sz<needle_len ) continue;
    for( ulong i=0UL; i+needle_len<=f->sz; i++ ) {
      if( !memcmp( f->payload+i, needle, needle_len ) ) return 1;
    }
  }
  return 0;
}

/* Conditions */

static int
cond_msg_seen( void * arg ) {
  (void)arg;
  return trace->msg_cnt>0UL && trace->fin_cnt>0UL;
}

static int
cond_start_seen( void * arg ) {
  (void)arg;
  return trace->start_cnt>0UL;
}

static int
cond_close_seen( void * arg ) {
  (void)arg;
  return trace->close_cnt>0UL;
}

struct evict_arg {
  fd_grpc_server_t * server;
};

static int
cond_evicted( void * arg ) {
  struct evict_arg * a = arg;
  return fd_grpc_server_metrics( a->server )->stream_evicted>0UL;
}

static int
cond_data_frame( void * arg ) {
  client_t * c = arg;
  frame_t    f[1];
  return find_frame( c, FD_H2_FRAME_TYPE_DATA, f );
}

/* Scenarios */

static fd_grpc_server_params_t
base_params( void ) {
  return (fd_grpc_server_params_t){
    .max_conn_cnt         = 2UL,
    .max_stream_cnt       = 4UL,
    .max_streams_per_conn = 2UL,
    .max_req_msg_sz       = 1024UL,
    .tx_desc_cnt          = 8UL,
    .oring_sz             = 1UL<<16,
    .conn_rx_bufsz        = 32768UL,
    .conn_tx_bufsz        = 4096UL,
    .initial_window_size  = 1U<<20
  };
}

/* A well-formed RPC produces start/msg/fin, and a broadcast comes back
   as response HEADERS followed by a length-prefixed DATA frame. */

static void
test_happy_path( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );
  client_headers( c, 1U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );

  uchar const body[] = "hello world";
  ulong const body_sz = sizeof(body)-1UL;
  client_msg( c, 1U, body, body_sz, 0UL, 1 );

  FD_TEST( pump( server, c, cond_msg_seen, NULL ) );
  FD_TEST( trace->start_cnt==1UL );
  FD_TEST( trace->last_path_len==sizeof(TEST_PATH)-1UL );
  FD_TEST( !memcmp( trace->last_path, TEST_PATH, trace->last_path_len ) );
  FD_TEST( trace->msg_cnt==1UL );
  FD_TEST( trace->last_msg_sz==body_sz );
  FD_TEST( !memcmp( trace->last_msg, body, body_sz ) );
  FD_TEST( trace->fin_cnt==1UL );

  /* Response headers must be on the wire before any payload. */
  pump_quiet( server, c, 32UL );
  frame_t f[1];
  FD_TEST( find_frame( c, FD_H2_FRAME_TYPE_HEADERS, f ) );
  FD_TEST( f->stream_id==1U );
  FD_TEST( f->flags & FD_H2_FLAG_END_HEADERS );

  ulong mask = fd_grpc_server_active_mask( server );
  FD_TEST( mask==1UL<<trace->last_msg_idx );

  uchar * stage = fd_grpc_server_msg_stage( server, 32UL );
  FD_TEST( stage );
  fd_memcpy( stage, "pong", 4UL );
  fd_grpc_server_msg_broadcast( server, 4UL, mask );

  c->rx_len = 0UL;
  FD_TEST( pump( server, c, cond_data_frame, c ) );
  FD_TEST( find_frame( c, FD_H2_FRAME_TYPE_DATA, f ) );
  FD_TEST( f->sz==9UL );          /* 5 byte gRPC prefix + 4 byte body */
  FD_TEST( f->payload[0]==0 );    /* not compressed */
  FD_TEST( f->payload[1]==0 && f->payload[2]==0 && f->payload[3]==0 && f->payload[4]==4 );
  FD_TEST( !memcmp( f->payload+5, "pong", 4UL ) );

  fd_grpc_server_stream_close( server, trace->last_msg_idx, FD_GRPC_STATUS_OK, NULL, 0UL );
  FD_TEST( pump( server, c, cond_close_seen, NULL ) );
  FD_TEST( trace->last_close_reason==FD_GRPC_SERVER_CLOSE_OK );

  /* Normal completion must not RST the stream. */
  pump_quiet( server, c, 32UL );
  FD_TEST( !count_frames( c, FD_H2_FRAME_TYPE_RST_STREAM ) );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_happy_path: ok" ));
}

/* A length-prefixed message split across two DATA frames, with the
   split landing inside the 5 byte prefix, must reassemble into exactly
   one request_msg. */

static void
test_split_message( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );
  client_headers( c, 1U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );

  uchar body[ 300 ];
  for( ulong i=0UL; i<sizeof(body); i++ ) body[ i ] = (uchar)i;
  client_msg( c, 1U, body, sizeof(body), 3UL, 1 ); /* split inside the prefix */

  FD_TEST( pump( server, c, cond_msg_seen, NULL ) );
  FD_TEST( trace->msg_cnt==1UL );
  FD_TEST( trace->last_msg_sz==sizeof(body) );
  FD_TEST( !memcmp( trace->last_msg, body, sizeof(body) ) );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_split_message: ok" ));
}

/* An unknown path is answered with UNIMPLEMENTED trailers, not a conn
   error, and the stream slot is returned. */

static void
test_unimplemented_path( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );
  client_headers( c, 1U, BAD_PATH, sizeof(BAD_PATH)-1UL, 1 );

  FD_TEST( pump( server, c, cond_start_seen, NULL ) );
  pump_quiet( server, c, 64UL );

  FD_TEST( fd_grpc_server_metrics( server )->stream_rejected>0UL );
  FD_TEST( !fd_grpc_server_active_mask( server ) );

  frame_t f[1];
  FD_TEST( find_frame( c, FD_H2_FRAME_TYPE_HEADERS, f ) );
  FD_TEST( f->flags & FD_H2_FLAG_END_STREAM );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_unimplemented_path: ok" ));
}

/* Opening more streams than max_streams_per_conn gets the extras
   refused without taking down the connection. */

static void
test_stream_limit( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );
  trace->accept_all = 1;

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );

  for( uint i=0U; i<4U; i++ ) {
    client_headers( c, 1U+2U*i, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );
  }
  pump_quiet( server, c, 256UL );

  FD_TEST( (ulong)fd_ulong_popcnt( fd_grpc_server_active_mask( server ) )==params.max_streams_per_conn );
  FD_TEST( count_frames( c, FD_H2_FRAME_TYPE_RST_STREAM )>=2UL );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_stream_limit: ok" ));
}

/* A subscriber whose flow control window never opens accumulates
   descriptors until tx_desc_cnt is exceeded, then gets evicted. */

static void
test_evict_tx_desc( void ) {
  fd_grpc_server_params_t params = base_params();
  params.tx_desc_cnt = 4UL;
  params.oring_sz    = 1UL<<20; /* large, so the ring cannot trip first */

  ushort             port;
  fd_grpc_server_t * server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U ); /* 1 byte stream window: nothing can drain */
  client_headers( c, 1U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );

  uchar body[ 4 ] = "req";
  client_msg( c, 1U, body, 3UL, 0UL, 1 );
  FD_TEST( pump( server, c, cond_msg_seen, NULL ) );

  ulong mask = fd_grpc_server_active_mask( server );
  FD_TEST( mask );

  struct evict_arg arg = { .server = server };
  for( ulong i=0UL; i<params.tx_desc_cnt+4UL; i++ ) {
    uchar * stage = fd_grpc_server_msg_stage( server, 256UL );
    FD_TEST( stage );
    memset( stage, (int)i, 256UL );
    fd_grpc_server_msg_broadcast( server, 256UL, mask );
  }
  FD_TEST( pump( server, c, cond_evicted, &arg ) );
  FD_TEST( trace->last_close_reason==FD_GRPC_SERVER_CLOSE_TOO_SLOW );
  FD_TEST( !fd_grpc_server_active_mask( server ) );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_evict_tx_desc: ok" ));
}

/* Same stall, but with a deep descriptor queue and a small ring, so the
   overtake check in msg_stage is what evicts. */

static void
test_evict_ring_wrap( void ) {
  fd_grpc_server_params_t params = base_params();
  params.tx_desc_cnt = 1024UL;  /* deep, so descriptors cannot trip first */
  params.oring_sz    = 1UL<<13; /* 8 KiB */

  ushort             port;
  fd_grpc_server_t * server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U );
  client_headers( c, 1U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );

  uchar body[ 4 ] = "req";
  client_msg( c, 1U, body, 3UL, 0UL, 1 );
  FD_TEST( pump( server, c, cond_msg_seen, NULL ) );

  ulong mask = fd_grpc_server_active_mask( server );
  FD_TEST( mask );

  struct evict_arg arg = { .server = server };
  /* Write several ring's worth so the staged region must overtake the
     stalled stream's oldest descriptor. */
  for( ulong i=0UL; i<128UL; i++ ) {
    uchar * stage = fd_grpc_server_msg_stage( server, 256UL );
    FD_TEST( stage );
    memset( stage, (int)i, 256UL );
    fd_grpc_server_msg_broadcast( server, 256UL, mask );
    if( fd_grpc_server_metrics( server )->stream_evicted ) break;
  }
  FD_TEST( pump( server, c, cond_evicted, &arg ) );
  FD_TEST( trace->last_close_reason==FD_GRPC_SERVER_CLOSE_TOO_SLOW );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_evict_ring_wrap: ok" ));
}

/* A request message larger than max_req_msg_sz is a protocol error, not
   a buffer overrun. */

static void
test_oversize_request( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );
  client_headers( c, 1U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );

  /* Declare a body far larger than max_req_msg_sz without sending it. */
  uchar hdr[ 9+5 ];
  ulong n = put_frame_hdr( hdr, 5UL, FD_H2_FRAME_TYPE_DATA, 0, 1U );
  hdr[ n+0 ] = 0;
  hdr[ n+1 ] = 0;
  hdr[ n+2 ] = 0x10; /* 1 MiB */
  hdr[ n+3 ] = 0;
  hdr[ n+4 ] = 0;
  client_send( c, hdr, n+5UL );

  FD_TEST( pump( server, c, cond_close_seen, NULL ) );
  FD_TEST( trace->msg_cnt==0UL );
  FD_TEST( !fd_grpc_server_active_mask( server ) );

  frame_t f[1];
  pump_quiet( server, c, 32UL );
  FD_TEST( find_frame( c, FD_H2_FRAME_TYPE_RST_STREAM, f ) );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_oversize_request: ok" ));
}

/* A header block spanning HEADERS + CONTINUATION must decode as one
   HPACK stream.  Split inside the :path literal so a per-fragment
   decoder would fail. */

static void
test_continuation( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );

  uchar block[ 256 ];
  ulong block_sz = build_hdr_block( block, TEST_PATH, sizeof(TEST_PATH)-1UL );
  ulong split    = 6UL; /* inside the :path value */

  uchar hdr[ 9 ];
  put_frame_hdr( hdr, split, FD_H2_FRAME_TYPE_HEADERS, 0, 1U );
  client_send( c, hdr, 9UL );
  client_send( c, block, split );

  put_frame_hdr( hdr, block_sz-split, FD_H2_FRAME_TYPE_CONTINUATION, FD_H2_FLAG_END_HEADERS, 1U );
  client_send( c, hdr, 9UL );
  client_send( c, block+split, block_sz-split );

  FD_TEST( pump( server, c, cond_start_seen, NULL ) );
  FD_TEST( trace->start_cnt==1UL );
  FD_TEST( trace->last_path_len==sizeof(TEST_PATH)-1UL );
  FD_TEST( !memcmp( trace->last_path, TEST_PATH, trace->last_path_len ) );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_continuation: ok" ));
}

/* grpc-message must arrive percent-encoded. */

static void
test_grpc_message_encoding( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );
  client_headers( c, 1U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );

  uchar body[ 4 ] = "req";
  client_msg( c, 1U, body, 3UL, 0UL, 1 );
  FD_TEST( pump( server, c, cond_msg_seen, NULL ) );

  c->rx_len = 0UL;
  char const msg[] = "bad\ninput 50%";
  fd_grpc_server_stream_close( server, trace->last_msg_idx, FD_GRPC_STATUS_INVALID_ARGUMENT,
                               msg, sizeof(msg)-1UL );
  FD_TEST( pump( server, c, cond_close_seen, NULL ) );
  pump_quiet( server, c, 32UL );

  /* Values are written as raw literals, so the escapes appear verbatim
     in the trailers block. */
  FD_TEST( any_hdr_contains( c, "%0A", 3UL ) );
  FD_TEST( any_hdr_contains( c, "%25", 3UL ) );
  FD_TEST( !any_hdr_contains( c, "\n", 1UL ) );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_grpc_message_encoding: ok" ));
}

/* Completing a stream the peer never half-closed must not emit
   RST_STREAM; the trailers alone end it. */

static void
test_close_without_half_close( void ) {
  fd_grpc_server_params_t params = base_params();
  ushort                  port;
  fd_grpc_server_t *      server = server_start( &params, &port );

  client_t c[1];
  client_connect( c, port );
  client_preface( c, 1U<<20 );
  client_headers( c, 1U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );

  uchar body[ 4 ] = "req";
  client_msg( c, 1U, body, 3UL, 0UL, 0 ); /* no END_STREAM */

  FD_TEST( pump( server, c, cond_start_seen, NULL ) );
  pump_quiet( server, c, 32UL );
  FD_TEST( trace->fin_cnt==0UL );

  ulong mask = fd_grpc_server_active_mask( server );
  FD_TEST( mask );

  c->rx_len = 0UL;
  fd_grpc_server_stream_close( server, (ulong)fd_ulong_find_lsb( mask ),
                               FD_GRPC_STATUS_OK, NULL, 0UL );
  FD_TEST( pump( server, c, cond_close_seen, NULL ) );
  pump_quiet( server, c, 64UL );

  FD_TEST( count_frames( c, FD_H2_FRAME_TYPE_HEADERS )>=1UL );
  FD_TEST( !count_frames( c, FD_H2_FRAME_TYPE_RST_STREAM ) );
  FD_TEST( !fd_grpc_server_active_mask( server ) );

  /* The slot must be reusable afterwards. */
  client_headers( c, 3U, TEST_PATH, sizeof(TEST_PATH)-1UL, 0 );
  FD_TEST( pump( server, c, cond_start_seen, NULL ) );
  FD_TEST( fd_grpc_server_active_mask( server ) );

  client_close( c );
  server_stop( server );
  FD_LOG_NOTICE(( "test_close_without_half_close: ok" ));
}

/* Rejecting bad params is part of the contract. */

static void
test_params( void ) {
  fd_grpc_server_params_t p = base_params();
  FD_TEST( fd_grpc_server_footprint( &p ) );

  fd_grpc_server_params_t bad;

  bad = p; bad.max_conn_cnt         = 0UL;      FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.max_stream_cnt       = 0UL;      FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.max_stream_cnt       = 65UL;     FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.max_streams_per_conn = 0UL;      FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.max_streams_per_conn = 5UL;      FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.tx_desc_cnt          = 3UL;      FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.oring_sz             = 3UL;      FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.max_req_msg_sz       = 0UL;      FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.conn_rx_bufsz        = 1024UL;   FD_TEST( !fd_grpc_server_footprint( &bad ) );
  bad = p; bad.conn_tx_bufsz        = 64UL;     FD_TEST( !fd_grpc_server_footprint( &bad ) );

  FD_TEST( !fd_grpc_server_footprint( NULL ) );
  FD_LOG_NOTICE(( "test_params: ok" ));
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  test_params();
  test_happy_path();
  test_split_message();
  test_continuation();
  test_unimplemented_path();
  test_close_without_half_close();
  test_stream_limit();
  test_oversize_request();
  test_evict_tx_desc();
  test_evict_ring_wrap();
  test_grpc_message_encoding();

  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}