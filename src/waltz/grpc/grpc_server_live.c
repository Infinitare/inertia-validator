#include "fd_grpc_server.h"
#include "../../util/fd_util.h"
#include "../../util/net/fd_ip4.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define TEST_PATH "/test.v1.Echo/Stream"

static char const test_path[] = TEST_PATH;

struct test_app {
  fd_grpc_server_t * server;
  ulong              reply_cnt;
  ulong              msg_sz;
  ulong              seq;
  int                forever;

  fd_grpc_server_metrics_t last_metrics;
  ulong                    last_live;
};

typedef struct test_app test_app_t;

static void
app_broadcast( test_app_t * app,
               ulong        mask ) {
  char hdr[ 32 ];
  int  hdr_len = snprintf( hdr, sizeof(hdr), "seq-%lu", app->seq );
  if( FD_UNLIKELY( hdr_len<0 ) ) FD_LOG_ERR(( "snprintf failed" ));

  ulong body_sz = fd_ulong_max( (ulong)hdr_len, app->msg_sz );
  uchar * dst = fd_grpc_server_msg_stage( app->server, body_sz );
  if( FD_UNLIKELY( !dst ) ) return;

  fd_memcpy( dst, hdr, (ulong)hdr_len );
  if( body_sz>(ulong)hdr_len ) memset( dst+(ulong)hdr_len, 'x', body_sz-(ulong)hdr_len );
  fd_grpc_server_msg_broadcast( app->server, body_sz, mask );
  app->seq++;
}

static void
app_report( test_app_t * app ) {
  fd_grpc_server_t *               server = app->server;
  fd_grpc_server_metrics_t const * m      = fd_grpc_server_metrics( server );
  ulong                            live   = fd_grpc_server_active_mask( server );

  /* Stay quiet while idle so a parked server doesn't look like a spin. */
  if( live==app->last_live &&
      !memcmp( m, &app->last_metrics, sizeof(fd_grpc_server_metrics_t) ) ) return;
  app->last_live    = live;
  app->last_metrics = *m;

  FD_LOG_NOTICE(( "live=%lu conn(acc=%lu rej=%lu err=%lu) stream(open=%lu rej=%lu evict=%lu) "
                  "msg(staged=%lu queued=%lu drop=%lu aband=%lu) tx=%lu",
                  (ulong)fd_ulong_popcnt( live ),
                  m->conn_accepted, m->conn_rejected, m->conn_err,
                  m->stream_opened, m->stream_rejected, m->stream_evicted,
                  m->msg_staged, m->msg_queued, m->msg_dropped, m->msg_abandoned,
                  m->bytes_tx ));
}

static int
app_request_start( void *       ctx,
                   ulong        stream_idx,
                   char const * path,
                   ulong        path_len ) {
  (void)ctx;
  if( path_len!=sizeof(test_path)-1UL || memcmp( path, test_path, path_len ) ) {
    FD_LOG_NOTICE(( "stream %lu: rejecting path %.*s", stream_idx, (int)path_len, path ));
    return 0;
  }
  FD_LOG_NOTICE(( "stream %lu: accepted %.*s", stream_idx, (int)path_len, path ));
  return 1;
}

static void
app_request_msg( void *        ctx,
                 ulong         stream_idx,
                 uchar const * msg,
                 ulong         msg_sz ) {
  test_app_t * app = ctx;
  FD_LOG_NOTICE(( "stream %lu: request %lu bytes", stream_idx, msg_sz ));
  FD_LOG_HEXDUMP_DEBUG(( "request", msg, msg_sz ));

  /* In forever mode the main loop drives the stream instead. */
  if( app->forever ) return;

  for( ulong i=0UL; i<app->reply_cnt; i++ ) app_broadcast( app, 1UL<<stream_idx );
  fd_grpc_server_stream_close( app->server, stream_idx, FD_GRPC_STATUS_OK, NULL, 0UL );
}

static void
app_request_fin( void * ctx,
                 ulong  stream_idx ) {
  (void)ctx;
  FD_LOG_NOTICE(( "stream %lu: client half-closed", stream_idx ));
}

static void
app_stream_close( void * ctx,
                  ulong  stream_idx,
                  int    reason ) {
  (void)ctx;
  FD_LOG_NOTICE(( "stream %lu: closed (%i)", stream_idx, reason ));
}

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );

  char const * bind_cstr = fd_env_strip_cmdline_cstr  ( &argc, &argv, "--bind",    NULL, "127.0.0.1" );
  ushort       port      = fd_env_strip_cmdline_ushort( &argc, &argv, "--port",    NULL, 50051       );
  ulong        reply_cnt = fd_env_strip_cmdline_ulong ( &argc, &argv, "--replies", NULL, 8UL         );
  ulong        rate      = fd_env_strip_cmdline_ulong ( &argc, &argv, "--rate",    NULL, 100UL       );
  ulong        msg_sz    = fd_env_strip_cmdline_ulong ( &argc, &argv, "--msg-sz",  NULL, 0UL         );
  int          forever   = fd_env_strip_cmdline_contains( &argc, &argv, "--forever" );

  uint ip4 = 0U;
  if( FD_UNLIKELY( !fd_cstr_to_ip4_addr( bind_cstr, &ip4 ) ) ) FD_LOG_ERR(( "invalid --bind" ));
  if( FD_UNLIKELY( !rate ) ) FD_LOG_ERR(( "--rate must be non-zero" ));

  fd_grpc_server_params_t params = {
    .max_conn_cnt         = 4UL,
    .max_stream_cnt       = 8UL,
    .max_streams_per_conn = 8UL,
    .max_req_msg_sz       = 4096UL,
    .tx_desc_cnt          = 64UL,
    .oring_sz             = 1UL<<20,
    .conn_rx_bufsz        = 32768UL,
    .conn_tx_bufsz        = 65536UL,
    .initial_window_size  = 1U<<20
  };

  ulong footprint = fd_grpc_server_footprint( &params );
  FD_TEST( footprint );

  void * mem = aligned_alloc( fd_grpc_server_align(), fd_ulong_align_up( footprint, fd_grpc_server_align() ) );
  FD_TEST( mem );

  static test_app_t app[1];
  app->reply_cnt = reply_cnt;
  app->msg_sz    = msg_sz;
  app->forever   = forever;

  fd_grpc_server_callbacks_t cb = {
    .request_start = app_request_start,
    .request_msg   = app_request_msg,
    .request_fin   = app_request_fin,
    .stream_close  = app_stream_close
  };

  FD_TEST( fd_grpc_server_new( mem, &params, &cb, app, 42UL ) );
  fd_grpc_server_t * server = fd_grpc_server_join( mem );
  FD_TEST( server );
  app->server = server;

  int err = fd_grpc_server_listen( server, ip4, port );
  if( FD_UNLIKELY( err ) ) FD_LOG_ERR(( "listen failed (%i-%s)", err, fd_io_strerror( err ) ));

  if( forever ) {
    FD_LOG_NOTICE(( "streaming forever at %lu msg/s, %lu byte bodies", rate, fd_ulong_max( 5UL, msg_sz ) ));
  }

  long send_interval = (long)( 1000000000UL/rate );
  long now           = fd_log_wallclock();
  long next_send     = now;
  long next_report   = now + 1000000000L;

  for(;;) {
    int busy = fd_grpc_server_poll( server );
    now = fd_log_wallclock();

    if( forever && now>=next_send ) {
      ulong mask = fd_grpc_server_active_mask( server );
      if( mask ) app_broadcast( app, mask );
      next_send += send_interval;
      /* Don't accumulate debt if we fell behind. */
      if( next_send<now ) next_send = now + send_interval;
      busy = 1;
    }

    if( now>=next_report ) {
      app_report( app );
      next_report = now + 1000000000L;
    }

    int send_due_soon = forever && ( next_send-now < 200000L );
    if( !busy && !send_due_soon ) {
      struct timespec ts = { .tv_sec=0, .tv_nsec=200000L };
      nanosleep( &ts, NULL );
    }
  }

  fd_halt();
  return 0;
}