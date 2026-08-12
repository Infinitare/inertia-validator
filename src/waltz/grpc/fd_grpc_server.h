#ifndef HEADER_fd_src_waltz_grpc_fd_grpc_server_h
#define HEADER_fd_src_waltz_grpc_fd_grpc_server_h

#include "fd_grpc_codec.h"
#include "../h2/fd_h2.h"

#define FD_GRPC_SERVER_ALIGN (128UL)

#define FD_GRPC_SERVER_STREAM_MAX (64UL)

/* Max percent-encoded 'grpc-message' bytes retained by
   fd_grpc_server_stream_close.  Encoding expands a byte to at most 3, so
   the caller-visible message may be truncated well before this. */

#define FD_GRPC_SERVER_GRPC_MSG_MAX (128UL)

/* Max HPACK-encoded bytes of a request header block, summed over the
   HEADERS frame and any CONTINUATION frames following it.  Also
   advertised as SETTINGS_MAX_HEADER_LIST_SIZE. */

#define FD_GRPC_SERVER_HDR_BLOCK_MAX (8192UL)

/* Buffer sizing floors.  conn_rx_bufsz must fit a max size HTTP/2 frame
   plus its header, else fd_h2_rx kills the conn.  conn_tx_bufsz must fit
   a HEADERS frame plus fd_h2's control frame reserve. */

#define FD_GRPC_SERVER_RX_BUFSZ_MIN (16384UL+9UL)
#define FD_GRPC_SERVER_TX_BUFSZ_MIN (4096UL)

#define FD_GRPC_SERVER_CLOSE_OK         ( -1)
#define FD_GRPC_SERVER_CLOSE_PEER_RESET ( -2)
#define FD_GRPC_SERVER_CLOSE_TOO_SLOW   ( -3)
#define FD_GRPC_SERVER_CLOSE_CONN_DEAD  ( -4)
#define FD_GRPC_SERVER_CLOSE_SHUTDOWN   ( -5)

struct fd_grpc_server_params {
  ulong max_conn_cnt;
  ulong max_stream_cnt;       /* <= FD_GRPC_SERVER_STREAM_MAX */
  ulong max_streams_per_conn; /* in [1,max_stream_cnt] */
  ulong max_req_msg_sz;       /* per-stream inbound reassembly cap */
  ulong tx_desc_cnt;          /* per-stream queued message descriptors, power of 2 */
  ulong oring_sz;             /* shared outgoing byte ring, power of 2 */
  ulong conn_rx_bufsz;        /* >= FD_GRPC_SERVER_RX_BUFSZ_MIN */
  ulong conn_tx_bufsz;        /* >= FD_GRPC_SERVER_TX_BUFSZ_MIN */
  uint  initial_window_size;  /* per-stream RX window advertised to peers */
};

typedef struct fd_grpc_server_params fd_grpc_server_params_t;

struct fd_grpc_server_callbacks {
  int  (* request_start)( void * ctx, ulong stream_idx, char const * path, ulong path_len );

  void (* request_msg  )( void * ctx, ulong stream_idx, uchar const * msg, ulong msg_sz );

  void (* request_fin  )( void * ctx, ulong stream_idx );

  void (* stream_close )( void * ctx, ulong stream_idx, int reason );
};

typedef struct fd_grpc_server_callbacks fd_grpc_server_callbacks_t;

struct fd_grpc_server;
typedef struct fd_grpc_server fd_grpc_server_t;

FD_PROTOTYPES_BEGIN

FD_FN_CONST ulong
fd_grpc_server_align( void );

FD_FN_PURE ulong
fd_grpc_server_footprint( fd_grpc_server_params_t const * params );

void *
fd_grpc_server_new( void *                             mem,
                    fd_grpc_server_params_t const *    params,
                    fd_grpc_server_callbacks_t const * cb,
                    void *                             ctx,
                    ulong                              seed );

fd_grpc_server_t *
fd_grpc_server_join( void * mem );

void *
fd_grpc_server_leave( fd_grpc_server_t * server );

void *
fd_grpc_server_delete( void * mem );

int
fd_grpc_server_listen( fd_grpc_server_t * server,
                       uint               ip4_addr,
                       ushort             port );

FD_FN_PURE int
fd_grpc_server_fd( fd_grpc_server_t const * server );

/* fd_grpc_server_listen_port returns the port the server bound to.  If
   fd_grpc_server_listen was called with port 0, this is the ephemeral
   port the kernel picked.  0 if not listening. */

FD_FN_PURE ushort
fd_grpc_server_listen_port( fd_grpc_server_t const * server );

/* fd_grpc_server_tx_urgent returns 1 if enough response bytes have
   piled up in a connection's send buffer to be worth a syscall now.
   Callers that poll on a timer use this to flush early under load
   without polling once per message.  Cleared by fd_grpc_server_poll. */

FD_FN_PURE int
fd_grpc_server_tx_urgent( fd_grpc_server_t const * server );

/* fd_grpc_server_shutdown closes all conns and the listen socket,
   delivering a stream_close callback for every live stream. */

void
fd_grpc_server_shutdown( fd_grpc_server_t * server );

int
fd_grpc_server_poll( fd_grpc_server_t * server );

uchar *
fd_grpc_server_msg_stage( fd_grpc_server_t * server,
                          ulong              msg_max );

void
fd_grpc_server_msg_broadcast( fd_grpc_server_t * server,
                              ulong              msg_sz,
                              ulong              stream_mask );

void
fd_grpc_server_stream_close( fd_grpc_server_t * server,
                             ulong              stream_idx,
                             uint               grpc_status,
                             char const *       grpc_msg,
                             ulong              grpc_msg_len );

FD_FN_PURE ulong
fd_grpc_server_active_mask( fd_grpc_server_t const * server );

struct fd_grpc_server_metrics {
  ulong conn_accepted;
  ulong conn_rejected;   /* over max_conn_cnt */
  ulong conn_err;        /* accept(2) failures */
  ulong stream_opened;
  ulong stream_rejected; /* over stream limits, or path unimplemented */
  ulong stream_evicted;  /* too slow */
  ulong msg_staged;
  ulong msg_abandoned;   /* staged, then restaged without a broadcast */
  ulong msg_queued;      /* sum over streams, i.e. fan-out count */
  ulong msg_dropped;     /* could not stage, ring too small */
  ulong bytes_tx;
};

typedef struct fd_grpc_server_metrics fd_grpc_server_metrics_t;

FD_FN_CONST fd_grpc_server_metrics_t const *
fd_grpc_server_metrics( fd_grpc_server_t const * server );

FD_PROTOTYPES_END

#endif /* HEADER_fd_src_waltz_grpc_fd_grpc_server_h */
