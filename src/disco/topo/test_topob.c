/* test_topob.c – unit tests for fd_topob_auto_layout_cpus.

   Tests verify the auto-layout algorithm on synthetic CPU topologies
   for various core counts, tile sets, and blocklists.

   Each test supplies:
     - a synthetic CPU topology  (physical cores × 2 with HT siblings)
     - a tile set                (tile names and counts)
     - a blocklist               (core indices to exclude)
     - an expected per-CPU array:
         expected[cpu] = tile name   →  tile assigned to that cpu
         expected[cpu] = NULL (__) →  cpu unassigned                   */

#include "fd_topob.h"
#include "fd_cpu_topo.h"
#include "../../util/tmpl/fd_unit_test.c"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

/* ---- Tile specification ------------------------------------------------ */

typedef struct { char const * name; ulong cnt; } tile_spec_t;

/* ---- Helpers ----------------------------------------------------------- */

/* Create a synthetic HT CPU topology.
   physical_cores physical cores on a single NUMA node.
   Core i has HT sibling at physical_cores+i.
   Total logical CPUs = 2 * physical_cores. */
static void
make_cpus( fd_topo_cpus_t * cpus,
           ulong            physical_cores ) {
  fd_memset( cpus, 0, sizeof(*cpus) );
  cpus->numa_node_cnt = 1UL;
  cpus->cpu_cnt       = 2UL * physical_cores;
  for( ulong i=0UL; i<physical_cores; i++ ) {
    cpus->cpu[ i ].idx       = i;
    cpus->cpu[ i ].online    = 1;
    cpus->cpu[ i ].numa_node = 0UL;
    cpus->cpu[ i ].sibling   = physical_cores + i;
  }
  for( ulong i=0UL; i<physical_cores; i++ ) {
    ulong s = physical_cores + i;
    cpus->cpu[ s ].idx       = s;
    cpus->cpu[ s ].online    = 1;
    cpus->cpu[ s ].numa_node = 0UL;
    cpus->cpu[ s ].sibling   = i;
  }
}

/* Add tiles to the topology from a NULL-terminated spec array.
   Only sets fields used by the layout algorithm. */
static void
make_tiles( fd_topo_t *         topo,
            tile_spec_t const * specs ) {
  topo->tile_cnt = 0UL;
  for( tile_spec_t const * s = specs; s->name; s++ ) {
    for( ulong i=0UL; i<s->cnt; i++ ) {
      FD_TEST( topo->tile_cnt < FD_TOPO_MAX_TILES );
      fd_topo_tile_t * tile = &topo->tiles[ topo->tile_cnt ];
      fd_memset( tile, 0, sizeof(*tile) );
      strncpy( tile->name, s->name, sizeof(tile->name) );
      tile->id      = topo->tile_cnt;
      tile->cpu_idx = ULONG_MAX;

      /* Compute kind_id = count of prior tiles with same name */
      ulong kind_id = 0UL;
      for( ulong j=0UL; j<topo->tile_cnt; j++ ) {
        if( !strcmp( topo->tiles[ j ].name, s->name ) ) kind_id++;
      }
      tile->kind_id = kind_id;
      topo->tile_cnt++;
    }
  }
}

/* Set the blocklist on the topology.
   blocklist[] is terminated by ULONG_MAX. */
static void
set_blocklist( fd_topo_t *   topo,
               ulong const * blocklist ) {
  topo->blocklist_cores_cnt = 0UL;
  for( ulong const * p = blocklist; *p != ULONG_MAX; p++ ) {
    topo->blocklist_cores_cpu_idx[ topo->blocklist_cores_cnt++ ] = *p;
  }
}

/* Build the per-CPU → label map from the layout result.
   Tiles get their tile name, the rest NULL. */
static void
build_cpu_map( fd_topo_t const * topo,
               ulong             cpu_cnt,
               char const **     out ) {
  for( ulong i=0UL; i<cpu_cnt; i++ ) out[ i ] = NULL;
  for( ulong i=0UL; i<topo->tile_cnt; i++ ) {
    ulong c = topo->tiles[ i ].cpu_idx;
    if( c != ULONG_MAX && c < cpu_cnt ) out[ c ] = topo->tiles[ i ].name;
  }
}

/* Print the actual layout for debugging / generating expected arrays. */
static void
print_layout( fd_topo_t const * topo,
              ulong             cpu_cnt ) {
  char const * map[ 1024 ];
  FD_TEST( cpu_cnt <= 1024UL );
  build_cpu_map( topo, cpu_cnt, map );
  for( ulong i=0UL; i<cpu_cnt; i++ ) {
    FD_LOG_NOTICE(( "  /* %3lu */ %s,", i, map[ i ] ? map[ i ] : "NULL" ));
  }
}

/* ---- Test runner ------------------------------------------------------- */

/* Verify the layout of tiles matches expected[].
   expected[i] = tile name  →  tile should be assigned to cpu i
   expected[i] = NULL       →  cpu unassigned (blocked / unused)
   Entries beyond expected_len must also be unassigned. */
static void
run_test( ulong                 physical_cores,
          tile_spec_t const *   tiles,
          ulong const *         blocklist,      /* ULONG_MAX terminated */
          ulong                 expected_len,
          char const * const *  expected ) {
  /* Build CPU topology */
  fd_topo_cpus_t cpus[1];
  make_cpus( cpus, physical_cores );
  ulong cpu_cnt = cpus->cpu_cnt;

  /* Build tile topology */
  static fd_topo_t _topo[1];   /* static – too large for stack */
  fd_topo_t * topo = _topo;
  fd_memset( topo, 0, sizeof(*topo) );
  make_tiles( topo, tiles );
  set_blocklist( topo, blocklist );

  /* Run the layout algorithm */
  fd_topob_auto_layout_cpus( topo, cpus );

  /* Build actual per-CPU map */
  char const * actual[ 1024 ];
  FD_TEST( cpu_cnt <= 1024UL );
  build_cpu_map( topo, cpu_cnt, actual );

  /* Compare against expected */
  int ok = 1;
  for( ulong i=0UL; i<cpu_cnt; i++ ) {
    char const * exp = (i < expected_len) ? expected[ i ] : NULL;
    char const * act = actual[ i ];
    int match;
    if( !exp && !act )      match = 1;
    else if( !exp || !act ) match = 0;
    else                    match = !strcmp( exp, act );
    if( !match ) {
      FD_LOG_WARNING(( "  cpu %3lu: expected %-8s got %-8s  MISMATCH",
                        i, exp ? exp : "NULL", act ? act : "NULL" ));
      ok = 0;
    }
  }
  if( !ok ) {
    FD_LOG_WARNING(( "  Full actual layout:" ));
    print_layout( topo, cpu_cnt );
  }
  FD_TEST( ok );
}

/* ======================================================================== */
/*  Tile definitions                                                        */
/* ======================================================================== */

/* --- Firedancer default (no snapshots, no rpc, no telemetry) -- */

static tile_spec_t const FIREDANCER_TILES[] = {
  /* floating tiles */
  { "netlnk", 1 },
  { "metric", 1 }, { "diag",   1 }, { "genesi", 1 }, { "ipecho", 1 },
  { "admin",  1 },
  /* ordered tiles (36) */
  { "net",    2 }, { "quic",   1 }, { "verify", 6 }, { "dedup",  1 },
  { "resolv", 1 }, { "pack",   1 }, { "execle", 2 }, { "poh",    1 },
  { "shred",  1 }, { "sign",   2 }, { "gui",    1 }, { "gossvf", 2 },
  { "gossip", 1 }, { "repair", 1 }, { "replay", 1 }, { "execrp",10 },
  { "txsend", 1 }, { "tower",  1 },
  { NULL, 0 }
};

/* ======================================================================== */
/*  Blocklists                                                              */
/* ======================================================================== */

/* Default "0h" blocklist for N physical cores:
   cores 0 and N are blocked.  Built per-test as a local array. */

#define BLOCKLIST_0H( N ) (ulong const[]){ 0, (N), ULONG_MAX }

/* ======================================================================== */
/*  Expected results                                                        */
/*                                                                          */
/*  For each test, expected[cpu] = tile name or __ (unused). */
/* ======================================================================== */

#define __ NULL

/* ---- Firedancer, skip-HT expected prefix (48/64/128 physical) ----------
   When enough physical cores, ALWAYS tiles on cores 1..21, then
   POST_START tiles on cores 22..36.  HT siblings all unassigned.          */

static char const * const FD_SKIP_HT[] = {
  /* phys cores 0-36 (ALWAYS 1-22, POST_START 23-36) */
  /*  0 */ __,        /*  1 */ "net",     /*  2 */ "net",     /*  3 */ "quic",
  /*  4 */ "verify",  /*  5 */ "verify",  /*  6 */ "verify",  /*  7 */ "verify",
  /*  8 */ "verify",  /*  9 */ "verify",  /* 10 */ "dedup",   /* 11 */ "resolv",
  /* 12 */ "pack",    /* 13 */ "sign",    /* 14 */ "sign",    /* 15 */ "shred",
  /* 16 */ "gui",     /* 17 */ "gossvf",  /* 18 */ "gossvf",  /* 19 */ "gossip",
  /* 20 */ "repair",  /* 21 */ "replay",  /* 22 */ "tower",   /* 23 */ "execle",
  /* 24 */ "execle",  /* 25 */ "poh",     /* 26 */ "execrp",  /* 27 */ "execrp",
  /* 28 */ "execrp",  /* 29 */ "execrp",  /* 30 */ "execrp",  /* 31 */ "execrp",
  /* 32 */ "execrp",  /* 33 */ "execrp",  /* 34 */ "execrp",  /* 35 */ "execrp",
  /* 36 */ "txsend",
};
#define FD_SKIP_HT_LEN (sizeof(FD_SKIP_HT)/sizeof(FD_SKIP_HT[0]))

/* ---- Firedancer 24×2  (no skip HT, 36 tiles > 23 physical cores) ------ */

static char const * const FD_24X2[] = {
  /*  0 */ __,        /*  1 */ "net",     /*  2 */ "quic",    /*  3 */ "verify",
  /*  4 */ "verify",  /*  5 */ "verify",  /*  6 */ "resolv",  /*  7 */ "pack",
  /*  8 */ "sign",    /*  9 */ "gui",     /* 10 */ "gossvf",  /* 11 */ "gossip",
  /* 12 */ "replay",  /* 13 */ "execle",  /* 14 */ "poh",     /* 15 */ "execrp",
  /* 16 */ "execrp",  /* 17 */ "execrp",  /* 18 */ "execrp",  /* 19 */ "execrp",
  /* 20 */ "txsend",  /* 21 */ __,        /* 22 */ __,        /* 23 */ __,
  /* --- HT siblings (24-47) --- */
  /* 24 */ __,        /* 25 */ "net",     /* 26 */ "verify",  /* 27 */ "verify",
  /* 28 */ "verify",  /* 29 */ "dedup",   /* 30 */ "sign",    /* 31 */ __,
  /* 32 */ "shred",   /* 33 */ __,        /* 34 */ "gossvf",  /* 35 */ "repair",
  /* 36 */ "tower",   /* 37 */ "execle",  /* 38 */ __,        /* 39 */ "execrp",
  /* 40 */ "execrp",  /* 41 */ "execrp",  /* 42 */ "execrp",  /* 43 */ "execrp",
  /* 44 */ __,        /* 45 */ __,        /* 46 */ __,        /* 47 */ __,
};
#define FD_24X2_LEN (sizeof(FD_24X2)/sizeof(FD_24X2[0]))

/* ---- Firedancer 32×2  (no skip HT, 36 tiles > 31 physical cores) ------ */

static char const * const FD_32X2[] = {
  /*  0 */ __,        /*  1 */ "net",     /*  2 */ "quic",    /*  3 */ "verify",
  /*  4 */ "verify",  /*  5 */ "verify",  /*  6 */ "resolv",  /*  7 */ "pack",
  /*  8 */ "sign",    /*  9 */ "gui",     /* 10 */ "gossvf",  /* 11 */ "gossip",
  /* 12 */ "replay",  /* 13 */ "execle",  /* 14 */ "poh",     /* 15 */ "execrp",
  /* 16 */ "execrp",  /* 17 */ "execrp",  /* 18 */ "execrp",  /* 19 */ "execrp",
  /* 20 */ "txsend",  /* 21 */ __,        /* 22 */ __,        /* 23 */ __,
  /* 24 */ __,        /* 25 */ __,        /* 26 */ __,        /* 27 */ __,
  /* 28 */ __,        /* 29 */ __,        /* 30 */ __,        /* 31 */ __,
  /* --- HT siblings (32-63) --- */
  /* 32 */ __,        /* 33 */ "net",     /* 34 */ "verify",  /* 35 */ "verify",
  /* 36 */ "verify",  /* 37 */ "dedup",   /* 38 */ "sign",    /* 39 */ __,
  /* 40 */ "shred",   /* 41 */ __,        /* 42 */ "gossvf",  /* 43 */ "repair",
  /* 44 */ "tower",   /* 45 */ "execle",  /* 46 */ __,        /* 47 */ "execrp",
  /* 48 */ "execrp",  /* 49 */ "execrp",  /* 50 */ "execrp",  /* 51 */ "execrp",
  /* 52 */ __,        /* 53 */ __,        /* 54 */ __,        /* 55 */ __,
  /* 56 */ __,        /* 57 */ __,        /* 58 */ __,        /* 59 */ __,
  /* 60 */ __,        /* 61 */ __,        /* 62 */ __,        /* 63 */ __,
};
#define FD_32X2_LEN (sizeof(FD_32X2)/sizeof(FD_32X2[0]))



/* ---- Variation: Firedancer 32×2, fewer tiles → skip HT -----------------
   verify=2, execrp=4  →  26 tiles total, 31 available ≥ 26              */

static tile_spec_t const FD_FEWER_TILES[] = {
  { "netlnk", 1 },
  { "metric", 1 }, { "diag",   1 }, { "genesi", 1 }, { "ipecho", 1 },
  { "admin",  1 },
  { "net",    2 }, { "quic",   1 }, { "verify", 2 }, { "dedup",  1 },
  { "resolv", 1 }, { "pack",   1 }, { "execle", 2 }, { "poh",    1 },
  { "shred",  1 }, { "sign",   2 }, { "gui",    1 }, { "gossvf", 2 },
  { "gossip", 1 }, { "repair", 1 }, { "replay", 1 }, { "execrp", 4 },
  { "txsend", 1 }, { "tower",  1 },
  { NULL, 0 }
};

static char const * const FD_32X2_FEWER[] = {
  /* skip_ht: ALWAYS on cores 1..18, POST_START on cores 19..26 */
  /*  0 */ __,        /*  1 */ "net",     /*  2 */ "net",     /*  3 */ "quic",
  /*  4 */ "verify",  /*  5 */ "verify",  /*  6 */ "dedup",   /*  7 */ "resolv",
  /*  8 */ "pack",    /*  9 */ "sign",    /* 10 */ "sign",    /* 11 */ "shred",
  /* 12 */ "gui",     /* 13 */ "gossvf",  /* 14 */ "gossvf",  /* 15 */ "gossip",
  /* 16 */ "repair",  /* 17 */ "replay",  /* 18 */ "tower",   /* 19 */ "execle",
  /* 20 */ "execle",  /* 21 */ "poh",     /* 22 */ "execrp",  /* 23 */ "execrp",
  /* 24 */ "execrp",  /* 25 */ "execrp",  /* 26 */ "txsend",
};
#define FD_32X2_FEWER_LEN (sizeof(FD_32X2_FEWER)/sizeof(FD_32X2_FEWER[0]))


/* ---- Variation: Firedancer 32×2, extra blocklist -----------------------
   blocklist = "0h,5h" → {0,32,5,37}. 29 phys avail. 36 tiles > 29.     */

static char const * const FD_32X2_EXTRA_BL[] = {
  /*  0 */ __,        /*  1 */ "net",     /*  2 */ "quic",    /*  3 */ "verify",
  /*  4 */ "verify",  /*  5 */ __,        /*  6 */ "verify",  /*  7 */ "resolv",
  /*  8 */ "pack",    /*  9 */ "sign",    /* 10 */ "gui",     /* 11 */ "gossvf",
  /* 12 */ "gossip",  /* 13 */ "replay",  /* 14 */ "execle",  /* 15 */ "poh",
  /* 16 */ "execrp",  /* 17 */ "execrp",  /* 18 */ "execrp",  /* 19 */ "execrp",
  /* 20 */ "execrp",  /* 21 */ "txsend",  /* 22 */ __,        /* 23 */ __,
  /* 24 */ __,        /* 25 */ __,        /* 26 */ __,        /* 27 */ __,
  /* 28 */ __,        /* 29 */ __,        /* 30 */ __,        /* 31 */ __,
  /* --- HT siblings (32-63) --- */
  /* 32 */ __,        /* 33 */ "net",     /* 34 */ "verify",  /* 35 */ "verify",
  /* 36 */ "verify",  /* 37 */ __,        /* 38 */ "dedup",   /* 39 */ "sign",
  /* 40 */ __,        /* 41 */ "shred",   /* 42 */ __,        /* 43 */ "gossvf",
  /* 44 */ "repair",  /* 45 */ "tower",   /* 46 */ "execle",  /* 47 */ __,
  /* 48 */ "execrp",  /* 49 */ "execrp",  /* 50 */ "execrp",  /* 51 */ "execrp",
  /* 52 */ "execrp",  /* 53 */ __,        /* 54 */ __,        /* 55 */ __,
  /* 56 */ __,        /* 57 */ __,        /* 58 */ __,        /* 59 */ __,
  /* 60 */ __,        /* 61 */ __,        /* 62 */ __,        /* 63 */ __,
};
#define FD_32X2_EXTRA_BL_LEN (sizeof(FD_32X2_EXTRA_BL)/sizeof(FD_32X2_EXTRA_BL[0]))

#undef __

/* ======================================================================== */
/*  Test functions                                                          */
/* ======================================================================== */

/* --- Firedancer ---------------------------------------------- */

FD_UNIT_TEST( test_firedancer_24x2 ) {
  run_test( 24, FIREDANCER_TILES, BLOCKLIST_0H( 24 ), FD_24X2_LEN, FD_24X2 );
}

FD_UNIT_TEST( test_firedancer_32x2 ) {
  run_test( 32, FIREDANCER_TILES, BLOCKLIST_0H( 32 ), FD_32X2_LEN, FD_32X2 );
}

FD_UNIT_TEST( test_firedancer_48x2 ) {
  run_test( 48, FIREDANCER_TILES, BLOCKLIST_0H( 48 ), FD_SKIP_HT_LEN, FD_SKIP_HT );
}

FD_UNIT_TEST( test_firedancer_64x2 ) {
  run_test( 64, FIREDANCER_TILES, BLOCKLIST_0H( 64 ), FD_SKIP_HT_LEN, FD_SKIP_HT );
}

FD_UNIT_TEST( test_firedancer_128x2 ) {
  run_test( 128, FIREDANCER_TILES, BLOCKLIST_0H( 128 ), FD_SKIP_HT_LEN, FD_SKIP_HT );
}

/* --- Variations on 32×2 ------------------------------------------------ */

FD_UNIT_TEST( test_firedancer_32x2_fewer_tiles ) {
  run_test( 32, FD_FEWER_TILES, BLOCKLIST_0H( 32 ),
            FD_32X2_FEWER_LEN, FD_32X2_FEWER );
}

FD_UNIT_TEST( test_firedancer_32x2_extra_blocklist ) {
  run_test( 32, FIREDANCER_TILES, (ulong[]){ 0, 32, 5, 37, ULONG_MAX },
            FD_32X2_EXTRA_BL_LEN, FD_32X2_EXTRA_BL );
}

static fd_topo_tile_t *
add_test_tile( fd_topo_t *  topo,
               char const * name,
               ulong        kind_id,
               ulong        cpu_idx ) {
  fd_topo_tile_t * tile = &topo->tiles[ topo->tile_cnt++ ];
  strncpy( tile->name, name, sizeof(tile->name) );
  tile->kind_id = kind_id;
  tile->cpu_idx = cpu_idx;
  return tile;
}

FD_UNIT_TEST( test_cpu_overlap_permitted ) {
  static fd_topo_t _topo[1];
  fd_topo_t * topo = _topo;
  fd_memset( topo, 0, sizeof(*topo) );

  add_test_tile( topo, "snapct", 0UL, 7UL );
  add_test_tile( topo, "execle", 0UL, 7UL );

  fd_topob_validate_cpu_overlaps( topo );
}

FD_UNIT_TEST( test_cpu_overlap_banned ) {
  pid_t pid = fork();
  FD_TEST( pid>=0 );

  if( pid==0 ) {
    fd_log_level_logfile_set( 6 );

    static fd_topo_t _topo[1];
    fd_topo_t * topo = _topo;
    fd_memset( topo, 0, sizeof(*topo) );

    add_test_tile( topo, "pack",  0UL, 9UL );
    add_test_tile( topo, "shred", 0UL, 9UL );

    fd_topob_validate_cpu_overlaps( topo );
    _exit( 0 );
  }

  int status = 0;
  FD_TEST( waitpid( pid, &status, 0 )==pid );
  FD_TEST( ( WIFEXITED( status ) && WEXITSTATUS( status )==1 ) ||
           ( WIFSIGNALED( status ) && WTERMSIG( status )==SIGABRT ) );
}

/* ======================================================================== */

int
main( int     argc,
      char ** argv ) {
  fd_boot( &argc, &argv );
  fd_unit_tests( argc, argv );
  FD_LOG_NOTICE(( "pass" ));
  fd_halt();
  return 0;
}
