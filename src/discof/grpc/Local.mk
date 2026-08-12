ifdef FD_HAS_HOSTED
$(call add-objs,fd_grpc_tile,fd_discof)
$(call make-unit-test,test_grpc_tile,test_grpc_tile,fd_discof firedancer_version fd_disco fd_flamenco fd_funk fd_waltz fd_tango fd_ballet fd_util)
endif