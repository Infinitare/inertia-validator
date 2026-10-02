$(call add-hdrs,fd_grpc_codec.h)
$(call add-objs,fd_grpc_codec,fd_waltz)
$(call make-unit-test,test_grpc_codec,test_grpc_codec,fd_waltz fd_ballet fd_util)
$(call run-unit-test,test_grpc_codec)

ifdef FD_HAS_HOSTED
$(call add-hdrs,fd_grpc_client.h)
$(call add-objs,fd_grpc_client,fd_waltz)

$(call make-unit-test,test_grpc_client,test_grpc_client,fd_waltz fd_ballet fd_util,$(OPENSSL_LIBS))
$(call run-unit-test,test_grpc_client)

$(call add-hdrs,fd_grpc_server.h)
$(call add-objs,fd_grpc_server,fd_waltz)
$(call make-unit-test,test_grpc_server,test_grpc_server,fd_waltz fd_ballet fd_util)
$(call run-unit-test,test_grpc_server)

# Interactive server for driving contrib/test/grpc_smoke.py by hand.
# Loops forever, so it is built but never run as a test.
$(call make-unit-test,grpc_server_live,grpc_server_live,fd_waltz fd_ballet fd_util)

$(call make-fuzz-test,fuzz_grpc_codec,fuzz_grpc_codec,fd_waltz fd_ballet fd_util)
$(call make-fuzz-test,fuzz_grpc_h2_gen_req_hdr,fuzz_grpc_h2_gen_req_hdr,fd_waltz fd_ballet fd_util)
$(call make-fuzz-test,fuzz_grpc_client,fuzz_grpc_client,fd_waltz fd_ballet fd_util,$(OPENSSL_LIBS))
$(call make-fuzz-test,fuzz_grpc_actor,fuzz_grpc_actor,fd_waltz fd_ballet fd_util,$(OPENSSL_LIBS))
endif
