# Vendored in-tree (src/third_party/blst).  Kept as a separate trailing
# archive rather than folded into libfd_ballet.a, matching upstream's
# link order.
BLST_LIBS=$(OBJDIR)/lib/libfd_blst.a
VENDOR_LINK_LIBS+=fd_blst
