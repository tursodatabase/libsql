# Copyright (c) 2011 WorkWare Systems http://www.workware.net.au/
# All rights reserved

# @synopsis:
#
# The 'cc-db' module provides a knowledge-base of system idiosyncrasies.
# In general, this module can always be included.

use cc

options {}

# openbsd needs sys/types.h to detect some system headers
cc-include-needs sys/socket.h sys/types.h
cc-include-needs netinet/in.h sys/types.h
