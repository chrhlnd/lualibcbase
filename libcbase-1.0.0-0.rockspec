package = "libcbase"
version = "1.0.0-0"
source = {
  url = "git://github.com/chrhlnd/lualibcbase",
  tag = "v1.0.0"
}
description = {
  summary = "Lua binding to libcouchbase.",
  license = "Apache"
}
dependencies = {
  "lua ~> 5.1"
}
external_dependencies = {
  COUCHBASE = {
    header = "libcouchbase/couchbase.h"
  },
  EVENT = {
    header = "event.h"
  }
}
build = {
  type = "builtin",
  modules = {
    libcbase = {
      sources = {
        "lualibcbase.c"
      },
      defines = {
      },
      libraries = {
        "couchbase",
        "event"
      },
      incdirs = {
        "$(COUCHBASE_INCDIR)",
        "$(EVENT_INCDIR)"
      },
      libdirs = {
        "$(COUCHBASE_LIBDIR)",
        "$(EVENT_LIBDIR)"
      }
    }
  }
}
