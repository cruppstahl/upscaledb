
#include "../src/config.h"

#include "ham/hamsterdb_int.h"

struct Globals {
  static const char *opath(const char *filename) {
    return (filename);
  }

  static const char *ipath(const char *filename) {
    return (filename);
  }
};
