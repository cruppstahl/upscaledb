#include <sparsemap.h>
#include <assert.h>
#include <stdio.h>

using namespace sparsemap;

int main() {
  int size = 4;
#if 1
  uint8_t buffer[1024];
  uint8_t buffer2[1024];

  SparseMap<uint32_t, uint64_t> sm;
  sm.create(buffer, sizeof(buffer));
  assert(sm.get_size() == size);
  sm.set(0, true);
  assert(sm.get_size() == size + 4 + 8 + 8);
  assert(sm.is_set(0) == true);
  assert(sm.get_size() == size + 4 + 8 + 8);
  assert(sm.is_set(1) == false);
  sm.set(0, false);
  assert(sm.get_size() == size);

  sm.clear();
  sm.set(64, true);
  assert(sm.is_set(64) == true);
  assert(sm.get_size() == size + 4 + 8 + 8);

  sm.clear();

  // set [0..100000]
  for (int i = 0; i < 100000; i++) {
    assert(sm.is_set(i) == false);
    sm.set(i, true);
    if (i > 5) {
      for (int j = i - 5; j <= i; j++)
        assert(sm.is_set(j) == true);
    }

    assert(sm.is_set(i) == true);
  }

  for (int i = 0; i < 100000; i++)
    assert(sm.is_set(i) == true);

  // unset [0..10000]
  for (int i = 0; i < 10000; i++) {
    assert(sm.is_set(i) == true);
    sm.set(i, false);
    assert(sm.is_set(i) == false);
  }

  for (int i = 0; i < 10000; i++)
    assert(sm.is_set(i) == false);

  sm.clear();

  // set [10000..0]
  for (int i = 10000; i >= 0; i--) {
    assert(sm.is_set(i) == false);
    sm.set(i, true);
    assert(sm.is_set(i) == true);
  }

  for (int i = 10000; i >= 0; i--)
    assert(sm.is_set(i) == true);

  // open and compare
  SparseMap<uint32_t, uint64_t> sm2;
  sm2.open(buffer, sizeof(buffer));
  for (int i = 0; i < 10000; i++)
    assert(sm2.is_set(i) == sm.is_set(i));

  // unset [10000..0]
  for (int i = 10000; i >= 0; i--) {
    assert(sm.is_set(i) == true);
    sm.set(i, false);
    assert(sm.is_set(i) == false);
  }

  for (int i = 10000; i >= 0; i--)
    assert(sm.is_set(i) == false);

  sm.clear();

  sm.set(0, true);
  sm.set(2048 * 2 + 1, true);
  assert(sm.is_set(0) == true);
  assert(sm.is_set(2048 * 2 + 0) == false);
  assert(sm.is_set(2048 * 2 + 1) == true);
  assert(sm.is_set(2048 * 2 + 2) == false);
  sm.set(2048, true);
  assert(sm.is_set(0) == true);
  assert(sm.is_set(2047) == false);
  assert(sm.is_set(2048) == true);
  assert(sm.is_set(2049) == false);
  assert(sm.is_set(2048 * 2 + 2) == false);
  assert(sm.is_set(2048 * 2 + 0) == false);
  assert(sm.is_set(2048 * 2 + 1) == true);
  assert(sm.is_set(2048 * 2 + 2) == false);

  sm.clear();

  for (int i = 0; i < 100000; i++)
    sm.set(i, true);
  for (int i = 0; i < 100000; i++)
    assert(sm.select(i) == (unsigned)i);

  sm.clear();

  for (int i = 1; i < 513; i++)
    sm.set(i, true);
  for (int i = 1; i < 513; i++)
    assert(sm.select(i - 1) == (unsigned)i);

  sm.clear();

  for (int i = 0; i < 8; i++)
    sm.set(i * 10, true);
  for (int i = 0; i < 8; i++)
    assert(sm.select(i) == (unsigned)i * 10);

  // split and move, aligned to MiniMap capacity
  sm2.create(buffer2, sizeof(buffer2));
  sm.clear();
  for (int i = 0; i < 2048 * 2; i++)
    sm.set(i, true);
  sm.split(2048, &sm2);
  for (int i = 0; i < 2048; i++) {
    assert(sm.is_set(i) == true);
    assert(sm2.is_set(i) == false);
  }
  for (int i = 2048; i < 2048 * 2; i++) {
    assert(sm.is_set(i) == false);
    assert(sm2.is_set(i) == true);
  }

  // split and move, aligned to BitVector capacity
  sm2.create(buffer2, sizeof(buffer2));
  sm.clear();
  for (int i = 0; i < 2048 * 3; i++)
    sm.set(i, true);
  sm.split(64, &sm2);
  for (int i = 0; i < 64; i++) {
    assert(sm.is_set(i) == true);
    assert(sm2.is_set(i) == false);
  }
  for (int i = 64; i < 2048 * 3; i++) {
    assert(sm.is_set(i) == false);
    assert(sm2.is_set(i) == true);
  }

  printf("ok\n");
#else
  //
  // This code was used to create the lookup table for
  // sparsemap::MiniMap<>::calc_vector_size()
  //
  printf("   ");
  for (unsigned int ch = 0; ch <= 0xff; ch++) {
    if (ch > 0 && ch % 16 == 0)
      printf("\n   ");

    /*
    // check if value is invalid (contains 2#01)
    if ((ch & (0x3 << 0)) >> 0 == 1
        || (ch & (0x3 << 2)) >> 2 == 1
        || (ch & (0x3 << 4)) >> 4 == 1
        || (ch & (0x3 << 6)) >> 6 == 1) {
      //printf("%d: -1\n", (int)ch);
      printf(" -1,");
      continue;
    }
    */

    // count all occurrences of 2#10
    int size = 0;
    if ((ch & (0x3 << 0)) >> 0 == 2)
      size++;
    if ((ch & (0x3 << 2)) >> 2 == 2)
      size++;
    if ((ch & (0x3 << 4)) >> 4 == 2)
      size++;
    if ((ch & (0x3 << 6)) >> 6 == 2)
      size++;
    //printf("%u: %d\n", (unsigned int)ch, size);
    printf("  %d,", size);
  }
#endif
}
