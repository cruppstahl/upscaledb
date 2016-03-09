/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
* Based on code from Saju Pillai (saju.pillai@gmail.com)
*      http://saju.net.in/code/misc/openssl_aes.c.txt
*
* @exception_safe: nothrow
* @thread_safe: no
*/

#ifndef UPS_AES_H
#define UPS_AES_H

#include "0root/root.h"

#include <algorithm>
#include <openssl/evp.h>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct AesCipher
{
  enum {
    kAesBlockSize = 16
  };

  AesCipher(const uint8_t key[kAesBlockSize], uint64_t salt = 0) {
    uint64_t iv[2] = {salt, 0};

    EVP_CIPHER_CTX_init(&encrypt_ctx_);
    EVP_EncryptInit_ex(&encrypt_ctx_, EVP_aes_128_cbc(), NULL, key,
                   (uint8_t *)&iv[0]);
    EVP_CIPHER_CTX_init(&decrypt_ctx_);
    EVP_DecryptInit_ex(&decrypt_ctx_, EVP_aes_128_cbc(), NULL, key,
                   (uint8_t *)&iv[0]);
    EVP_CIPHER_CTX_set_padding(&encrypt_ctx_, 0);
    EVP_CIPHER_CTX_set_padding(&decrypt_ctx_, 0);
  }

  ~AesCipher() {
    EVP_CIPHER_CTX_cleanup(&encrypt_ctx_);
    EVP_CIPHER_CTX_cleanup(&decrypt_ctx_);
  }

  /*
   * Encrypt |len| bytes of binary data in |plaintext|, stores the
   * encrypted data in |ciphertext|.
   *
   * The input data length must be aligned to the aes block size (16 bytes)!
   */
  void encrypt(const uint8_t *plaintext, uint8_t *ciphertext, size_t len) {
    assert(len % kAesBlockSize == 0);

    /* update ciphertext, c_len is filled with the length of ciphertext
     * generated, len is the size of plaintext in bytes */
    int clen = (int)len;
    EVP_EncryptUpdate(&encrypt_ctx_, ciphertext, &clen, plaintext, (int)len);

    /* update ciphertext with the final remaining bytes */
    int outlen;
    EVP_EncryptFinal(&encrypt_ctx_, ciphertext + clen, &outlen);
  }

  /*
   * Decrypts |len| bytes of |ciphertext|, stores the decoded data in
   * |plaintext|.
   *
   * The input data length must be aligned to the aes block size (16 bytes)!
   */
  void decrypt(const uint8_t *ciphertext, uint8_t *plaintext, size_t len) {
    assert(len % kAesBlockSize == 0);

    int plen = (int)len, flen = 0;
    EVP_DecryptUpdate(&decrypt_ctx_, plaintext, &plen, ciphertext, (int)len);
    EVP_DecryptFinal(&decrypt_ctx_, plaintext + plen, &flen);
  }

  EVP_CIPHER_CTX encrypt_ctx_;
  EVP_CIPHER_CTX decrypt_ctx_;
};

} // namespace upscaledb

#endif // UPS_AES_H
