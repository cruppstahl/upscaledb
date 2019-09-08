/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    encrypt_ctx_ = EVP_CIPHER_CTX_new();
    EVP_EncryptInit_ex(encrypt_ctx_, EVP_aes_128_cbc(), NULL, key,
                   (uint8_t *)&iv[0]);
    decrypt_ctx_ = EVP_CIPHER_CTX_new();
    EVP_DecryptInit_ex(decrypt_ctx_, EVP_aes_128_cbc(), NULL, key,
                   (uint8_t *)&iv[0]);

    EVP_CIPHER_CTX_set_padding(encrypt_ctx_, 0);
    EVP_CIPHER_CTX_set_padding(decrypt_ctx_, 0);
  }

  ~AesCipher() {
    EVP_CIPHER_CTX_cleanup(encrypt_ctx_);
    EVP_CIPHER_CTX_cleanup(decrypt_ctx_);
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
    EVP_EncryptUpdate(encrypt_ctx_, ciphertext, &clen, plaintext, (int)len);

    /* update ciphertext with the final remaining bytes */
    int outlen;
    EVP_EncryptFinal(encrypt_ctx_, ciphertext + clen, &outlen);
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
    EVP_DecryptUpdate(decrypt_ctx_, plaintext, &plen, ciphertext, (int)len);
    EVP_DecryptFinal(decrypt_ctx_, plaintext + plen, &flen);
  }

  EVP_CIPHER_CTX *encrypt_ctx_;
  EVP_CIPHER_CTX *decrypt_ctx_;
};

} // namespace upscaledb

#endif // UPS_AES_H
