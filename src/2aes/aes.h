/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * Based on code from Saju Pillai (saju.pillai@gmail.com)
 *      http://saju.net.in/code/misc/openssl_aes.c.txt
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_AES_H
#define HAM_AES_H

#include "0root/root.h"

#include <algorithm>
#include <openssl/evp.h>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class AesCipher {
  enum {
    kAesBlockSize = 16
  };

  public:
    AesCipher(const uint8_t key[kAesBlockSize], uint64_t salt = 0) {
      uint64_t iv[2] = {salt, 0};
  
      EVP_CIPHER_CTX_init(&m_encrypt_ctx);
      EVP_EncryptInit_ex(&m_encrypt_ctx, EVP_aes_128_cbc(), NULL, key,
              (uint8_t *)&iv[0]);
      EVP_CIPHER_CTX_init(&m_decrypt_ctx);
      EVP_DecryptInit_ex(&m_decrypt_ctx, EVP_aes_128_cbc(), NULL, key,
              (uint8_t *)&iv[0]);
      EVP_CIPHER_CTX_set_padding(&m_encrypt_ctx, 0);
      EVP_CIPHER_CTX_set_padding(&m_decrypt_ctx, 0);
    }

    ~AesCipher() {
      EVP_CIPHER_CTX_cleanup(&m_encrypt_ctx);
      EVP_CIPHER_CTX_cleanup(&m_decrypt_ctx);
    }

    /*
     * Encrypt |len| bytes of binary data in |plaintext|, stores the
     * encrypted data in |ciphertext|.
     *
     * The input data length must be aligned to the aes block size (16 bytes)!
     */
    void encrypt(const uint8_t *plaintext, uint8_t *ciphertext, int len) {
      ham_assert(len % kAesBlockSize == 0);

      /* update ciphertext, c_len is filled with the length of ciphertext
       * generated, len is the size of plaintext in bytes */
      int clen = len;
      EVP_EncryptUpdate(&m_encrypt_ctx, ciphertext, &clen, plaintext, len);

      /* update ciphertext with the final remaining bytes */
      int outlen;
      EVP_EncryptFinal(&m_encrypt_ctx, ciphertext + clen, &outlen);
    }

    /*
     * Decrypts |len| bytes of |ciphertext|, stores the decoded data in
     * |plaintext|.
     *
     * The input data length must be aligned to the aes block size (16 bytes)!
     */
    void decrypt(const uint8_t *ciphertext, uint8_t *plaintext, int len) {
      ham_assert(len % kAesBlockSize == 0);

      int plen = len, flen = 0;
  
      EVP_DecryptUpdate(&m_decrypt_ctx, plaintext, &plen, ciphertext, len);
      EVP_DecryptFinal(&m_decrypt_ctx, plaintext + plen, &flen);
    }

  private:
    EVP_CIPHER_CTX m_encrypt_ctx;
    EVP_CIPHER_CTX m_decrypt_ctx;
};

} // namespace hamsterdb

#endif // HAM_AES_H
