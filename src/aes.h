/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/*
 * Based on code from Saju Pillai (saju.pillai@gmail.com)
 *      http://saju.net.in/code/misc/openssl_aes.c.txt
 */

#ifndef HAM_AES_H__
#define HAM_AES_H__

#include <algorithm>
#include <openssl/evp.h>

#include "error.h"
#include "endianswap.h"

class AesCipher {
  enum {
    kAesBlockSize = 16
  };

  public:
    AesCipher(const ham_u8_t key[kAesBlockSize], ham_u64_t salt = 0) {
      ham_u64_t iv[2];
      memset(iv, 0, sizeof(iv));
      iv[0] = ham_h2db64(salt);
  
      EVP_CIPHER_CTX_init(&m_encrypt_ctx);
      EVP_EncryptInit_ex(&m_encrypt_ctx, EVP_aes_128_cbc(), NULL, key,
              (ham_u8_t *)&iv[0]);
      EVP_CIPHER_CTX_init(&m_decrypt_ctx);
      EVP_DecryptInit_ex(&m_decrypt_ctx, EVP_aes_128_cbc(), NULL, key,
              (ham_u8_t *)&iv[0]);
      EVP_CIPHER_CTX_set_padding(&m_encrypt_ctx, 0);
      EVP_CIPHER_CTX_set_padding(&m_decrypt_ctx, 0);
    }

    ~AesCipher() {
      EVP_CIPHER_CTX_cleanup(&m_encrypt_ctx);
      EVP_CIPHER_CTX_cleanup(&m_decrypt_ctx);
    }

    /*
     * Encrypt len bytes of binary data
     */
    void encrypt(const ham_u8_t *plaintext, ham_u8_t *ciphertext, int len) {
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
     * Decrypt len bytes of ciphertext
     */
    void decrypt(const ham_u8_t *ciphertext, ham_u8_t *plaintext, int len) {
      ham_assert(len % kAesBlockSize == 0);

      int plen = len, flen = 0;
  
      EVP_DecryptUpdate(&m_decrypt_ctx, plaintext, &plen, ciphertext, len);
      EVP_DecryptFinal(&m_decrypt_ctx, plaintext + plen, &flen);
    }

  private:
    EVP_CIPHER_CTX m_encrypt_ctx;
    EVP_CIPHER_CTX m_decrypt_ctx;
};

#endif // HAM_AES_H__
