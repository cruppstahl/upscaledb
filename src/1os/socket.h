/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * A simple wrapper around a tcp socket handle. Throws exceptions in
 * case of errors
 *
 * @exception_safe: basic
 * @thread_safe: unknown
 */

#ifndef HAM_SOCKET_H
#define HAM_SOCKET_H

#include "0root/root.h"

#include <stdio.h>
#include <limits.h>

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/os.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Socket
{
  public:
    // Constructor creates an empty socket
    Socket()
      : m_socket(HAM_INVALID_FD) {
    }

    // Destructor closes the socket
    ~Socket() {
      close();
    }

    // Connects to a remote host
    void connect(const char *hostname, uint16_t port, uint32_t timeout_sec);

    // Sends data to the connected server
    void send(const uint8_t *data, size_t len);

    // Receives data from the connected server; blocking!
    void recv(uint8_t *data, size_t len);

    // Closes the connection; no problem if socket was already closed
    void close();

  private:
    ham_socket_t m_socket;
};

} // namespace hamsterdb

#endif /* HAM_SOCKET_H */
