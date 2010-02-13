/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include "db.h"
#include "env.h"

ham_u16_t
env_get_max_databases(ham_env_t *env)
{
    env_header_t *hdr=(env_header_t*)(page_get_payload(env_get_header_page(env)));
    return (ham_db2h16(hdr->_max_databases));
}

ham_u8_t
env_get_version(ham_env_t *env, ham_size_t idx)
{
    env_header_t *hdr=(env_header_t*)(page_get_payload(env_get_header_page(env)));
    return (envheader_get_version(hdr, idx));
}

ham_u32_t
env_get_serialno(ham_env_t *env)
{
    env_header_t *hdr=(env_header_t*)(page_get_payload(env_get_header_page(env)));
    return (ham_db2h32(hdr->_serialno));
}

void
env_set_serialno(ham_env_t *env, ham_u32_t n)
{
    env_header_t *hdr=(env_header_t*)(page_get_payload(env_get_header_page(env)));
    hdr->_serialno=ham_h2db32(n);
}

env_header_t *
env_get_header(ham_env_t *env)
{
    return ((env_header_t*)(page_get_payload(env_get_header_page(env))));
}
