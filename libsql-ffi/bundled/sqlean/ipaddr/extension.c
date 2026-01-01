// Copyright (c) 2021 Vincent Bernat, MIT License
// https://github.com/nalgeon/sqlean

// IP address manipulation in SQLite.

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __FreeBSD__
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#endif

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

struct ipaddress {
    int af;
    union {
        struct in6_addr ipv6;
        struct in_addr ipv4;
    };
    unsigned masklen;
};

static struct ipaddress* parse_ipaddress(const char* address) {
    struct ipaddress* ip = NULL;
    unsigned char buf[sizeof(struct in6_addr)];
    char* sep = strchr(address, '/');
    unsigned long masklen = 0;
    if (sep) {
        char* end;
        errno = 0;
        masklen = strtoul(sep + 1, &end, 10);
        if (errno != 0 || sep + 1 == end || *end != '\0')
            return NULL;
        *sep = '\0';
    }
    if (inet_pton(AF_INET, address, buf)) {
        if (sep && masklen > 32)
            goto end;

        ip = sqlite3_malloc(sizeof(struct ipaddress));
        memcpy(&ip->ipv4, buf, sizeof(struct in_addr));
        ip->af = AF_INET;
        ip->masklen = sep ? masklen : 32;
    } else if (inet_pton(AF_INET6, address, buf)) {
        if (sep && masklen > 128)
            goto end;

        ip = sqlite3_malloc(sizeof(struct ipaddress));
        memcpy(&ip->ipv6, buf, sizeof(struct in6_addr));
        ip->af = AF_INET6;
        ip->masklen = sep ? masklen : 128;
    }
end:
    if (sep)
        *sep = '/';
    return ip;
}

static void ipaddr_ipfamily(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    const char* address = (char*)sqlite3_value_text(argv[0]);
    struct ipaddress* ip = parse_ipaddress(address);
    if (ip == NULL) {
        sqlite3_result_null(context);
        return;
    }
    sqlite3_result_int(context, ip->af == AF_INET ? 4 : 6);
    sqlite3_free(ip);
}

static void ipaddr_iphost(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    const char* address = (char*)sqlite3_value_text(argv[0]);
    struct ipaddress* ip = parse_ipaddress(address);
    if (ip == NULL) {
        sqlite3_result_null(context);
        return;
    }
    if (ip->af == AF_INET) {
        char* result = sqlite3_malloc(INET_ADDRSTRLEN);
        inet_ntop(AF_INET, &ip->ipv4, result, INET_ADDRSTRLEN);
        sqlite3_result_text(context, result, -1, sqlite3_free);
    } else if (ip->af == AF_INET6) {
        char* result = sqlite3_malloc(INET6_ADDRSTRLEN);
        inet_ntop(AF_INET6, &ip->ipv6, result, INET6_ADDRSTRLEN);
        sqlite3_result_text(context, result, -1, sqlite3_free);
    }
    sqlite3_free(ip);
}

static void ipaddr_ipmasklen(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    const char* address = (char*)sqlite3_value_text(argv[0]);
    struct ipaddress* ip = parse_ipaddress(address);
    if (ip == NULL) {
        sqlite3_result_null(context);
        return;
    }
    sqlite3_result_int(context, ip->masklen);
    return;
}

static void ipaddr_ipnetwork(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 1);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    const char* address = (char*)sqlite3_value_text(argv[0]);
    struct ipaddress* ip = parse_ipaddress(address);
    if (ip == NULL) {
        sqlite3_result_null(context);
        return;
    }
    if (ip->af == AF_INET) {
        char buf[INET_ADDRSTRLEN];
        ip->ipv4.s_addr =
            htonl(ntohl(ip->ipv4.s_addr) & ~(uint32_t)((1ULL << (32 - ip->masklen)) - 1));
        inet_ntop(AF_INET, &ip->ipv4, buf, INET_ADDRSTRLEN);
        char* result = sqlite3_malloc(INET_ADDRSTRLEN + 3);
        sprintf(result, "%s/%u", buf, ip->masklen);
        sqlite3_result_text(context, result, -1, sqlite3_free);
    } else if (ip->af == AF_INET6) {
        char buf[INET6_ADDRSTRLEN];
        for (unsigned i = 0; i < 16; i++) {
            if (ip->masklen / 8 < i)
                ip->ipv6.s6_addr[i] = 0;
            else if (ip->masklen / 8 == i)
                ip->ipv6.s6_addr[i] &= ~(ip->masklen % 8);
        }
        inet_ntop(AF_INET6, &ip->ipv6, buf, INET6_ADDRSTRLEN);
        char* result = sqlite3_malloc(INET6_ADDRSTRLEN + 4);
        sprintf(result, "%s/%u", buf, ip->masklen);
        sqlite3_result_text(context, result, -1, sqlite3_free);
    }
    sqlite3_free(ip);
}

static void ipaddr_ipcontains(sqlite3_context* context, int argc, sqlite3_value** argv) {
    assert(argc == 2);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }

    const char* address1 = (char*)sqlite3_value_text(argv[0]);
    struct ipaddress* ip1 = parse_ipaddress(address1);
    const char* address2 = (char*)sqlite3_value_text(argv[1]);
    struct ipaddress* ip2 = parse_ipaddress(address2);
    if (ip1 == NULL || ip2 == NULL) {
        sqlite3_result_null(context);
        goto end;
    }
    if (ip1->af != ip2->af || ip1->masklen > ip2->masklen) {
        sqlite3_result_int(context, 0);
        goto end;
    }

    if (ip1->af == AF_INET) {
        ip1->ipv4.s_addr =
            htonl(ntohl(ip1->ipv4.s_addr) & ~(uint32_t)((1ULL << (32 - ip1->masklen)) - 1));
        ip2->ipv4.s_addr =
            htonl(ntohl(ip2->ipv4.s_addr) & ~(uint32_t)((1ULL << (32 - ip1->masklen)) - 1));
        sqlite3_result_int(context, ip1->ipv4.s_addr == ip2->ipv4.s_addr);
        goto end;
    }
    if (ip1->af == AF_INET6) {
        for (unsigned i = 0; i < 16; i++) {
            if (ip1->masklen / 8 < i) {
                ip1->ipv6.s6_addr[i] = 0;
                ip2->ipv6.s6_addr[i] = 0;
            } else if (ip1->masklen / 8 == i) {
                ip1->ipv6.s6_addr[i] &= ~(ip1->masklen % 8);
                ip2->ipv6.s6_addr[i] &= ~(ip1->masklen % 8);
            }
            if (ip1->ipv6.s6_addr[i] != ip2->ipv6.s6_addr[i]) {
                sqlite3_result_int(context, 0);
                goto end;
            }
        }
        sqlite3_result_int(context, 1);
    }
end:
    sqlite3_free(ip1);
    sqlite3_free(ip2);
}

int ipaddr_init(sqlite3* db) {
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;
    sqlite3_create_function(db, "ipfamily", 1, flags, 0, ipaddr_ipfamily, 0, 0);
    sqlite3_create_function(db, "iphost", 1, flags, 0, ipaddr_iphost, 0, 0);
    sqlite3_create_function(db, "ipmasklen", 1, flags, 0, ipaddr_ipmasklen, 0, 0);
    sqlite3_create_function(db, "ipnetwork", 1, flags, 0, ipaddr_ipnetwork, 0, 0);
    sqlite3_create_function(db, "ipcontains", 2, flags, 0, ipaddr_ipcontains, 0, 0);
    return SQLITE_OK;
}
