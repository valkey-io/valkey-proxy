#include "test_common.h"

/*
 * Drain n elements from a GEORADIUS/GEOSEARCH array response.
 * Each element may itself be an array (WITHDIST/WITHCOORD) or a plain string.
 */
static void drain_geo_results(int fd, int n) {
    char hdr[64];
    for (int i = 0; i < n; i++) {
        resp_next_str(fd, hdr, sizeof(hdr));
        if (hdr[0] == '*') {
            /* nested array: read its elements (names, distances, coords) */
            int m = atoi(hdr + 1);
            for (int j = 0; j < m; j++) {
                char tmp[128];
                resp_next_str(fd, tmp, sizeof(tmp));
                if (tmp[0] == '*') {
                    /* coordinate pair: read lon/lat */
                    int k = atoi(tmp + 1);
                    for (int l = 0; l < k; l++) {
                        char coord[64];
                        resp_next_str(fd, coord, sizeof(coord));
                    }
                }
            }
        }
        /* plain string: already consumed by resp_next_str */
    }
}

void run_geospatial_tests(const char *host, int port, int *pass, int *fail) {
    printf("\n=== Geospatial Tests ===\n");

    int fd = proxy_connect(host, port);
    if (fd < 0) {
        printf("  [FAIL] Cannot connect to proxy at %s:%d\n", host, port);
        (*fail)++;
        return;
    }

    char r[512];
    char r2[512];

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");

    /* ── test data ────────────────────────────────────────────────────────
     * tc:geo:cities  — plain key, used for basic functionality tests
     * {geo}:cities   — hash-tagged key, source for STORE cross-slot tests
     */
    proxy_cmd(fd, r, sizeof(r), 8, "GEOADD", "tc:geo:cities", "13.361389", "38.115556", "Palermo", "15.087269",
              "37.502669", "Catania");
    proxy_cmd(fd, r, sizeof(r), 8, "GEOADD", "tc:geo:cities", "13.583333", "37.316667", "Agrigento", "2.349014",
              "48.864716", "Paris");

    /* hash-tagged copy for STORE tests */
    proxy_cmd(fd, r, sizeof(r), 8, "GEOADD", "{geo}:cities", "13.361389", "38.115556", "Palermo", "15.087269",
              "37.502669", "Catania");
    proxy_cmd(fd, r, sizeof(r), 5, "GEOADD", "{geo}:cities", "13.583333", "37.316667", "Agrigento");

    /* ── GEOADD ──────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 5, "GEOADD", "tc:geo:add", "13.361389", "38.115556", "Palermo");
    CHECK(strcmp(r, ":1") == 0, "GEOADD: new member returns 1", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 5, "GEOADD", "tc:geo:add", "13.361389", "38.115556", "Palermo");
    CHECK(strcmp(r, ":0") == 0, "GEOADD: re-adding same member returns 0", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 8, "GEOADD", "tc:geo:add", "15.087269", "37.502669", "Catania", "13.583333",
              "37.316667", "Agrigento");
    CHECK(strcmp(r, ":2") == 0, "GEOADD: adding 2 new members returns 2", pass, fail);

    /* NX: do not update if member exists */
    proxy_cmd(fd, r, sizeof(r), 6, "GEOADD", "tc:geo:add", "NX", "0", "0", "Palermo");
    CHECK(strcmp(r, ":0") == 0, "GEOADD NX: existing member not added, returns 0", pass, fail);

    /* XX: update only existing members (returns count of added, not updated) */
    proxy_cmd(fd, r, sizeof(r), 6, "GEOADD", "tc:geo:add", "XX", "13.400000", "38.100000", "Palermo");
    CHECK(strcmp(r, ":0") == 0, "GEOADD XX: updating existing returns 0", pass, fail);

    /* CH: count both added and updated members */
    proxy_cmd(fd, r, sizeof(r), 9, "GEOADD", "tc:geo:add", "CH", "13.361389", "38.115556", "Palermo", "14.000000",
              "37.000000", "NewCity");
    CHECK(strcmp(r, ":2") == 0, "GEOADD CH: 1 updated + 1 new member = 2", pass, fail);

    /* ── GEODIST ──────────────────────────────────────────────────────────── */
    proxy_cmd(fd, r, sizeof(r), 4, "GEODIST", "tc:geo:cities", "Palermo", "Catania");
    {
        double d = atof(r);
        CHECK(d > 166000.0 && d < 167000.0, "GEODIST: Palermo-Catania ~166274 m (default unit)", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 5, "GEODIST", "tc:geo:cities", "Palermo", "Catania", "km");
    {
        double d = atof(r);
        CHECK(d > 166.0 && d < 167.0, "GEODIST km: Palermo-Catania ~166.2 km", pass, fail);
    }

    proxy_cmd(fd, r, sizeof(r), 5, "GEODIST", "tc:geo:cities", "Palermo", "NoExist", "km");
    CHECK(strcmp(r, "(nil)") == 0, "GEODIST: non-existent member returns nil", pass, fail);

    /* ── GEOHASH ──────────────────────────────────────────────────────────── */
    resp_send(fd, 3, "GEOHASH", "tc:geo:cities", "Palermo");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "GEOHASH: 1 member returns *1 array", pass, fail);
    {
        char h[64];
        resp_next_str(fd, h, sizeof(h));
        CHECK(strlen(h) == 11, "GEOHASH: Palermo hash is an 11-char geohash string", pass, fail);
    }

    resp_send(fd, 4, "GEOHASH", "tc:geo:cities", "Palermo", "Catania");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "GEOHASH: 2 members returns *2 array", pass, fail);
    {
        char h1[64], h2[64];
        resp_next_str(fd, h1, sizeof(h1));
        resp_next_str(fd, h2, sizeof(h2));
        CHECK(strlen(h1) == 11 && strlen(h2) == 11, "GEOHASH: both hashes are 11 chars", pass, fail);
        CHECK(strcmp(h1, h2) != 0, "GEOHASH: Palermo and Catania have different hashes", pass, fail);
    }

    resp_send(fd, 3, "GEOHASH", "tc:geo:cities", "NoExist");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "GEOHASH: non-existent member returns *1 array", pass, fail);
    {
        char h[64];
        resp_next_str(fd, h, sizeof(h));
        CHECK(strcmp(h, "(nil)") == 0, "GEOHASH: non-existent member element is nil", pass, fail);
    }

    /* ── GEOPOS ───────────────────────────────────────────────────────────── */
    resp_send(fd, 3, "GEOPOS", "tc:geo:cities", "Palermo");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "GEOPOS: 1 member returns *1 outer array", pass, fail);
    {
        char inner[32], lon[64], lat[64];
        resp_next_str(fd, inner, sizeof(inner));
        CHECK(strcmp(inner, "*2") == 0, "GEOPOS: inner element is *2 [lon, lat]", pass, fail);
        resp_next_str(fd, lon, sizeof(lon));
        resp_next_str(fd, lat, sizeof(lat));
        double lo = atof(lon), la = atof(lat);
        CHECK(lo > 13.36 && lo < 13.37, "GEOPOS: Palermo longitude ~13.36", pass, fail);
        CHECK(la > 38.11 && la < 38.12, "GEOPOS: Palermo latitude ~38.12", pass, fail);
    }

    resp_send(fd, 4, "GEOPOS", "tc:geo:cities", "Palermo", "NoExist");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "GEOPOS: 2-member query returns *2 outer array", pass, fail);
    {
        char inner[32];
        resp_next_str(fd, inner, sizeof(inner));
        CHECK(strcmp(inner, "*2") == 0, "GEOPOS: existing member element is *2", pass, fail);
        char lon[64], lat[64];
        resp_next_str(fd, lon, sizeof(lon));
        resp_next_str(fd, lat, sizeof(lat));

        resp_next_str(fd, inner, sizeof(inner));
        /* GEOPOS returns *-1 (null array) for non-existent members, not $-1 */
        CHECK(strcmp(inner, "*-1") == 0, "GEOPOS: non-existent member element is nil", pass, fail);
    }

    /* ── GEORADIUS ────────────────────────────────────────────────────────── */
    resp_send(fd, 6, "GEORADIUS", "tc:geo:cities", "15", "37", "200", "km");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 2, "GEORADIUS: >=2 cities within 200 km of (15,37)", pass, fail);
        int has_catania = resp_array_contains(fd, n, "Catania");
        CHECK(has_catania, "GEORADIUS: Catania is within 200 km", pass, fail);
    }

    resp_send(fd, 7, "GEORADIUS", "tc:geo:cities", "15", "37", "200", "km", "ASC");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 1, "GEORADIUS ASC: at least 1 result", pass, fail);
        char first[64];
        resp_next_str(fd, first, sizeof(first));
        CHECK(strcmp(first, "Catania") == 0, "GEORADIUS ASC: Catania is closest to (15,37)", pass, fail);
        drain_geo_results(fd, n - 1);
    }

    resp_send(fd, 8, "GEORADIUS", "tc:geo:cities", "15", "37", "200", "km", "COUNT", "2");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*2") == 0, "GEORADIUS COUNT 2: returns *2 array", pass, fail);
    drain_geo_results(fd, 2);

    /* STORE to same-slot key */
    proxy_cmd(fd, r, sizeof(r), 8, "GEORADIUS", "{geo}:cities", "15", "37", "200", "km", "STORE",
              "{geo}:radius_result");
    CHECK(r[0] == ':' && atoi(r + 1) >= 0, "GEORADIUS STORE same slot: returns integer count", pass, fail);

    /* STORE to different-slot key → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 8, "GEORADIUS", "{geo}:cities", "15", "37", "200", "km", "STORE", "{other}:result");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "GEORADIUS STORE cross-slot: returns CROSSSLOT error", pass, fail);

    /* ── GEORADIUS_RO ─────────────────────────────────────────────────────── */
    resp_send(fd, 6, "GEORADIUS_RO", "tc:geo:cities", "15", "37", "200", "km");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 2, "GEORADIUS_RO: >=2 cities within 200 km", pass, fail);
        drain_geo_results(fd, n);
    }

    /* ── GEORADIUSBYMEMBER ────────────────────────────────────────────────── */
    resp_send(fd, 5, "GEORADIUSBYMEMBER", "tc:geo:cities", "Catania", "200", "km");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 2, "GEORADIUSBYMEMBER: >=2 cities within 200 km of Catania", pass, fail);
        int has_palermo = resp_array_contains(fd, n, "Palermo");
        CHECK(has_palermo, "GEORADIUSBYMEMBER: Palermo is within 200 km of Catania", pass, fail);
    }

    /* with WITHDIST ASC: verify Catania (self) comes first with distance 0 */
    resp_send(fd, 7, "GEORADIUSBYMEMBER", "tc:geo:cities", "Catania", "200", "km", "WITHDIST", "ASC");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 1, "GEORADIUSBYMEMBER WITHDIST: returns at least 1 result", pass, fail);
        for (int i = 0; i < n; i++) {
            char hdr[32], name[64], dist[64];
            resp_next_str(fd, hdr, sizeof(hdr)); /* *2 per result */
            resp_next_str(fd, name, sizeof(name));
            resp_next_str(fd, dist, sizeof(dist));
            if (i == 0) {
                CHECK(strcmp(name, "Catania") == 0, "GEORADIUSBYMEMBER WITHDIST ASC: Catania is first (dist=0)", pass,
                      fail);
                CHECK(atof(dist) < 0.001, "GEORADIUSBYMEMBER WITHDIST: Catania self-distance is 0", pass, fail);
            }
        }
    }

    /* STORE to same-slot key */
    proxy_cmd(fd, r, sizeof(r), 7, "GEORADIUSBYMEMBER", "{geo}:cities", "Catania", "200", "km", "STORE",
              "{geo}:bymember_result");
    CHECK(r[0] == ':' && atoi(r + 1) >= 0, "GEORADIUSBYMEMBER STORE same slot: returns integer count", pass, fail);

    /* STORE to different-slot key → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 7, "GEORADIUSBYMEMBER", "{geo}:cities", "Catania", "200", "km", "STORE",
              "{other}:bymember_result");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "GEORADIUSBYMEMBER STORE cross-slot: returns CROSSSLOT error", pass, fail);

    /* ── GEORADIUSBYMEMBER_RO ─────────────────────────────────────────────── */
    resp_send(fd, 5, "GEORADIUSBYMEMBER_RO", "tc:geo:cities", "Catania", "200", "km");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 2, "GEORADIUSBYMEMBER_RO: >=2 cities within 200 km of Catania", pass, fail);
        drain_geo_results(fd, n);
    }

    /* ── GEOSEARCH ────────────────────────────────────────────────────────── */

    /* FROMLONLAT BYRADIUS */
    resp_send(fd, 8, "GEOSEARCH", "tc:geo:cities", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 2, "GEOSEARCH FROMLONLAT BYRADIUS: >=2 results", pass, fail);
        int has_catania = resp_array_contains(fd, n, "Catania");
        CHECK(has_catania, "GEOSEARCH FROMLONLAT BYRADIUS: contains Catania", pass, fail);
    }

    /* FROMMEMBER BYRADIUS */
    resp_send(fd, 7, "GEOSEARCH", "tc:geo:cities", "FROMMEMBER", "Catania", "BYRADIUS", "200", "km");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 2, "GEOSEARCH FROMMEMBER BYRADIUS: >=2 results", pass, fail);
        int has_palermo = resp_array_contains(fd, n, "Palermo");
        CHECK(has_palermo, "GEOSEARCH FROMMEMBER: contains Palermo", pass, fail);
    }

    /* FROMLONLAT BYBOX */
    resp_send(fd, 9, "GEOSEARCH", "tc:geo:cities", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 3, "GEOSEARCH BYBOX 400x400 km: >=3 Sicilian cities in box", pass, fail);
        drain_geo_results(fd, n);
    }

    /* ASC ordering */
    resp_send(fd, 9, "GEOSEARCH", "tc:geo:cities", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC");
    resp_next_str(fd, r, sizeof(r));
    {
        int n = atoi(r + 1);
        CHECK(n >= 1, "GEOSEARCH ASC: at least 1 result", pass, fail);
        char first[64];
        resp_next_str(fd, first, sizeof(first));
        CHECK(strcmp(first, "Catania") == 0, "GEOSEARCH ASC: Catania is closest to (15,37)", pass, fail);
        drain_geo_results(fd, n - 1);
    }

    /* COUNT limit */
    resp_send(fd, 10, "GEOSEARCH", "tc:geo:cities", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "1");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "GEOSEARCH COUNT 1: returns *1 array", pass, fail);
    drain_geo_results(fd, 1);

    /* WITHDIST: each result is a *2 sub-array [name, distance] */
    resp_send(fd, 12, "GEOSEARCH", "tc:geo:cities", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC",
              "WITHDIST", "COUNT", "1");
    resp_next_str(fd, r, sizeof(r));
    CHECK(strcmp(r, "*1") == 0, "GEOSEARCH WITHDIST COUNT 1: returns *1 array", pass, fail);
    {
        resp_next_str(fd, r, sizeof(r)); /* *2 sub-array */
        CHECK(strcmp(r, "*2") == 0, "GEOSEARCH WITHDIST: each result is *2 sub-array", pass, fail);
        resp_next_str(fd, r, sizeof(r)); /* member name */
        CHECK(strcmp(r, "Catania") == 0, "GEOSEARCH WITHDIST ASC: closest member is Catania", pass, fail);
        resp_next_str(fd, r2, sizeof(r2)); /* distance string */
        {
            double d = atof(r2);
            CHECK(d > 50.0 && d < 70.0, "GEOSEARCH WITHDIST: Catania ~56 km from (15,37)", pass, fail);
        }
    }

    /* ── GEOSEARCHSTORE ───────────────────────────────────────────────────── */

    /* same-slot keys (both use hash tag {geo}) */
    proxy_cmd(fd, r, sizeof(r), 10, "GEOSEARCHSTORE", "{geo}:result", "{geo}:cities", "FROMLONLAT", "15", "37",
              "BYRADIUS", "200", "km", "ASC");
    CHECK(r[0] == ':' && atoi(r + 1) >= 0, "GEOSEARCHSTORE same slot: returns stored element count", pass, fail);

    /* different-slot keys: {geo} vs {other} → CROSSSLOT error */
    proxy_cmd(fd, r, sizeof(r), 10, "GEOSEARCHSTORE", "{other}:result", "{geo}:cities", "FROMLONLAT", "15", "37",
              "BYRADIUS", "200", "km", "ASC");
    CHECK(strstr(r, "CROSSSLOT") != NULL, "GEOSEARCHSTORE cross-slot: returns CROSSSLOT error", pass, fail);

    proxy_cmd(fd, r, sizeof(r), 1, "FLUSHDB");
    close(fd);
}
