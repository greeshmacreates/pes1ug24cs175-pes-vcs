// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // 1. Determine type string
    const char *type_str;
    if (type == OBJ_BLOB) type_str = "blob";
    else if (type == OBJ_TREE) type_str = "tree";
    else type_str = "commit";

    // 2. Create header "<type> <size>\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    // 3. Combine header + data
    size_t total_len = header_len + len;
    unsigned char *full = malloc(total_len);
    if (!full) return -1;

    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // 4. Compute SHA-256 hash
    compute_hash(full, total_len, id_out);

    // 5. If already exists → done (deduplication)
    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // 6. Get object path
    char path[512];
    object_path(id_out, path, sizeof(path));

    // 7. Create shard directory (.pes/objects/XX/)
    char dir[512];
    strncpy(dir, path, sizeof(dir));
    dir[sizeof(dir)-1] = '\0';

    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        mkdir(dir, 0755);
    }

    // 8. Write to file
    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full);
        return -1;
    }

    if (write(fd, full, total_len) != (ssize_t)total_len) {
        close(fd);
        free(full);
        return -1;
    }

    fsync(fd);
    close(fd);

    free(full);
    return 0;
}


int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;

    // Get file size
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return -1;
    }

    size_t size = st.st_size;
    unsigned char *buffer = malloc(size);
    if (!buffer) {
        close(fd);
        return -1;
    }

    if (read(fd, buffer, size) != (ssize_t)size) {
        close(fd);
        free(buffer);
        return -1;
    }

    close(fd);

    // 1. Verify hash integrity
    ObjectID check;
    compute_hash(buffer, size, &check);
    if (memcmp(check.hash, id->hash, HASH_SIZE) != 0) {
        free(buffer);
        return -1;
    }

    // 2. Find header/data split
    char *null = memchr(buffer, '\0', size);
    if (!null) {
        free(buffer);
        return -1;
    }

    // 3. Parse type
    if (strncmp((char *)buffer, "blob", 4) == 0)
        *type_out = OBJ_BLOB;
    else if (strncmp((char *)buffer, "tree", 4) == 0)
        *type_out = OBJ_TREE;
    else
        *type_out = OBJ_COMMIT;

    // 4. Extract data
    size_t data_len = size - (null - (char *)buffer) - 1;

    void *data = malloc(data_len);
    if (!data) {
        free(buffer);
        return -1;
    }

    memcpy(data, null + 1, data_len);

    *data_out = data;
    *len_out = data_len;

    free(buffer);
    return 0;
}