#include <stdint.h>

struct structured_value {
    struct slot {
        int key_length;
        int value_length;
        uint8_t key[16];
        uint8_t value[16];
    };
    int nkvs;
    struct slot slots[64];
};

int i = sizeof(struct structured_value);