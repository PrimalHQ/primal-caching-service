#include <stdint.h>
#include <stddef.h>
#include <string.h>

#include "segwit_addr.c"

int nostr_bech32_decode(uint8_t* outdata, size_t* outdata_len, const char *hrp, const char* input) {
    uint8_t data[84];
    char hrp_actual[84];
    size_t data_len;
    bech32_encoding enc = bech32_decode(hrp_actual, data, &data_len, input);
    if (enc == BECH32_ENCODING_NONE) return 0;
    if (data_len == 0 || data_len > 65) return 0;
    if (strncmp(hrp, hrp_actual, 84) != 0) return 0;
    /* if (data[0] > 16) return 0; */
    /* if (data[0] == 0 && enc != BECH32_ENCODING_BECH32) return 0; */
    /* if (data[0] > 0 && enc != BECH32_ENCODING_BECH32M) return 0; */
    *outdata_len = 0;
    if (!convert_bits(outdata, outdata_len, 8, data, data_len, 5, 0)) return 0;
    if (*outdata_len < 2 || *outdata_len > 40) return 0;
    /* if (data[0] == 0 && *outdata_len != 20 && *outdata_len != 32) return 0; */
    return 1;
}

int nostr_bech32_encode(char *output, const char *hrp, int witver, const uint8_t *witprog, size_t witprog_len) {
    uint8_t data[65];
    size_t datalen = 0;
    bech32_encoding enc = BECH32_ENCODING_BECH32;
    if (witver > 16) return 0;
    if (witver == 0 && witprog_len != 20 && witprog_len != 32) return 0;
    if (witprog_len < 2 || witprog_len > 40) return 0;
    if (witver > 0) enc = BECH32_ENCODING_BECH32M;
    /* data[0] = witver; */
    /* convert_bits(data + 1, &datalen, 5, witprog, witprog_len, 8, 1); */
    convert_bits(data, &datalen, 5, witprog, witprog_len, 8, 1);
    /* ++datalen; */
    return bech32_encode(output, hrp, data, datalen, enc);
}

