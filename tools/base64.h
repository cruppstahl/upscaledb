
/**
 * base64 encoding/decoding from ryyst
 * http://stackoverflow.com/questions/342409/how-do-i-base64-encode-decode-in-c
 */

#include <math.h>
#include <stdint.h>
#include <stdlib.h>


static char encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                                'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                                'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                                'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
                                'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
                                'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                                'w', 'x', 'y', 'z', '0', '1', '2', '3',
                                '4', '5', '6', '7', '8', '9', '+', '/'};
static int mod_table[] = {0, 2, 1};


class Base64Encoder {
  public:
    Base64Encoder() {
      for (int i = 0; i < 0x40; i++)
        decoding_table[(int)encoding_table[i]] = i;
    }

    char *encode(const unsigned char *data, size_t input_length,
                size_t *output_length) {
      *output_length = (size_t) (4.0 * ceil((double) input_length / 3.0)) + 1;

      char *encoded_data = (char *)malloc(*output_length + 1);
      if (encoded_data == NULL) return NULL;

      for (unsigned i = 0, j = 0; i < input_length;) {
        uint32_t octet_a = i < input_length ? data[i++] : 0;
        uint32_t octet_b = i < input_length ? data[i++] : 0;
        uint32_t octet_c = i < input_length ? data[i++] : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
      }

      for (int i = 0; i < mod_table[input_length % 3]; i++)
        encoded_data[*output_length - 1 - i] = '=';

      encoded_data[*output_length] = '\0';

      return encoded_data;
    }

    unsigned char *decode(const char *data, size_t input_length,
                    size_t *output_length) {
      if (input_length % 4 != 0) return NULL;

      *output_length = input_length / 4 * 3;
      if (data[input_length - 1] == '=') (*output_length)--;
      if (data[input_length - 2] == '=') (*output_length)--;

      unsigned char *decoded_data = (unsigned char *)malloc(*output_length);
      if (decoded_data == NULL) return NULL;

      for (unsigned i = 0, j = 0; i < input_length;) {
        uint32_t a = data[i] == '=' ? 0 & i++ : decoding_table[(int)data[i++]];
        uint32_t b = data[i] == '=' ? 0 & i++ : decoding_table[(int)data[i++]];
        uint32_t c = data[i] == '=' ? 0 & i++ : decoding_table[(int)data[i++]];
        uint32_t d = data[i] == '=' ? 0 & i++ : decoding_table[(int)data[i++]];

        uint32_t triple = (a << 3 * 6)
                        + (b << 2 * 6)
                        + (c << 1 * 6)
                        + (d << 0 * 6);

        if (j < *output_length) decoded_data[j++] = (triple >> 2 * 8) & 0xFF;
        if (j < *output_length) decoded_data[j++] = (triple >> 1 * 8) & 0xFF;
        if (j < *output_length) decoded_data[j++] = (triple >> 0 * 8) & 0xFF;
      }

      return decoded_data;
    }

  private:
    char decoding_table[256];
};
