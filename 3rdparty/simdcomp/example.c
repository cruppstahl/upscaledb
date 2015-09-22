#include <stdio.h>
#include <time.h>
#include "simdcomp.h"


/* compresses data from datain to buffer, returns how many bytes written */
size_t compress(uint32_t * datain, size_t length, uint8_t * buffer) {
    uint32_t offset;
    uint8_t * initout;
    size_t k;
	if(length/SIMDBlockSize*SIMDBlockSize != length) {
	    printf("Data length should be a multiple of %i \n",SIMDBlockSize);
	}
    offset = 0;
    initout = buffer;
	for(k = 0; k < length / SIMDBlockSize; ++k) {
        uint32_t b = simdmaxbitsd1(offset,
                    datain + k * SIMDBlockSize);
		*buffer++ = b;
		simdpackwithoutmaskd1(offset, datain + k * SIMDBlockSize, (__m128i *) buffer,
                    b);
        offset = datain[k * SIMDBlockSize + SIMDBlockSize - 1];
        buffer += b * sizeof(__m128i);
	}
	return buffer - initout;
}


int main() {
    int REPEAT = 10, gap;
    int N = 1000000 * SIMDBlockSize;/* SIMDBlockSize is 128 */
    uint32_t * datain = malloc(N * sizeof(uint32_t));
    size_t compsize;
    clock_t start, end;

    uint8_t * buffer = malloc(N * sizeof(uint32_t) + N / SIMDBlockSize); /* output buffer */
    uint32_t * backbuffer = malloc(SIMDBlockSize * sizeof(uint32_t));
    for (gap = 1; gap <= 243; gap *= 3) {
        int k, repeat;
        uint32_t offset = 0;
        uint32_t bogus = 0;
        double numberofseconds;

    	printf("\n");
        printf(" gap = %u \n", gap);
        for (k = 0; k < N; ++k)
            datain[k] = k * gap;
        compsize = compress(datain,N,buffer);
        printf("compression ratio = %f \n",  (N * sizeof(uint32_t))/ (compsize * 1.0 ));
        start = clock();
        for(repeat = 0; repeat < REPEAT; ++repeat) {
         uint8_t * decbuffer = buffer;
         for (k = 0; k * SIMDBlockSize < N; ++k) {
        	uint8_t b = *decbuffer++;
            simdunpackd1(offset, (__m128i *) decbuffer, backbuffer, b);
            /* do something here with backbuffer */
            bogus += backbuffer[3];
            decbuffer += b * sizeof(__m128i);
            offset = backbuffer[SIMDBlockSize - 1];
         }
        }
        end = clock();
        numberofseconds = (end-start)/(double)CLOCKS_PER_SEC;
        printf("decoding speed in million of integers per second %f \n",N*REPEAT/(numberofseconds*1000.0*1000.0));
        start = clock();
        for(repeat = 0; repeat < REPEAT; ++repeat) {
         uint8_t * decbuffer = buffer;
         for (k = 0; k * SIMDBlockSize < N; ++k) {
            memcpy(backbuffer,decbuffer+k*SIMDBlockSize,SIMDBlockSize*sizeof(uint32_t));
            bogus += backbuffer[3] - backbuffer[100];
         }
        }
        end = clock();
        numberofseconds = (end-start)/(double)CLOCKS_PER_SEC;
         printf("memcpy speed in million of integers per second %f \n",N*REPEAT/(numberofseconds*1000.0*1000.0));
       printf("ignore me %i \n",bogus);
       printf("All tests are in CPU cache. Avoid out-of-cache decoding in applications.\n");
    }
    free(buffer);
    free(datain);
    free(backbuffer);
    return 0;
}

