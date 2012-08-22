/* main.c */

/*
    This program demonstrates a simple application of JSON_parser. It reads
    a JSON text from STDIN, producing an error message if the text is rejected.

        % JSON_parser <test/pass1.json
*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <locale.h>

#include "JSON_parser.h"

static size_t s_Level = 0;

static const char* s_pIndention = "  ";

static int s_IsKey = 0;

static void print_indention()
{
    size_t i;

    for (i = 0; i < s_Level; ++i) {
        printf(s_pIndention);
    }
}


static int print(void* ctx, int type, const JSON_value* value)
{
    switch(type) {
    case JSON_T_ARRAY_BEGIN:
        if (!s_IsKey) print_indention();
        s_IsKey = 0;
        printf("[\n");
        ++s_Level;
        break;
    case JSON_T_ARRAY_END:
        assert(!s_IsKey);
        if (s_Level > 0) --s_Level;
        print_indention();
        printf("]\n");
        break;
   case JSON_T_OBJECT_BEGIN:
       if (!s_IsKey) print_indention();
       s_IsKey = 0;
       printf("{\n");
        ++s_Level;
        break;
    case JSON_T_OBJECT_END:
        assert(!s_IsKey);
        if (s_Level > 0) --s_Level;
        print_indention();
        printf("}\n");
        break;
    case JSON_T_INTEGER:
        if (!s_IsKey) print_indention();
        s_IsKey = 0;
        printf("integer: "JSON_PARSER_INTEGER_SPRINTF_TOKEN"\n", value->vu.integer_value);
        break;
    case JSON_T_FLOAT:
        if (!s_IsKey) print_indention();
        s_IsKey = 0;
        // printf("float: %s\n", value->vu.str.value); /* We wanted stringified floats */
        printf("float: " JSON_PARSER_FLOAT_SPRINTF_TOKEN "\n", value->vu.float_value); /* We wanted stringified floats */
        break;
    case JSON_T_NULL:
        if (!s_IsKey) print_indention();
        s_IsKey = 0;
        printf("null\n");
        break;
    case JSON_T_TRUE:
        if (!s_IsKey) print_indention();
        s_IsKey = 0;
        printf("true\n");
        break;
    case JSON_T_FALSE:
        if (!s_IsKey) print_indention();
        s_IsKey = 0;
        printf("false\n");
        break;
    case JSON_T_KEY:
        s_IsKey = 1;
        print_indention();
        printf("key = '%s', value = ", value->vu.str.value);
        break;
    case JSON_T_STRING:
        if (!s_IsKey) print_indention();
        s_IsKey = 0;
        printf("string: '%s'\n", value->vu.str.value);
        break;
    default:
        assert(0);
        break;
    }

    return 1;
}


int main(int argc, char* argv[]) {
    int count = 0;
    FILE* input;

/*
    Read STDIN. Exit with a message if the input is not well-formed JSON text.

    jc will contain a JSON_parser with a maximum depth of 20.
*/

    JSON_config config;

    struct JSON_parser_struct* jc = NULL;

    init_JSON_config(&config);

    config.depth                  = 20;
    config.callback               = &print;
    config.allow_comments         = 1;
    config.handle_floats_manually = 0;

    /* Important! Set locale before parser is created.*/
    if (argc >= 2) {
        if (!setlocale(LC_ALL, argv[1])) {
            fprintf(stderr, "Failed to set locale to '%s'\n", argv[1]);
        }
    } else {
        fprintf(stderr, "No locale provided, C locale is used\n");
    }

    jc = new_JSON_parser(&config);

    input = stdin;
    for (; input ; ++count) {
        int next_char = fgetc(input);
        if (next_char <= 0) {
            break;
        }
        if (!JSON_parser_char(jc, next_char)) {
            delete_JSON_parser(jc);
            fprintf(stderr, "JSON_parser_char: syntax error, byte %d\n", count);
            return 1;
        }
    }
    if (!JSON_parser_done(jc)) {
        delete_JSON_parser(jc);
        fprintf(stderr, "JSON_parser_end: syntax error\n");
        return 1;
    }

    return 0;
}

