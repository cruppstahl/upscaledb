/* 
 * wsclean

  Copyright (C) 2012 Ger Hobbelt, www.hebbut.net

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the authors be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.

*/

/*
This little tool is a quick way to 'rewrite' or otherwise process files' whitespace.

Features:

- removes trailing whitespace
- produces either UNIX or WIn32 compliant line endings
- 'smart' to/from TAB replacement at line leading, hence providing proper indenting whatever you did.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#if defined(_MSC_VER)
#include <io.h>
#else
#include <unistd.h>
#endif

#include "getopts.h"

typedef enum command
{
	ARG_HELP = 1,
	ARG_FILE,
	ARG_OUT_FILE,
	ARG_TRIM_TRAILING,
	ARG_TABSIZE,
	ARG_ENTAB,
	ARG_DETAB,
	ARG_RETAB,
	ARG_LANGUAGE,
	ARG_LE_UNIX,
	ARG_LE_MSDOS,
	ARG_VERBOSE,
} command_t;

static const option_t opts[] = 
{
	{ 
		ARG_HELP,					
			"h",                    
			"help",                 
			"this help screen",     
			0                       
	},
	{ 
		ARG_FILE,					
			"f",                    
			"file",                 
			"<filename> input file name",
			GETOPTS_NEED_ARGUMENT	
	},
	{ 
		ARG_OUT_FILE,					
			"o",                    
			"out",                 
			"write output to file <filename>",
			GETOPTS_NEED_ARGUMENT	
	},
	{ 
		ARG_TRIM_TRAILING,					
			"e",                    
			"trim",                 
			"trim trailing whitespace",     
			0                       
	},
	{ 
		ARG_TABSIZE,					
			"T",                    
			"tabsize",                 
			"set the tabsize (default: 4)",     
			GETOPTS_NEED_ARGUMENT
	},
	{ 
		ARG_ENTAB,					
			"t",                    
			"entab",                 
			"convert leading whitespace to tabs",     
			0                       
	},
	{ 
		ARG_DETAB,					
			"x",                    
			"detab",                 
			"convert all tabs to spaces",     
			0                       
	},
	{ 
		ARG_RETAB,					
			"r",                    
			"retab",                 
			"convert leading whitespace to tabs, all other\n"
			"        whitespace to spaces",     
			0                       
	},
	{ 
		ARG_LANGUAGE,					
			"l",                    
			"lang",                 
			"assume the input file is source code written in the\n"
			"        specified language and adjust the 'entab/retab' conversion-to-TAB\n"
			"        rules accordingly.\n"
			"\n"
			"        These languages are supported:\n"
			"           none\n"
			"           auto (default)\n"
			"           C (which can also be used to process JavaScript, PHP, etc.)",     
			GETOPTS_NEED_ARGUMENT   
	},
	{ 
		ARG_LE_UNIX,					
			"U",                    
			"unix",                 
			"produce output with UNIX line endings (LF only)",     
			0                       
	},
	{ 
		ARG_LE_MSDOS,					
			"W",                    
			"windows",                 
			"produce output with Windows/MSDOS line endings (CR+LF)",     
			0                       
	},
	{ 
		ARG_VERBOSE,					
			"v",                    
			"verbose",                 
			"print process progress to stderr",     
			0                       
	},
	{ 0, 0, 0, 0, 0 }				// sentinel
};

const char *filename(const char *path)
{
	const char *delims = "/\\:";

	for ( ; *delims; delims++)
	{
		const char *p = strrchr(path, *delims);
		if (p) path = p + 1;
	}
	return path;
}

static const char **infiles = NULL;

void add_infile(const char *path)
{
	int idx = 0;

	if (!infiles)
	{
		infiles = malloc(2 * sizeof(*infiles));
	}
	else
	{
		for (idx = 0; infiles[idx]; idx++)
			;
		infiles = realloc((void *)infiles, (idx + 2) * sizeof(*infiles));
	}
	infiles[idx] = path;
	infiles[++idx] = NULL;
}

typedef struct
{
	FILE *fin;
	FILE *fout;
} filedef_t;

int pop_filedef(filedef_t *dst, const char **filepath, const char *out_fname)
{
	static int idx = 0;

	memset(dst, 0, sizeof(*dst));

	if (idx == 0 && !infiles)
	{
		dst->fin = stdin;
		dst->fout = stdout;
		*filepath = "stdin -> stdout";

		if (out_fname)
		{
			dst->fout = fopen(out_fname, "wb");
			*filepath = "stdin -> file";
		}
		idx++;
		return !!dst->fout;
	}
	else if (infiles && infiles[idx])
	{
		if (out_fname)
		{
			dst->fin = fopen(infiles[idx], "rb");
			dst->fout = fopen(out_fname, "wb");
		}
		else
		{
			dst->fin = fopen(infiles[idx], "r+b");
			dst->fout = dst->fin;
		}
		*filepath = infiles[idx];
		idx++;
		return !!dst->fin;
	}
	return 0;
}

char *readfile(filedef_t *f, size_t *read_length)
{
	char *buf;
	char *dst;
	size_t size = 1024 * 1024;
	size_t fill = 0;
	int len;
	const size_t CHUNKSIZE = 1024;

	dst = buf = (char *)malloc(size + 4);
	if (!buf) return NULL;
	while (!feof(f->fin))
	{
		if (size - fill < CHUNKSIZE)
		{
			size *= 2;
			buf = (char *)realloc(buf, size + 4);
			if (!buf) return NULL;
			dst = buf + fill;
		}
		len = fread(dst, 1, size - fill, f->fin);
		if (len > 0)
		{
			dst += len;
			fill += len;
		}
		else if (len < 0)
		{
			return NULL;
		}
	}
	*read_length = fill;
	*dst++ = 0;
	*dst++ = 0;
	*dst++ = 0;
	*dst++ = 0;
	return buf;
}

int writefile(filedef_t *f, const char *buf, size_t size)
{
	int len;

	if (f->fout != stdout)
	{
		rewind(f->fout);
	}

	len = fwrite(buf, 1, size, f->fout);
	if (len >= 0 && f->fout == f->fin)
	{
		int rv;

		fflush(f->fout);

#if !defined(_MSC_VER)
		rv = ftruncate(fileno(f->fout), len);
#else
		rv = _chsize(fileno(f->fout), len);
#endif
		if (rv)
		{
			return -2;
		}
	}
	return len;
}

void closefile(filedef_t *f)
{
	if (f->fin && f->fin != stdin)
	{
		fclose(f->fin);
		f->fin = NULL;
	}
	if (f->fout && f->fout != stdout)
	{
		fclose(f->fout);
		f->fout = NULL;
	}
}

typedef struct
{
	unsigned verbose: 2;
	unsigned trim_trailing: 1;
	unsigned tab_mode: 2; /* 0: none, 1: entab, 2: detab; 3: retab */
	unsigned lf_mode: 2; /* 0: autodetect, 1: UNIX, 2: MSDOS, 3: old-style Mac (CR-only) */
} cmd_t;

void report_lf_mode(cmd_t cmd)
{
	if (cmd.verbose) 
		fprintf(stderr, "            Detected LF mode: %s\n", (cmd.lf_mode == 1 ? "UNIX" : cmd.lf_mode == 2 ? "Windows/MSDOS" : "old Mac"));
}

char *strtolower(char *s)
{
	while (*s)
	{
		if (*s < 127)
		{
			*s = (char)tolower(*s);
		}
		s++;
	}
	return s;
}

int determine_indent_language(cmd_t cmd, const char *lang, const char *fname, const char *buf, size_t len)
{
	int smart = (lang && strcmp(lang, "none") != 0);
	const char *ext = strrchr(fname, '.');
	static const char *exts[] = 
	{
		"c",
		"cc",
		"cpp",
		"h",
		"hpp",
		"js",
		"php",
		"sh",
		NULL
	};
	const char **elst = exts;
	char *e;
	const char *known_lang = NULL;

	if (ext)
		ext++;
	else
		ext = "";
	e = strdup(ext);
	strtolower(e);

	while (*elst)
	{
		if (strcmp(e, *elst) == 0)
		{
			known_lang = *elst;
			break;
		}
		elst++;
	}

	free(e);
	return smart && known_lang;
}


int main(int argc, char **argv)
{
	unsigned int opt;
	char *param;
	const char *appname = filename(argv[0]);
	unsigned int tabsize = 4;
	const char *lang = "auto";
	cmd_t cmd = {0};
	const char *fpath;
	const char *fname;
	const char *out_fname = NULL;
	filedef_t fdef;

	getopts_init(argc, argv, appname);

	for (;;)
	{
		char *p;
		unsigned long l;

		opt = getopts(opts, &param);
		switch (opt)
		{
		case 0:
			break;

		case ARG_HELP:
			getopts_usage(opts);
			exit(EXIT_FAILURE);

		case ARG_OUT_FILE:
			out_fname = param;
			continue;

		case ARG_FILE:
		case GETOPTS_PARAMETER:
			add_infile(param);
			continue;

		case ARG_TRIM_TRAILING:
			cmd.trim_trailing = 1;
			continue;

		case ARG_TABSIZE:
			l = strtoul(param, &p, 10);
			if (*p || l == 0 || l > 16 /* heuristicly sane max tab size */)
			{
				printf("%s: invalid tabsize specified: %s\n", appname, param);
				exit(EXIT_FAILURE);
			}
			tabsize = (unsigned int)l;
			continue;

		case ARG_ENTAB:
			cmd.tab_mode |= 1;
			continue;

		case ARG_DETAB:
			cmd.tab_mode |= 2;
			continue;

		case ARG_RETAB:
			cmd.tab_mode |= 3;
			continue;

		case ARG_VERBOSE:
			cmd.verbose++;
			if (!cmd.verbose) cmd.verbose = ~0u;
			continue;

		case ARG_LANGUAGE:
			lang = param;
			continue;

		case ARG_LE_UNIX:
			cmd.lf_mode = 1;
			continue;

		case ARG_LE_MSDOS:
			cmd.lf_mode = 2;
			continue;

		case GETOPTS_UNKNOWN:
			printf("%s: unknown parameter %s\n", appname, param);
			exit(EXIT_FAILURE);

		case GETOPTS_MISSING_PARAM:
			printf("%s: option %s is missing a mandatory parameter\n", appname, param);
			exit(EXIT_FAILURE);
		}
		break;
	}

	if (out_fname && infiles && infiles[1])
	{
		printf("%s: when you specify an output file (%s), you can only specify one input file or none at all\n", appname, out_fname);
		exit(EXIT_FAILURE);
	}

	fflush(stdout);

	while (pop_filedef(&fdef, &fpath, out_fname))
	{
		char *buf;
		size_t len;
		size_t size;
		char *s;
		char *d;
		char *e;
		char *d_non_ws;
		char *obuf;
		unsigned int colpos;
		unsigned int previous_line_indent;
		union
		{
			unsigned int anything;

			struct
			{
				unsigned c_comment: 1;
				unsigned cpp_comment: 1;
				unsigned continued_line: 2; /* state: 1 = continued line will follow beyond next LF; 2 = on continued line */
				unsigned conditional_exp: 12; /* count the number of braces/brackets here */
				unsigned quoted_string: 1;
				unsigned dquoted_string: 1;
				unsigned doctext: 1; /* <<<EOT ... EOT */
			} el;
		} inside = {0};
		const char *doctext_marker;
		int doctext_marker_len;
		int do_smart_lang_indent;

		fname = filename(fpath);

		if (cmd.verbose) fprintf(stderr, "Processing: %s\n", (cmd.verbose > 1 ? fpath : fname));

		// read file into buffer:
		buf = readfile(&fdef, &len);
		if (!buf) 
		{
			fprintf(stderr, "*** ERROR: failure while reading data from file '%s'\n", (cmd.verbose ? fpath : fname));
			exit(EXIT_FAILURE);
		}

		do_smart_lang_indent = determine_indent_language(cmd, lang, fname, buf, len);

		// get me a fast, rough, worst-case estimate of the buffer size needed for the conversion:
		size = len;
		for (s = buf, e = buf + len; s < e; s++)
		{
			switch (*s)
			{
			case '\r':	// may be old-style Mac input: CR-only lines!
			case '\n':
				size++;
				continue;

			case '\t':
				size += tabsize - 1;
				continue;
			}
		}
		obuf = (char *)malloc(size + 4);
		if (!obuf) 
		{
			fprintf(stderr, "*** ERROR: failure while processing data from file '%s'\n", (cmd.verbose ? fpath : fname));
			exit(EXIT_FAILURE);
		}

		colpos = 0;
		previous_line_indent = 0;
		doctext_marker = NULL;
		doctext_marker_len = 0;
		d_non_ws = NULL;
		d = obuf;
		for (s = buf, e = buf + len; s < e; s++)
		{
			unsigned int colstep;
			static const char *tab2space_blob = "                "; /* another reason why we limit tabsize to 16 ;-) */

			switch (*s)
			{
			case '\r':	// may be old-style Mac input: CR-only lines!
				if (s[1] == '\n')
				{
					s++;
					if (cmd.lf_mode == 0) { cmd.lf_mode = 2; report_lf_mode(cmd); }
				}
				if (cmd.lf_mode == 0) { cmd.lf_mode = 3; report_lf_mode(cmd); }
			case '\n':
				if (cmd.lf_mode == 0) { cmd.lf_mode = 1; report_lf_mode(cmd); }

				// trim trailing WS?
				if (cmd.trim_trailing && d_non_ws)
				{
					d = d_non_ws;
				}

				// one newline:
				colpos = 0;
				d_non_ws = NULL;

				if (inside.el.cpp_comment)
				{
					inside.el.cpp_comment = 0;
				}
				if (inside.el.continued_line)
				{
					inside.el.continued_line++;
					inside.el.continued_line %= 3;
				}

				switch (cmd.lf_mode)
				{
				default:
					*d++ = '\n';
					continue;

				case 2:
					*d++ = '\r';
					*d++ = '\n';
					continue;
				}

			case '\t':
				colstep = tabsize - (colpos % tabsize);
				colpos += colstep;

				switch (!inside.anything 
						? cmd.tab_mode
						: (inside.el.doctext || inside.el.dquoted_string || inside.el.quoted_string)
						? 0
						: colpos > previous_line_indent
						? 2
						: cmd.tab_mode)
				{
				default:
				case 0:
				case 1:
					*d++ = *s;
					continue;

				case 3:
					if (!d_non_ws)
					{
						*d++ = '\t';
						continue;
					}
				case 2:
					memcpy(d, tab2space_blob, colstep);
					d += colstep;
					continue;
				}

			case ' ':
				colpos++;

				switch (!inside.anything 
						? cmd.tab_mode
						: (inside.el.doctext || inside.el.dquoted_string || inside.el.quoted_string)
						? 0
						: colpos > previous_line_indent
						? 2
						: cmd.tab_mode)
				{
				case 1:
				case 3:
					if (!d_non_ws)
					{
						unsigned int c = colpos % tabsize;
						if (c == 0)
						{
							*d++ = '\t';
						}
						else
						{
							unsigned int i;

							for (i = c; i < tabsize && *++s == ' '; i++)
								;
							if (i == tabsize)
							{
								if (inside.anything && colpos + i - c > previous_line_indent)
								{
									s++;
								}
								else
								{
									colpos += i - c;
									*d++ = '\t';
									continue;
								}
							}
							s--;
							i -= c - 1;
							memcpy(d, tab2space_blob, i);
							d += i;
							colpos += i - 1;
						}
						continue;
					}
				default:
				case 0:
				case 2:
					*d++ = *s;
					continue;
				}

			default:
				// non-whitespace is assumed:
				if (!inside.anything && !d_non_ws)
				{
					previous_line_indent = colpos;
				}

				/* now check if we are entering a comment, conditional expression, string, or other possibly multiline spanning 'feature': */
				if (do_smart_lang_indent)
				{
					if (!inside.el.cpp_comment && !inside.el.c_comment)
					{
						if (*s == '/' && s[1] == '/')
							inside.el.cpp_comment = 1;
						if (*s == '/' && s[1] == '*')
							inside.el.c_comment = inside.el.cpp_comment = 1;

						if (*s == '\\' && (s[1] == '\r' || s[1] == '\n'))
							inside.el.continued_line = 1;

						if (*s == '\'' && s != buf && s[-1] != '\\' && !inside.el.dquoted_string)
							inside.el.quoted_string = !inside.el.quoted_string;
						if (*s == '"' && s != buf && s[-1] != '\\' && !inside.el.quoted_string)
							inside.el.dquoted_string = !inside.el.dquoted_string;

						if (*s == '<' && s[1] == '<' && s[2] == '<' && !inside.el.quoted_string && !inside.el.dquoted_string)
						{
							inside.el.doctext = 1;

							doctext_marker = s + 3;
							doctext_marker_len = strcspn(doctext_marker, "\r\n \t");
						}
						if (inside.el.doctext && colpos == 0 && strncmp(s, doctext_marker, doctext_marker_len) == 0)
						{
							inside.el.doctext = 0;

							doctext_marker = NULL;
							doctext_marker_len = 0;
						}

						if (!inside.el.doctext && !inside.el.dquoted_string && !inside.el.quoted_string)
						{
							if (*s == '(' || *s == '[')
							{
								inside.el.conditional_exp++;
							}
							if (inside.el.conditional_exp && (*s == ')' || *s == ']'))
							{
								inside.el.conditional_exp--;
							}
							// note: because we also support JavaScript, we cannot simply reset the conditional_exp value to 0 when encountering a '{'!
						}
					}
					else if (inside.el.c_comment)
					{
						if (*s == '*' && s[1] == '/')
							inside.el.c_comment = 0;
					}
				}

				colpos++;
				*d++ = *s;
				d_non_ws = d;
				continue;
			}
		}
		*d = 0;

		// processing done, now write result to file:
		len = writefile(&fdef, obuf, d - obuf);
		if (len < 0)
		{
			fprintf(stderr, "*** ERROR: failure while WRITING data to file '%s'\n", (cmd.verbose ? fpath : fname));
			exit(EXIT_FAILURE);
		}

		closefile(&fdef);
	}
	
	if (cmd.verbose) fprintf(stderr, "Processing: ---done---\n");
	exit(EXIT_SUCCESS);
}

