/*++
/* NAME
/*	name_code 3
/* SUMMARY
/*	name to number table mapping
/* SYNOPSIS
/*	#include <name_code.h>
/*
/*	typedef struct {
/* .in +4
/*		const char *name;
/*		int code;
/* .in -4
/*	} NAME_CODE;
/*
/*	int	name_code(table, name)
/*	NAME_CODE *table;
/*	const char *name;
/*
/*	const char *str_name_code(table, code)
/*	NAME_CODE *table;
/*	int	code;
/* DESCRIPTION
/*	This module does simple name<->number mapping. The process
/*	is controlled by a table of (name, code) values.
/*	The table is terminated with a null pointer and a code that
/*	corresponds to "name not found".
/*
/*	name_code() looks up the code that corresponds with the name.
/*	The lookup is case insensitive.
/*
/*	str_name_code() translates a number to its equivalend string.
/* DIAGNOSTICS
/*	When the search fails, the result is the "name not found" code
/*	or the null pointer, respectively.
/* LICENSE
/* .ad
/* .fi
/*	The Secure Mailer license must be distributed with this software.
/* AUTHOR(S)
/*	Wietse Venema
/*	IBM T.J. Watson Research
/*	P.O. Box 704
/*	Yorktown Heights, NY 10598, USA
/*--*/

/* System library. */

#include <sys_defs.h>
#include <string.h>

#ifdef STRCASECMP_IN_STRINGS_H
#include <strings.h>
#endif

/* Utility library. */

#include <name_code.h>

/* name_code - look up code by name */

int     name_code(NAME_CODE *table, const char *name)
{
    NAME_CODE *np;

    for (np = table; np->name; np++)
	if (strcasecmp(name, np->name) == 0)
	    break;
    return (np->code);
}

/* str_name_code - look up name by code */

const char *str_name_code(NAME_CODE *table, int code)
{
    NAME_CODE *np;

    for (np = table; np->name; np++)
	if (code == np->code)
	    break;
    return (np->name);
}
