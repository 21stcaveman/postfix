/*++
/* NAME
/*	dict_mongodb 3
/* SUMMARY
/*	dictionary interface to mongodb, compatible with libmongoc-1.0
/* SYNOPSIS
/*	#include <dict_mongodb.h>
/*
/*	DICT *dict_mongodb_open(name, open_flags, dict_flags)
/*	const char *name;
/*	int open_flags;
/*	int dict_flags;
/* DESCRIPTION
/*	dict_mongodb_open() opens a mongodb, providing
/*	a dictionary interface for Postfix mappings.
/*	The result is a pointer to the installed dictionary.
/*
/*	Configuration parameters are described in mongodb_table(5).
/*
/*	Arguments:
/* .IP name
/*	Either the path to the MongoDB configuration file (if it starts
/*	with '/' or '.'), or the prefix which will be used to obtain
/*	main.cf configuration parameters for this search.
/*
/*	In the first case, the configuration parameters below are
/*	specified in the file as \fIname\fR=\fIvalue\fR pairs.
/*
/*	In the second case, the configuration parameters are
/*	prefixed with the value of \fIname\fR and an underscore,
/*	and they are specified in main.cf.  For example, if this
/*	value is \fImongodbconf\fR, the parameters would look like
/*	\fImongodbconf_uri\fR, \fImongodbconf_collection\fR, and so on.
/*
/* .IP open_flags
/*	Must be O_RDONLY
/* 
/* .IP dict_flags
/*	See dict_open(3).
/* 
/* SEE ALSO
/*	dict(3) generic dictionary manager
/* HISTORY
/* .ad
/* .fi
/* 
/* AUTHOR(S)
/*  Hamid Maadani (hamid@dexo.tech)
/*  Dextrous Technologies, LLC
/*  P.O. Box 213
/*  5627 Kanan Rd.,
/*  Agoura Hills, CA, USA
/*--*/

/* System library. */
#include "sys_defs.h"

#ifdef HAS_MONGODB
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <ctype.h>

/* Utility library. */

#include "dict.h"
#include "msg.h"
#include "mymalloc.h"
#include "vstring.h"
#include "stringops.h"
#include <auto_clnt.h>
#include <vstream.h>

/* Global library. */

#include "cfg_parser.h"
#include "db_common.h"

/* Application-specific. */

#include <bson/bson.h>
#include <mongoc/mongoc.h>
#include "dict_mongodb.h"

#ifndef BUFFER_SIZE
#define BUFFER_SIZE 1024
#endif

#define INIT_VSTR(buf, len) do { \
	if (buf == 0) \
		buf = vstring_alloc(len); \
	VSTRING_RESET(buf); \
	VSTRING_TERMINATE(buf); \
} while (0)

/* Structure of one mongodb dictionary handle. */
typedef struct {
	DICT dict; /* parent class */
	CFG_PARSER *parser; /* common parameter parser */
	void *ctx;
	mongoc_client_t *client; /* Mongo client */
	char *uri; /* URI like mongodb+srv://localhost:27017 */
	char *dbname; /* database name */
	char *collection; /* collection name */
	char *filter; /* MongoDB filter */
	char *result_attribute; /* the key(s) to return the data for */
	char *result_format; /* db_common_expand() result_format */
	int expansion_limit;
	char *projection; /* advanced MongoDB projection */
} DICT_MONGODB;

static bool init_done = false;

static char *itoa(int val) {
	static char buf[32] = {0};
	int i = 30;

	for(; val && i ; --i, val /= 10) {
		buf[i] = "0123456789abcdef"[val % 10];
	}

	return &buf[i+1];
}

/* mongodb_parse_config - parse mongodb configuration file */
static void mongodb_parse_config(DICT_MONGODB *dict_mongodb, const char *mongodbcf)
{
	CFG_PARSER *p = dict_mongodb->parser;

	dict_mongodb->uri = cfg_get_str(p, "uri", NULL, 1, 0);
	dict_mongodb->dbname = cfg_get_str(p, "dbname", NULL, 1, 0);
	dict_mongodb->collection = cfg_get_str(p, "collection", NULL, 1, 0);
	dict_mongodb->filter = cfg_get_str(p, "filter", NULL, 1, 0);
	dict_mongodb->projection = cfg_get_str(p, "projection", NULL, 0, 0);
	dict_mongodb->result_attribute = cfg_get_str(p, "result_attribute", NULL, 0, 0);
	if ((dict_mongodb->result_format = cfg_get_str(dict_mongodb->parser, "result_format", 0, 0, 0)) == 0) {
		dict_mongodb->result_format = cfg_get_str(dict_mongodb->parser, "result_filter", "%s", 1, 0);
	}
	dict_mongodb->expansion_limit = cfg_get_int(dict_mongodb->parser, "expansion_limit", 10, 0, 100);;

	dict_mongodb->ctx = 0;
	(void) db_common_parse(&dict_mongodb->dict, &dict_mongodb->ctx, dict_mongodb->filter, 1);
	db_common_parse_domain(dict_mongodb->parser, dict_mongodb->ctx);
}

static bool myexpand(DICT_MONGODB *dict_mongodb, const char *p, VSTRING *resultString, int *expansion, const char *key) {
	if (dict_mongodb->expansion_limit > 0 && ++(*expansion) > dict_mongodb->expansion_limit) {
			msg_warn("%s(%s): expansion limit exceeded for key: '%s'", dict_mongodb->dict.type, dict_mongodb->dict.name, key);
// 			dict_mongodb->dict.error = DICT_ERR_RETRY;
			return false;
	}

	db_common_expand(dict_mongodb->ctx, dict_mongodb->result_format, p, 0, resultString, 0);

	return true;
}

static char *get_result_string(DICT_MONGODB *dict_mongodb, bson_iter_t *iter, const char *key, int *expansion) {
	char *p = NULL;
	VSTRING *resultString = NULL;
	bool got_one_result = false;

	INIT_VSTR(resultString, BUFFER_SIZE);
	while (bson_iter_next(iter)) {
		switch (bson_iter_type(iter)) {
			case BSON_TYPE_UTF8:
				p = (char *)bson_iter_utf8(iter, NULL);
				if (! bson_utf8_validate((const char *)p, strlen(p), true)) {
					msg_error("%s(%s): invalid UTF-8 string '%s'", dict_mongodb->dict.type, dict_mongodb->dict.name, p);
					break;
				}
				got_one_result |= myexpand(dict_mongodb, (const char *)p, resultString, expansion, key);
				break;
			case BSON_TYPE_INT64:
			case BSON_TYPE_INT32:
				p = itoa(bson_iter_as_int64(iter));
				got_one_result |= myexpand(dict_mongodb, (const char *)p, resultString, expansion, key);
				break;
			case BSON_TYPE_ARRAY:
				const uint8_t *dataBuffer = NULL;
				unsigned int len = 0;
				bson_iter_t dataIter;
				bson_t *data = NULL;

				bson_iter_array(iter, &len, &dataBuffer);
				data = bson_new_from_data(dataBuffer, len);
				if (bson_iter_init(&dataIter, data)) {
					if (p = get_result_string(dict_mongodb, &dataIter, key, expansion)) {
						vstring_sprintf_append(resultString, (got_one_result) ? ",%s" : "%s", p);
						got_one_result = true;
						myfree(p);
					}
				}
				bson_destroy(data);
				break;
			default:
				msg_warn("%s(%s): failed to retrieve value of '%s', Unknown result type %d.", dict_mongodb->dict.type, 
							dict_mongodb->dict.name, bson_iter_key(iter), bson_iter_type(iter));
		}
	}

	if (got_one_result) {
		return vstring_export(resultString);
	}

	vstring_free(resultString);
	return NULL;
}

/* dict_mongodb_lookup - find database entry using mongo query language */
static const char *dict_mongodb_lookup(DICT *dict, const char *name)
{
	DICT_MONGODB *dict_mongodb = (DICT_MONGODB *) dict;
	mongoc_collection_t *coll = NULL;
	mongoc_cursor_t *cursor = NULL;
	bson_iter_t iter;
	const bson_t *doc = NULL;
	bson_t *query = NULL;
	bson_t *options = NULL;
	bson_t *projection = NULL;
	bson_error_t error;
	char *result = NULL;
	VSTRING *queryString = NULL;
	int domain_rc;
	char *p = NULL;
	int expansion = 0;

	dict->error = DICT_STAT_SUCCESS;

	// If they specified a domain list for this map, then only search for
	// addresses in domains on the list. This can significantly reduce the
	// load on the database.
	if ((domain_rc = db_common_check_domain(dict_mongodb->ctx, name)) == 0) {
		msg_info("%s(%s): skipping lookup of '%s': domain mismatch", dict_mongodb->dict.type, dict_mongodb->dict.name, name);
		return NULL;
	} else if (domain_rc < 0) {
		DICT_ERR_VAL_RETURN(dict, domain_rc, NULL);
	}

	if (! dict_mongodb->client) {
		msg_error("%s(%s): mongo client not initialized!", dict_mongodb->dict.type, dict_mongodb->dict.name);
		DICT_ERR_VAL_RETURN(dict, DICT_STAT_ERROR, NULL);
	}

	coll = mongoc_client_get_collection(dict_mongodb->client, dict_mongodb->dbname, dict_mongodb->collection);
	if (!coll) {
		msg_error("%s(%s): failed to get collection [%s] from [%s]", dict_mongodb->dict.type, dict_mongodb->dict.name, 
				  dict_mongodb->collection, dict_mongodb->dbname);
		dict->error = DICT_STAT_ERROR;
		goto cleanup;
	}

	options = bson_new();
	// Is a projection provided?
	if (dict_mongodb->projection) {
		// Use provided projection, ignore result_attribute
		projection = bson_new_from_json((uint8_t *)dict_mongodb->projection, -1, &error);
		if (! projection) {
			msg_error("%s(%s): failed to create a projection from '%s' : %s", dict_mongodb->dict.type, dict_mongodb->dict.name, 
					  dict_mongodb->projection, error.message);
			dict->error = DICT_STAT_ERROR;
			goto cleanup;
		}
		BSON_APPEND_DOCUMENT(options, "projection", projection);
	} else {
		// Create a projection using result_attribute
		if (! dict_mongodb->result_attribute) {                                                                                     
			msg_error("%s(%s): 'result_attribute' can not be empty!", dict_mongodb->dict.type, dict_mongodb->dict.name);
			dict->error = DICT_STAT_ERROR;
			goto cleanup;
		}
		char *ra = mystrdup(dict_mongodb->result_attribute);
		char *pp = ra;
		projection = bson_new();
		BSON_APPEND_DOCUMENT_BEGIN(options, "projection", projection);
		BSON_APPEND_INT32(projection, "_id", 0);
		while (p = mystrtok(&pp, ",")) {
			BSON_APPEND_INT32(projection, p, 1);
		}
		bson_append_document_end(options, projection);
		myfree(ra);
	}

	// Create query after expanding the filter template
	INIT_VSTR(queryString, BUFFER_SIZE);
	db_common_expand(dict_mongodb->ctx, dict_mongodb->filter, name, 0, queryString, 0);
	query = bson_new_from_json((uint8_t *)vstring_str(queryString), -1, &error);
	if (! query) {
		msg_error("%s(%s): failed to create a query from '%s' : %s", dict_mongodb->dict.type, dict_mongodb->dict.name, 
				  vstring_str(queryString), error.message);
		dict->error = DICT_STAT_ERROR;
		goto cleanup;
	}

	// Run the query
	cursor = mongoc_collection_find_with_opts(coll, query, options, NULL);
	if (mongoc_cursor_error(cursor, &error)) {
		msg_error("%s(%s): cursor error: %s", dict_mongodb->dict.type, dict_mongodb->dict.name, error.message);
		dict->error = DICT_STAT_ERROR;
		goto cleanup;
	}

	while (mongoc_cursor_next(cursor, &doc)) {
		// Iterate through all results
		if (bson_iter_init(&iter, doc)) {
			result = get_result_string(dict_mongodb, &iter, (const char *)name, &expansion);
		}
	}

cleanup:
	mongoc_cursor_destroy(cursor);
	bson_destroy(query);
	bson_destroy(projection);
	bson_destroy(options);

	if (queryString) {
		vstring_free(queryString);
	}

	mongoc_collection_destroy(coll);

	return result;
}

/* dict_mongodb_close - close MongoDB database */
static void dict_mongodb_close(DICT *dict)
{
	DICT_MONGODB *dict_mongodb = (DICT_MONGODB *) dict;

	cfg_parser_free(dict_mongodb->parser);
	if (dict_mongodb->ctx) {                                                                                         
		db_common_free_ctx(dict_mongodb->ctx);                                                                   
	}

	myfree(dict_mongodb->collection);
	myfree(dict_mongodb->filter);

	if (dict_mongodb->result_attribute) {
		myfree(dict_mongodb->result_attribute);
	}
	if (dict_mongodb->result_format) {
		myfree(dict_mongodb->result_format);
	}
	if (dict_mongodb->projection) {
		myfree(dict_mongodb->projection);
	}
	if (dict_mongodb->client) {
		mongoc_client_destroy(dict_mongodb->client);
	}

	dict_free(dict);
}

/* dict_mongodb_open - open MongoDB database connection */
DICT *dict_mongodb_open(const char *name, int open_flags, int dict_flags)
{
	DICT_MONGODB *dict_mongodb;
	CFG_PARSER *parser;
	bson_error_t error;

	// Sanity checks.
	if (open_flags != O_RDONLY) {
		return (dict_surrogate(DICT_TYPE_MONGODB, name, open_flags, dict_flags,
				"%s(%s): map requires O_RDONLY access mode", DICT_TYPE_MONGODB, name));
	}

	// Open the configuration file.
	if ((parser = cfg_parser_alloc(name)) == 0) {
		return (dict_surrogate(DICT_TYPE_MONGODB, name, open_flags, dict_flags, "open %s: %m", name));
	}

	// Create the dictionary object.
	dict_mongodb = (DICT_MONGODB *)dict_alloc(DICT_TYPE_MONGODB, name, sizeof(*dict_mongodb));

	/* Pass pointer functions */
	dict_mongodb->dict.lookup = dict_mongodb_lookup;
	dict_mongodb->dict.close = dict_mongodb_close;
	dict_mongodb->dict.flags = dict_flags;
	dict_mongodb->parser = parser;
	dict_mongodb->dict.owner = cfg_get_owner(dict_mongodb->parser);
	dict_mongodb->client = NULL;

	/* Parse config */
	mongodb_parse_config(dict_mongodb, name);

	// Initialize libmongoc's internals if not done yet
	if (! init_done) {
		mongoc_init();
		init_done = true;
	}

	// Safely create a MongoDB URI object from the given string
	mongoc_uri_t *uri = mongoc_uri_new_with_error(dict_mongodb->uri, &error);
	if (! uri) {
		msg_error("%s(%s): failed to parse URI '%s' : %s", dict_mongodb->dict.type, dict_mongodb->dict.name, 
				  dict_mongodb->uri, error.message);
		dict_mongodb->dict.error = DICT_STAT_ERROR;
	} else {
		// Create a new mongo-c client
		dict_mongodb->client = mongoc_client_new_from_uri(uri);
		if (! dict_mongodb->client) {
			msg_error("%s(%s): failed to create client for '%s'!", dict_mongodb->dict.type, dict_mongodb->dict.name, dict_mongodb->uri);
			dict_mongodb->dict.error = DICT_STAT_ERROR;
		} else {
			mongoc_client_set_error_api(dict_mongodb->client, MONGOC_ERROR_API_VERSION_2);
		}
		mongoc_uri_destroy(uri);
	}

	return (DICT_DEBUG(&dict_mongodb->dict));
}

#endif
