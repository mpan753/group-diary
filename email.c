/*
 * src/tutorial/email.c
 * modified by Lujie Wang & Pan Meng
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */


PG_MODULE_MAGIC;

typedef struct Email {
    char* local;
    char* domain;
} Email;

char *strlwr(char *);

/*
 * Since we use V1 function calling convention, all these functions have
 * the same signature as far as C is concerned.  We provide these prototypes
 * just to forestall warnings when compiled with gcc -Wmissing-prototypes.
 */
Datum email_in(PG_FUNCTION_ARGS);
Datum email_out(PG_FUNCTION_ARGS);
Datum email_recv(PG_FUNCTION_ARGS);
Datum email_send(PG_FUNCTION_ARGS);

Datum email_abs_lt(PG_FUNCTION_ARGS);
Datum email_abs_le(PG_FUNCTION_ARGS);
Datum email_abs_eq(PG_FUNCTION_ARGS);
Datum email_abs_ge(PG_FUNCTION_ARGS);
Datum email_abs_gt(PG_FUNCTION_ARGS);
Datum email_abs_ne(PG_FUNCTION_ARGS);

Datum email_abs_same_domain(PG_FUNCTION_ARGS);
Datum email_abs_not_same_domain(PG_FUNCTION_ARGS);

Datum email_abs_cmp(PG_FUNCTION_ARGS);



/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(email_in);

Datum
email_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    char *local = (char *) malloc(sizeof(char *));
    char *domain = (char *) malloc(sizeof(char *));
    Email *result;

    char *lowercase = strlwr(str);

    if (sscanf(lowercase, "%[_a-zA-Z0-9.]@%[_a-zA-Z0-9.]", local, domain) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for email: \"%s\"", str)));

    result = (Email *) palloc(sizeof(Email));
    result->local = local;
    result->domain = domain;
    PG_RETURN_POINTER(result);
}

char* strlwr(char *string) {
	int len = strlen(string);

        /* Changed this line to "my style", since I am believing dynamic allocation for string
         * and returning causes pointer error, which could be the issue. */
	char *email = malloc(sizeof(char) * (len + 1));

        int i;
	for (i = 0; i < len; ++i) //Or use "i <= len" to copy the ending '\0'
	{
		if (isalpha(string[i]))
		{
			email[i] = tolower(string[i]);

		} else {
			email[i] = string[i];
		}
	}
        email[len] = '\0'; //Or delete this line for using above method.
	return email;
}

PG_FUNCTION_INFO_V1(email_out);

Datum
email_out(PG_FUNCTION_ARGS) {
    Email *email = (Email *) PG_GETARG_POINTER(0);
    char *result;

    result = (char *) palloc(257);
    snprintf(result, 257, "%s@%s", email->local, email->domain);
    PG_RETURN_CSTRING(result);
}

/*****************************************************************************
 * Binary Input/Output functions
 *
 * These are optional.
 *****************************************************************************/

PG_FUNCTION_INFO_V1(email_recv);

Datum
email_recv(PG_FUNCTION_ARGS) {
    StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
    Email *result;

    result = (Email *) palloc(sizeof(Email));
    result->local = pq_getmsgstring(buf);
    result->domain = pq_getmsgstring(buf);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(email_send);

Datum
email_send(PG_FUNCTION_ARGS) {
	Email *email = (Email *) PG_GETARG_POINTER(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendstring(&buf, email->local);
	pq_sendstring(&buf, email->domain);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*****************************************************************************
 * New Operators
 *
 * A practical Email datatype would provide much more than this, of course.
 *****************************************************************************/

/* Something should be here? */


/*****************************************************************************
 * Operator class for defining B-tree index
 *
 * It's essential that the comparison operators and support function for a
 * B-tree index opclass always agree on the relative ordering of any two
 * data values.  Experience has shown that it's depressingly easy to write
 * unintentionally inconsistent functions.	One way to reduce the odds of
 * making a mistake is to make all the functions simple wrappers around
 * an internal three-way-comparison function, as we do here.
 *****************************************************************************/

static int
internal_same_domain(Email *a, Email *b) {
    if (!strcmp(a->local, b->local))
        return 1;
}


static int
email_abs_cmp_internal(Email *a, Email *b) {
    if (strcmp(a->domain, b->domain) > 0)
        return 1;
    else if (strcmp(a->domain, b->domain) < 0)
        return -1;
    else {
        if (!strcmp(a->local, b->local))
            return 0;
        else if(strcmp(a->local, b->local) > 0)
            return 1;
        else
            return -1;
    }
}


PG_FUNCTION_INFO_V1(email_abs_lt);

Datum
email_abs_lt(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_abs_cmp_internal(a, b) < 0);
}

PG_FUNCTION_INFO_V1(email_abs_le);

Datum
email_abs_le(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_abs_cmp_internal(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(email_abs_eq);

Datum
email_abs_eq(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_abs_cmp_internal(a, b) == 0);
}

PG_FUNCTION_INFO_V1(email_abs_ne);

Datum
email_abs_ne(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_abs_cmp_internal(a, b) != 0);
}

PG_FUNCTION_INFO_V1(email_abs_ge);

Datum
email_abs_ge(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_abs_cmp_internal(a, b) >= 0);
}

PG_FUNCTION_INFO_V1(email_abs_gt);

Datum
email_abs_gt(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(email_abs_cmp_internal(a, b) > 0);
}

PG_FUNCTION_INFO_V1(email_abs_cmp);

Datum
email_abs_cmp(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_INT32(email_abs_cmp_internal(a, b));
}

PG_FUNCTION_INFO_V1(email_abs_same_domain);

Datum
email_abs_same_domain(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(internal_same_domain(a, b));
}

PG_FUNCTION_INFO_V1(email_abs_not_same_domain);

Datum
email_abs_not_same_domain(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(!internal_same_domain(a, b));
}

/*
void string_to_lower(char* string) {
    int i = 0;
    while(string[i] != '\0'){
        string[i] = tolower(string[i]);
        ++i;
    }
    return 0;
}

*/
