/*
 * src/tutorial/email.c
 * modified by Lujie Wang & Meng Pan
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */
#include <regex.h>

PG_MODULE_MAGIC;

typedef struct Email{
    char *local;
    char *domain;
} Email;


/*
 * Since we use V1 function calling convention, all these functions have
 * the same signature as far as C is concerned.  We provide these prototypes
 * just to forestall warnings when compiled with gcc -Wmissing-prototypes.
 */
Datum email_in(PG_FUNCTION_ARGS);
Datum email_out(PG_FUNCTION_ARGS);

Datum email_abs_lt(PG_FUNCTION_ARGS);
Datum email_abs_le(PG_FUNCTION_ARGS);
Datum email_abs_eq(PG_FUNCTION_ARGS);
Datum email_abs_ge(PG_FUNCTION_ARGS);
Datum email_abs_gt(PG_FUNCTION_ARGS);
Datum email_abs_ne(PG_FUNCTION_ARGS);

Datum email_abs_same_domain(PG_FUNCTION_ARGS);
Datum email_abs_not_same_domain(PG_FUNCTION_ARGS);

Datum email_abs_cmp(PG_FUNCTION_ARGS);

char *strlwr(char *);
bool is_valid_email(char *);
bool is_valid_local(char *);
bool is_valid_domain(char *);
bool is_name_part(char **);
bool is_name_parts(char **);
bool is_name_chars(char **);

/*
  Datum		complex_abs_lt(PG_FUNCTION_ARGS);
  Datum		complex_abs_le(PG_FUNCTION_ARGS);
  Datum		complex_abs_eq(PG_FUNCTION_ARGS);
  Datum		complex_abs_ge(PG_FUNCTION_ARGS);
  Datum		complex_abs_gt(PG_FUNCTION_ARGS);
  Datum		complex_abs_cmp(PG_FUNCTION_ARGS);
*/


/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(email_in);

Datum
email_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    char *local = (char *) malloc(sizeof(char *));
    char *domain = (char *) malloc(sizeof(char *));
//    char local[128];
//    char domain[128];
//    char *local = (char *) palloc(sizeof(128));
//    char *domain = (char *) palloc(sizeof(128));
    Email *result;

    char *email = strlwr(str);

//    if (sscanf(email, "%[_a-zA-Z0-9.]@%[_a-zA-Z0-9.]", local, domain) != 2)
//        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
//                 errmsg("invalid input syntax for email: \"%s\"", str)));

    if ( !is_valid_email(email) ) {
    	ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
    			errmsg("invalid input syntax for email: \"%s\"", str)));
    }
    sscanf(email, "%[-a-zA-Z0-9.]@%[-a-zA-Z0-9.]", local, domain);

    result = (Email *) palloc(sizeof(Email));
    result->local = local;
    result->domain = domain;

//    free(local);
//    free(domain);
//    free(email);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(email_out);

Datum
email_out(PG_FUNCTION_ARGS) {
    Email *email = (Email *) PG_GETARG_POINTER(0);
    char *result;

    result = (char *) palloc(100);
    snprintf(result, 100, "[%s@%s]", email->local, email->domain);
    PG_RETURN_CSTRING(result);

	//	Datum email = PG_GETARG_DATUM(0);
//	PG_RETURN_CSTRING(TextDatumGetCString(email));
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

bool is_valid_email(char *email) {
    char *local = (char *) malloc(sizeof(char *));
    char *domain = (char *) malloc(sizeof(char *));
	char *reg_exp_mail = "^[a-z0-9.-]+@[a-z0-9.-]+$";
	regex_t regex;
	int pre;
	pre = regcomp(&regex,reg_exp_mail,1);
	pre = regexec(&regex, email, 0, NULL, 0);
	if(pre){ // incorrect
		return false;
	}
	regfree (&regex);

	if ( sscanf(email, "%[-a-zA-Z0-9.]@%s", local, domain) != 2 ) {
		free(local);
		free(domain);
		return false;
	}

	if (!is_valid_local(local)) {
		return false;
	}
	if(!is_valid_domain(domain)) {
		return false;
	}
//	free(local);
//	free(domain);
	return true;
}

char *strlwr(char *string) {
	size_t len = strlen(string);

	char *email = malloc(sizeof(char) * (len+1));

	for (int i = 0; i < len; ++i)
	{
		if (isalpha(string[i]))
		{
			email[i] = (tolower(string[i]));

		} else {
			email[i] = string[i];
		}
	}
	email[len] = '\0';
	return email;
}

bool is_valid_local(char *loc) {
	if (!is_name_part(&loc)) {
		printf("name_part_false\n");
		return false;
	}
	if (!is_name_parts(&loc)){
		printf("name_parts_false\n");
		return false;
	}
	if (*loc != '\0') {
		return false;
	}
	return true;
}

bool is_valid_domain(char *dom) {
	if ( !is_name_part(&dom) ) {
		return false;
	}
	if ( (*dom) == '.' ) {
		dom++;
		if ( !is_name_part(&dom) ) {
			return false;
		}
		if ( !is_name_parts(&dom) ) {
			return false;
		}
	} else {
		return false;
	}
	if (*dom != '\0') {
		return false;
	}
	return true;
}

bool is_name_part(char **pt_to_str) {
	if (!isalpha(*(*pt_to_str))) {
		return false;
	}
	while (isalpha(*(*pt_to_str))) {
		(*pt_to_str)++;
	}
	if (!is_name_chars(pt_to_str)) {
		return false;
	}
	if ((*(*pt_to_str)) == '\0') {
		return true;
	}
	return true;
}

bool is_name_parts(char **pt_to_str) {
	if ( (*(*pt_to_str)) == '\0' ) {
		return true;
	}
	if ( (*(*pt_to_str)) == '.' ) {
		(*pt_to_str)++;
		if ( !is_name_part(pt_to_str) ) {
			return false;
		}
		if ( !is_name_parts(pt_to_str) ) {
			return false;
		}
	}
	return true; // should never reach here.
}

bool is_name_chars(char **pt_to_str) {
	while (isalpha(*(*pt_to_str)) || isdigit(*(*pt_to_str)) || ((*(*pt_to_str)) == '-')) {
		if ((*(*pt_to_str)) == '-') {
			if (*((*pt_to_str)+1) == '\0') {
				return false;
			}
		}
		(*pt_to_str)++;
	}
	return true;
}
// end of check
