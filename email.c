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

typedef struct Email{
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
//Datum email_recv(PG_FUNCTION_ARGS);
//Datum email_send(PG_FUNCTION_ARGS);
//Datum email_add(PG_FUNCTION_ARGS);
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
    Email *result;

    char *lowercase = strlwr(str);

    if (sscanf(lowercase, "%[_a-zA-Z0-9.]@%[_a-zA-Z0-9.]", local, domain) != 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for email: \"%s\"", str)));

    result = (Email *) malloc(sizeof(Email));
    result->local = local;
    result->domain = domain;
    PG_RETURN_POINTER(result);
}

char *strlwr(char *string) {
	size_t len = strlen(string);

	char *email = malloc(sizeof(char *));

	for (int i = 0; i < len; ++i)
	{
		if (isalpha(string[i]))
		{
			email[i] = (tolower(string[i]));

		} else {
			email[i] = string[i];
		}
	}
	return email;
}

PG_FUNCTION_INFO_V1(email_out);

Datum
email_out(PG_FUNCTION_ARGS) {
    Email *email = (Email *) PG_GETARG_POINTER(0);
    char *result;

    result = (char *) palloc(100);
    snprintf(result, 100, "(%s@%s)", email->local, email->domain);
    PG_RETURN_CSTRING(result);
}

///*****************************************************************************
// * Binary Input/Output functions
// *
// * These are optional.
// *****************************************************************************/

//PG_FUNCTION_INFO_V1(email_recv);
//
//Datum
//email_recv(PG_FUNCTION_ARGS) {
//    StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
//    Email *result;
//
//    result = (Email *) palloc(sizeof(Email));
//    result->local = pq_getmsgstring(buf);
//    result->domain = pq_getmsgstring(buf);
//    PG_RETURN_POINTER(result);
//}
//
//PG_FUNCTION_INFO_V1(email_send);
//
//Datum
//email_send(PG_FUNCTION_ARGS) {
//	Email *email = (Email *) PG_GETARG_POINTER(0);
//	StringInfoData buf;
//
//	pq_begintypsend(&buf);
//	pq_sendstring(&buf, email->local);
//	pq_sendstring(&buf, email->domain);
//	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
//}

///*****************************************************************************
// * New Operators
// *
// * A practical Complex datatype would provide much more than this, of course.
// *****************************************************************************/
//
//PG_FUNCTION_INFO_V1(email_add);
//
//Datum
//email_add(PG_FUNCTION_ARGS) {
//    Email *a = (Email *) PG_GETARG_POINTER(0);
//    Email *b = (Email *) PG_GETARG_POINTER(1);
//    Email *result;
//
//    result = (Email *) palloc(sizeof(Email));
//    /* Something need to change... */
//    result->x = a->x + b->x;
//    result->y = a->y + b->y;
//    PG_RETURN_POINTER(result);
//}
//
//
///*****************************************************************************
// * Operator class for defining B-tree index
// *
// * It's essential that the comparison operators and support function for a
// * B-tree index opclass always agree on the relative ordering of any two
// * data values.  Experience has shown that it's depressingly easy to write
// * unintentionally inconsistent functions.	One way to reduce the odds of
// * making a mistake is to make all the functions simple wrappers around
// * an internal three-way-comparison function, as we do here.
// *****************************************************************************/
//
//#define Mag(c)	((c)->x*(c)->x + (c)->y*(c)->y)
//
//static int
//complex_abs_cmp_internal(Complex * a, Complex * b)
//{
//	double		amag = Mag(a),
//				bmag = Mag(b);
//
//	if (amag < bmag)
//		return -1;
//	if (amag > bmag)
//		return 1;
//	return 0;
//}
//
//
//PG_FUNCTION_INFO_V1(complex_abs_lt);
//
//Datum
//complex_abs_lt(PG_FUNCTION_ARGS)
//{
//	Complex    *a = (Complex *) PG_GETARG_POINTER(0);
//	Complex    *b = (Complex *) PG_GETARG_POINTER(1);
//
//	PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) < 0);
//}
//
//PG_FUNCTION_INFO_V1(complex_abs_le);
//
//Datum
//complex_abs_le(PG_FUNCTION_ARGS)
//{
//	Complex    *a = (Complex *) PG_GETARG_POINTER(0);
//	Complex    *b = (Complex *) PG_GETARG_POINTER(1);
//
//	PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) <= 0);
//}
//
//PG_FUNCTION_INFO_V1(complex_abs_eq);
//
//Datum
//complex_abs_eq(PG_FUNCTION_ARGS)
//{
//	Complex    *a = (Complex *) PG_GETARG_POINTER(0);
//	Complex    *b = (Complex *) PG_GETARG_POINTER(1);
//
//	PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) == 0);
//}
//
//PG_FUNCTION_INFO_V1(complex_abs_ge);
//
//Datum
//complex_abs_ge(PG_FUNCTION_ARGS)
//{
//	Complex    *a = (Complex *) PG_GETARG_POINTER(0);
//	Complex    *b = (Complex *) PG_GETARG_POINTER(1);
//
//	PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) >= 0);
//}
//
//PG_FUNCTION_INFO_V1(complex_abs_gt);
//
//Datum
//complex_abs_gt(PG_FUNCTION_ARGS)
//{
//	Complex    *a = (Complex *) PG_GETARG_POINTER(0);
//	Complex    *b = (Complex *) PG_GETARG_POINTER(1);
//
//	PG_RETURN_BOOL(complex_abs_cmp_internal(a, b) > 0);
//}
//
//PG_FUNCTION_INFO_V1(complex_abs_cmp);
//
//Datum
//complex_abs_cmp(PG_FUNCTION_ARGS)
//{
//	Complex    *a = (Complex *) PG_GETARG_POINTER(0);
//	Complex    *b = (Complex *) PG_GETARG_POINTER(1);
//
//	PG_RETURN_INT32(complex_abs_cmp_internal(a, b));
//}
