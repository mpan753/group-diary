/*
 * src/tutorial/email.c
 * written by Lujie Wang & Meng Pan
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/
#define MAXLEN 128

#include "postgres.h"
#include "libpq/pqformat.h"
#include "string.h"
#include "fmgr.h"
#include "libpq/pqformat.h"     /* needed for send/recv functions */
#include <regex.h>
#include <sys/types.h>
#include "catalog/pg_type.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

typedef struct Email{
    char local[MAXLEN];
    char domain[MAXLEN];
} Email;


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

Datum pjw(PG_FUNCTION_ARGS);
int PJWHash(char *); 

void strlwr(char *); // lower all string characters
bool is_valid_email(char *); // check if the email is valid
bool is_valid_local(char *); // check if the local part is valid
bool is_valid_domain(char *); // check if the domian part is valid
bool is_name_part(char **); // check if the name part is valid
bool is_name_parts(char **); // check if the name parts is valid
bool is_name_chars(char **); // check if the name chars is valid

/*
    Datum email_in(PG_FUNCTION_ARGS); // input function
    Datum email_out(PG_FUNCTION_ARGS); // output function
    Datum email_recv(PG_FUNCTION_ARGS); // binary input function
    Datum email_send(PG_FUNCTION_ARGS); // binary output funtion
    Datum email_abs_lt(PG_FUNCTION_ARGS); // <: less than function
    Datum email_abs_le(PG_FUNCTION_ARGS); // <=: less or equal than function
    Datum email_abs_eq(PG_FUNCTION_ARGS); // =: absolutely equal function
    Datum email_abs_ge(PG_FUNCTION_ARGS); // >=: greater or equal than function
    Datum email_abs_gt(PG_FUNCTION_ARGS); // >: greater than function
    Datum email_abs_ne(PG_FUNCTION_ARGS); // !=: not equal function
    Datum email_abs_same_domain(PG_FUNCTION_ARGS); // ~ : domain is same
    Datum email_abs_not_same_domain(PG_FUNCTION_ARGS); // !~ : domain not same
    Datum email_abs_cmp(PG_FUNCTION_ARGS); // comparator for btree
    Datum pjw(PG_FUNCTION_ARGS); // hash 
*/


/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(email_in);

Datum
email_in(PG_FUNCTION_ARGS) {
    char *str = PG_GETARG_CSTRING(0);
    Email *result;

    char email[strlen(str)+1];
    strcpy(email, str);
    strlwr(email);
    email[strlen(str)] = '\0';


    if ( !is_valid_email(email) ) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for email: \"%s\"", str)));
    }

    result = (Email *) palloc(sizeof(Email));
    sscanf(email, "%[-a-z0-9.]@%[-a-z0-9.]", result->local, result->domain);
    
    if ((strlen(result->local) > MAXLEN) || (strlen(result->domain) > MAXLEN)) {
     ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("the input should be less than 257 characters.")));
    }

    PG_RETURN_POINTER(result);
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
    StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
    Email *result;

    result = (Email *) palloc(sizeof(Email));
    strcpy(result->local, pq_getmsgstring(buf));
    strcpy(result->domain, pq_getmsgstring(buf));

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
 * unintentionally inconsistent functions.  One way to reduce the odds of
 * making a mistake is to make all the functions simple wrappers around
 * an internal three-way-comparison function, as we do here.
 *****************************************************************************/

static int
internal_same_domain(Email *a, Email *b) {
    if (!strcmp(a->domain, b->domain))
        return 1;
    return 0;
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

    PG_RETURN_BOOL(internal_same_domain(a, b) == 1);
}

PG_FUNCTION_INFO_V1(email_abs_not_same_domain);

Datum
email_abs_not_same_domain(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    Email *b = (Email *) PG_GETARG_POINTER(1);

    PG_RETURN_BOOL(internal_same_domain(a, b) == 0);
}

PG_FUNCTION_INFO_V1(pjw);

Datum
pjw(PG_FUNCTION_ARGS) {
    Email *a = (Email *) PG_GETARG_POINTER(0);
    char *local = a->local;
    char *domain = a->domain;
    
    int local_hash = PJWHash(local);
    int domain_hash = PJWHash(domain);

    int hash = 2 * local_hash + domain_hash;
    PG_RETURN_INT32(hash);
}
/* The actual hash algorithm, taken from website */
int PJWHash(char *str) {  
    long BitsInUnsignedInt = (long)(4 * 8);  
    long ThreeQuarters     = (long)((BitsInUnsignedInt  * 3) / 4);  
    long OneEighth         = (long)(BitsInUnsignedInt / 8);  
    long HighBits          = (long)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);  
    long hash              = 0;  
    long test              = 0;
    int i;
    for(i = 0; i < strlen(str); i++) {  
        hash = (hash << OneEighth) + str[i];  
        if((test = hash & HighBits)  != 0) {  
            hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));  
        }  
    }  
    return (int)hash;  
}  

/* This will tolower all characters in a string */
void strlwr(char *string) {
    size_t len = strlen(string);
    int i;
    for (i = 0; i < len; ++i)
    {
        if (isalpha(string[i]))
        {
            string[i] = ((unsigned int)tolower(string[i]));
        } 
    }
}
/***********************************************************************
* EmailAddress ::= Local '@' Domain
*
* Local        ::= NamePart NameParts
*
* Domain       ::= NamePart '.' NamePart NameParts
*
* NamePart     ::= Letter | Letter NameChars (Letter|Digit)
*
* NameParts    ::= Empty | '.' NamePart NameParts
*
* NameChars    ::= Empty | (Letter|Digit|'-') NameChars
*
* Letter       ::= 'a' | 'b' | ... | 'z' | 'A' | 'B' | ... 'Z'
* Digit        ::= '0' | '1' | '2' | ... | '8' | '9'
************************************************************************/
bool is_valid_email(char email[]) {
    char *local = (char *) palloc(sizeof(char *));
    char *domain = (char *) palloc(sizeof(char *));
    char *reg_exp_mail = "^[a-z0-9.-]+@[a-z0-9.-]+$";
    regex_t regex;
    int pre;
    pre = regcomp(&regex,reg_exp_mail,1);
    pre = regexec(&regex, email, 0, NULL, 0);
    if(pre){ // invalid email regex
        return false;
    }
    regfree (&regex);

    if ( sscanf(email, "%[-a-zA-Z0-9.]@%s", local, domain) != 2 ) {
        return false;
    }

    if (!is_valid_local(local)) {
        return false;
    }
    if(!is_valid_domain(domain)) {
        return false;
    }
    return true;
}

bool is_valid_local(char *loc) {
    if (!is_name_part(&loc)) {
        return false;
    }
    if (!is_name_parts(&loc)){
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
