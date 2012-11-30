/*
 * Copyright (c) 2011 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 *
 * ---------------------------------------------------------------------
 *
 * Generic interface to SHA-256 encryption via the FIPS-140 compliant
 * RSA BSAFE Crypto-C ME library.
 */

#include "postgres.h"
#include "miscadmin.h"

/* Only implemented if the RSA libraries are present */
#ifdef HAVE_RSA_BSAFE_FIPS
#include "libpq/bsafe_wrapper/rsa_bsafe_sha2.h"

/* For RSA BSAFE Crypto-C ME */
#include "bsafe/r_prod.h"
#include "bsafe/r_memory.h"
#include "bsafe/r_prov_fips140.h"

/*
 * Load the library.
 *
 * It's actually statically compiled but the library requires run time sanity
 * checking to resist against tampering.
 */
static int
get_provider(R_PROV **provider, R_LIB_CTX *lib_ctx)
{
	int ret = R_ERROR_NONE;
	const R_FEATURE *features[] = {&R_FIPS140_FEATURE_no_default_ecdrbg,
								   R_FEATURE_END_OF_LIST};
	char libpath[MAXPGPATH];

	get_lib_path(my_exec_path, libpath);

	ret = R_PROV_FIPS140_new(NULL, features, provider);
	if (ret != R_ERROR_NONE)
		/* out of memory */
		return ret;

	ret = R_PROV_set_info(*provider,
						  R_PROV_TYPE_CRYPTO_FIPS_140_2,
						  R_FIPS_INFO_ID_LIBRARY_PATH,
						  libpath);
	if (ret != R_ERROR_NONE)
		/* argument error */
		return ret;

	ret = R_PROV_FIPS140_load(*provider);

	/* could be a security issue, make sure we log it */
	if (ret != R_ERROR_NONE)
		elog(LOG, "failed to load RSA BSAFE FIPS-140 library in %s: %s",
			 libpath,
			 R_LIB_CTX_get_error_string(lib_ctx, R_RES_MOD_ID_LIBRARY, ret));

	return ret;
}

/*
 * Actual entry point to sha-256 hashing.
 */
bool
rsa_bsafe_sha_256_digest(const char *str, char *digest)
{
    int ret = R_ERROR_NONE;
    unsigned int alg_id;
    R_RES_LIST res_list[] = {R_LIB_RES_ERROR_STRINGS, R_RES_END_OF_LIST};
    R_LIB_CTX *lib_ctx = NULL;
    R_CR_CTX *ctx = NULL;
    R_CR *dgst_obj = NULL;
    char *string;
    unsigned char dgst_buf[R_CR_DIGEST_MAX_LEN];
    unsigned int dgst_len = sizeof(dgst_buf);
    R_PROV *provider = NULL;

    /* Initialize the library using the system defaults. */
    ret = R_STATE_init_defaults();
    if (R_ERROR_NONE != ret)
        goto end;

    /* Set the default values for the digest operation */
    alg_id = R_CR_ID_SHA256;
    string = NULL;


    /*************************************************************************
     * Step 1. Create the library context.
     * Create a library context to provide access to all configurable aspects
     * of the library.
     *************************************************************************/
    if ((ret = R_LIB_CTX_new(res_list, 0, &lib_ctx)) !=
        R_ERROR_NONE)
    {
		/* would just be a memory error */
        goto end;
    }

    /*************************************************************************
     * Step 1a. Get and add the provider of cryptographic implementations.
     *************************************************************************/
    ret = get_provider(&provider, lib_ctx);
    if (ret != R_ERROR_NONE)
    {
		/* see LOG messages in get_provider() */
        goto end;
    }
    ret = R_LIB_CTX_add_provider(lib_ctx, provider);
    if (ret != R_ERROR_NONE)
    {
        goto end;
    }

    /*************************************************************************
     * Step 2. Create a cryptographic context.
     *************************************************************************/
    if ((ret = R_CR_CTX_new(lib_ctx, 0, &ctx)) != R_ERROR_NONE)
    {
        goto end;
    }

    /*************************************************************************
     * Step 3. Create a message digest cryptographic object using the
     * algorithm specified on the command line.
     *************************************************************************/
    if ((ret = R_CR_new(ctx, R_CR_TYPE_DIGEST, alg_id, R_CR_SUB_NONE,
        &dgst_obj)) != R_ERROR_NONE)
    {
        goto end;
    }

    /*************************************************************************
     * Step 4. Initialize the message digest object.
     *************************************************************************/
    if ((ret = R_CR_digest_init(dgst_obj)) != R_ERROR_NONE)
    {
        goto end;
    }

    /*************************************************************************
     * Step 5. Generate the message digest of the data.
     * The data is available in a single block so only one digest update call
     * is required. If the message consists of multiple parts then multiple
     * calls to R_CR_digest_update() must be be made.
     *************************************************************************/
    if ((ret = R_CR_digest_update(dgst_obj, (unsigned char *)str,
        (unsigned int)strlen(str))) != R_ERROR_NONE)
    {
        goto end;
    }

    /*************************************************************************
     * Step 6. Return the message digest of the data.
     *************************************************************************/
    if ((ret = R_CR_digest_final(dgst_obj, dgst_buf, &dgst_len)) !=
        R_ERROR_NONE)
    {
        goto end;
    }

	memcpy(digest, dgst_buf, dgst_len);
	digest[dgst_len] = '\0';

end:

    /*************************************************************************
     * Step 7.  Clean up.
     * Report errors if there is an output stream using both the error and the
     * string representation.
     * Destroy the dynamically allocated objects and return an exit code.
     *************************************************************************/

    R_CR_free(dgst_obj);
    R_CR_CTX_free(ctx);
    R_PROV_free(provider);
    R_LIB_CTX_free(lib_ctx);

    R_STATE_cleanup();

	return (ret == R_ERROR_NONE);
}
#endif /* !HAVE_RSA_BSAFE_FIPS */
