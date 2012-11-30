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
 * Interfaces to SHA-256 hashing.
 */
#include "postgres.h"

#include "sha2.h"

#include "libpq/password_hash.h"
#include "libpq/pg_sha2.h"

/* RSA BSAFE only supports 10.6 */
#if defined(__darwin__)
#include <AvailabilityMacros.h>

#if !defined(MAC_OS_X_VERSION_10_6)
	#undef HAVE_RSA_BSAFE_FIPS
#else
	#if MAC_OS_X_VERSION_MAX_ALLOWED != MAC_OS_X_VERSION_10_6
		#undef HAVE_RSA_BSAFE_FIPS
	#endif
#endif

#endif /* __darwin__ */

#ifdef HAVE_RSA_BSAFE_FIPS
#include "libpq/bsafe_wrapper/rsa_bsafe_sha2.h"
#endif

static void
to_hex(uint8 b[SHA256_DIGEST_LENGTH], char *s)
{
	static const char *hex = "0123456789abcdef";
	int			q,
				w;

	for (q = 0, w = 0; q < SHA256_DIGEST_LENGTH; q++)
	{
		s[w++] = hex[(b[q] >> 4) & 0x0F];
		s[w++] = hex[b[q] & 0x0F];
	}
	s[w] = '\0';
}


bool
pg_sha256_encrypt(const char *pass, char *salt, size_t salt_len,
				  char *cryptpass)
{
	size_t passwd_len = strlen(pass);
	char *target = palloc(passwd_len + salt_len + 1);

	memcpy(target, pass, passwd_len);
	memcpy(target + passwd_len, salt, salt_len);
	target[passwd_len + salt_len] = '\0';

#ifdef HAVE_RSA_BSAFE_FIPS
	/* RSA BSAFE is FIPS 140-2 certified but only supported on some platforms */
	char digest[SHA256_DIGEST_LENGTH];

	if (!rsa_bsafe_sha_256_digest(target, digest))
		return false;

#else
	/* For platforms on which RSA BSAFE is not supported */
	SHA256_CTX ctx;
	uint8 digest[SHA256_DIGEST_LENGTH];

	/* 
	 * Users might require a FIPS compliant implementation. They can specify
	 * this by setting the password_hash_algorithm  GUC to SHA-256-FIPS.
	 */
	if (password_hash_algorithm == PASSWORD_HASH_SHA_256_FIPS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("FIPS certified SHA-256 is not supported on "
						"this platform")));
	}

	SHA256_Init(&ctx);
	SHA256_Update(&ctx, (uint8 *)target, passwd_len + salt_len);
	SHA256_Final(digest, &ctx);

#endif /* !HAVE_RSA_BSAFE_FIPS */

	strcpy(cryptpass, SHA256_PREFIX);

	to_hex((uint8 *)digest, cryptpass + strlen(SHA256_PREFIX));

	cryptpass[SHA256_PASSWD_LEN] = '\0';

	return true;
}
