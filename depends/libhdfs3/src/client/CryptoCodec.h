/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_CLIENT_CRYPTOCODEC_H_
#define _HDFS_LIBHDFS3_CLIENT_CRYPTOCODEC_H_

#include <string>

#include "openssl/conf.h"
#include "openssl/evp.h"
#include "openssl/err.h"
#include "FileEncryptionInfo.h"
#include "KmsClientProvider.h"

#define KEY_LENGTH_256 32
#define KEY_LENGTH_128 16

namespace Hdfs {

	enum CryptoMethod {
		DECRYPT = 0,
		ENCRYPT = 1
	};

	class CryptoCodec {
	public:
		/**
		 * Construct a CryptoCodec instance.
		 * @param encryptionInfo the encryption info of file.
		 * @param kcp a KmsClientProvider instance to get key from kms server.
		 * @param bufSize crypto buffer size.
		 */
		CryptoCodec(FileEncryptionInfo *encryptionInfo, shared_ptr<KmsClientProvider> kcp, int32_t bufSize);

		/**
		 * Destroy a CryptoCodec instance.
		 */
		virtual ~CryptoCodec();

		/**
		 * encrypt/decrypt(depends on init()) buffer data
		 * @param buffer
		 * @param size
		 * @return encrypt/decrypt result string
		 */
		virtual std::string cipher_wrap(const char * buffer, int64_t size);

		/**
		 * init CryptoCodec
		 * @param method CryptoMethod
		 * @param stream_offset 0 when open a new file; file_lenght when append a existed file
		 * @return 1 success; 0 no need(already inited); -1 failed
		 */
		virtual int init(CryptoMethod crypto_method, int64_t stream_offset = 0);

		/**
		 * Reset iv and padding value when seek file.
		 * @param crypto_method do encrypt/decrypt work according to crypto_method.
		 * @param stream_offset the offset of the current file.
		 * @return 1 sucess; -1 failed.
		 */
		virtual int resetStreamOffset(CryptoMethod crypto_method, int64_t stream_offset);

	private:

		/**
		 * Get decrypted key from kms.
		 */
		std::string getDecryptedKeyFromKms();

		/**
		 * calculate new IV for appending a existed file
		 * @param initIV
		 * @param counter
		 * @return new IV string
		 */
		std::string calculateIV(const std::string& initIV, unsigned long counter);

		shared_ptr<KmsClientProvider>	kcp;
		FileEncryptionInfo*	encryptionInfo;
		EVP_CIPHER_CTX*	cipherCtx;
		const EVP_CIPHER*	cipher;
		CryptoMethod	method;

		bool	is_init;
		int32_t	bufSize;
		int64_t	padding;
		int64_t	counter;
		std::string decryptedKey;
		uint64_t AlgorithmBlockSize;
	};

}
#endif
