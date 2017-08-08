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

#include "CryptoCodec.h"
#include "Logger.h"

using namespace Hdfs::Internal;


namespace Hdfs {

	//copy from java HDFS code
	std::string CryptoCodec::calculateIV(const std::string& initIV, unsigned long counter) {
		char IV[initIV.length()];

		int i = initIV.length(); // IV length
		int j = 0; // counter bytes index
		unsigned int sum = 0;
		while (i-- > 0) {
			// (sum >>> Byte.SIZE) is the carry for addition
			sum = (initIV[i] & 0xff) + (sum >> 8);
			if (j++ < 8) { // Big-endian, and long is 8 bytes length
				sum += (char) counter & 0xff;
				counter >>= 8;
			}
			IV[i] = (char) sum;
		}

		return std::string(IV, initIV.length());
	}

	CryptoCodec::CryptoCodec(FileEncryptionInfo *encryptionInfo, shared_ptr<KmsClientProvider> kcp, int32_t bufSize) :
		encryptionInfo(encryptionInfo), kcp(kcp), bufSize(bufSize)
	{

		// Init global status
		ERR_load_crypto_strings();
		OpenSSL_add_all_algorithms();
		OPENSSL_config(NULL);

		// Create cipher context
		cipherCtx = EVP_CIPHER_CTX_new();
		cipher = NULL;

		padding = 0;
		counter = 0;
		is_init = false;
	}

	CryptoCodec::~CryptoCodec()
	{
		if (cipherCtx)
			EVP_CIPHER_CTX_free(cipherCtx);
	}

	std::string CryptoCodec::getDecryptedKeyFromKms()
	{
		ptree map = kcp->decryptEncryptedKey(*encryptionInfo);
		std::string key;
		try {
			key = map.get < std::string > ("material");
		} catch (...) {
			THROW(HdfsIOException, "CryptoCodec : Can not get key from kms.");
		}

		int rem = key.length() % 4;
		if (rem) {
			rem = 4 - rem;
			while (rem != 0) {
				key = key + "=";
				rem--;
			}
		}

		std::replace(key.begin(), key.end(), '-', '+');
		std::replace(key.begin(), key.end(), '_', '/');

		LOG(DEBUG3, "CryptoCodec : getDecryptedKeyFromKms material is :%s", key.c_str());

		key = KmsClientProvider::base64Decode(key);
		return key;
	}

	int CryptoCodec::init(CryptoMethod crypto_method, int64_t stream_offset) {
		// Check CryptoCodec init or not.
		if (is_init)
			return 0;

		// Get decrypted key from KMS.
		decryptedKey = getDecryptedKeyFromKms();

		// Select cipher method based on the decrypted key length.
		AlgorithmBlockSize = decryptedKey.length();
		if (AlgorithmBlockSize == KEY_LENGTH_256) {
			cipher = EVP_aes_256_ctr();
		} else if (AlgorithmBlockSize == KEY_LENGTH_128) {
			cipher = EVP_aes_128_ctr();
		} else {
			LOG(WARNING, "CryptoCodec : Invalid key length.");
			return -1;
		}

		is_init = true;
		// Calculate iv and counter in order to init cipher context with cipher method. Default value is 0.
		if ((resetStreamOffset(crypto_method, stream_offset)) < 0) {
			is_init = false;
			return -1;
		}

		LOG(DEBUG3, "CryptoCodec init success, length of the decrypted key is : %llu, crypto method is : %d", AlgorithmBlockSize, crypto_method);
		return 1;

	}

	int CryptoCodec::resetStreamOffset(CryptoMethod crypto_method, int64_t stream_offset) {
		// Check CryptoCodec init or not.
		if (is_init == false)
			return -1;
		// Calculate new IV when appending an existed file.
		std::string iv = encryptionInfo->getIv();
		if (stream_offset > 0) {
			counter = stream_offset / AlgorithmBlockSize;
			padding = stream_offset % AlgorithmBlockSize;
			iv = this->calculateIV(iv, counter);
		}

		// Judge the crypto method is encrypt or decrypt.
		int enc = (method == CryptoMethod::ENCRYPT) ? 1 : 0;

		// Init cipher context with cipher method.
		if (!EVP_CipherInit_ex(cipherCtx, cipher, NULL,
				(const unsigned char *) decryptedKey.c_str(), (const unsigned char *) iv.c_str(),
				enc)) {
			LOG(WARNING, "EVP_CipherInit_ex failed");
			return -1;
		}

		// AES/CTR/NoPadding, set padding to 0.
		EVP_CIPHER_CTX_set_padding(cipherCtx, 0);

		return 1;
	}

	std::string CryptoCodec::cipher_wrap(const char * buffer, int64_t size) {
		if (!is_init)
			THROW(InvalidParameter, "CryptoCodec isn't init");

		int offset = 0;
		int remaining = size;
		int len = 0;
		int ret = 0;

		std::string in_buf(buffer,size);
		std::string out_buf(size, 0);
		//set necessary padding when appending a existed file
		if (padding > 0) {
			in_buf.insert(0, padding, 0);
			out_buf.resize(padding+size);
			remaining += padding;
		}

		// If the encode/decode buffer size larger than crypto buffer size, encode/decode buffer one by one
		while (remaining > bufSize) {
			ret = EVP_CipherUpdate(cipherCtx, (unsigned char *) &out_buf[offset], &len, 
				(const unsigned char *)in_buf.data() + offset, bufSize);

			if (!ret) {
				std::string err = ERR_lib_error_string(ERR_get_error());
				THROW(HdfsIOException, "CryptoCodec : cipher_wrap AES data failed:%s, crypto_method:%d", err.c_str(), method);
			}
			offset += len;
			remaining -= len;
			LOG(DEBUG3, "CryptoCodec : EVP_CipherUpdate successfully, len:%d", len);
		}

		if (remaining) {
			ret = EVP_CipherUpdate(cipherCtx, (unsigned char *) &out_buf[offset], &len,
				(const unsigned char *) in_buf.data() + offset, remaining);

			if (!ret) {
				std::string err = ERR_lib_error_string(ERR_get_error());
				THROW(HdfsIOException, "CryptoCodec : cipher_wrap AES data failed:%s, crypto_method:%d", err.c_str(), method);
			}

		}

		//cut off padding when necessary
		if (padding > 0) {
			out_buf.erase(0, padding);
			padding = 0;
		}

		return out_buf;
	}

}

