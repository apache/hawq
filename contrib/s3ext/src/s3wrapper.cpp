#include <sstream>
#include <string>

#include "gps3ext.h"
#include "s3conf.h"
#include "s3log.h"
#include "s3utils.h"
#include "s3wrapper.h"

using std::string;
using std::stringstream;

// invoked by s3_import(), need to be exception safe
S3ExtBase *CreateExtWrapper(const char *url) {
    try {
        return new S3Reader(url);
    } catch (...) {
        S3ERROR("Caught an exception, aborting");
        return NULL;
    }
}

S3ExtBase::S3ExtBase(const string &url) {
    this->url = url;

    // get following from config
    this->cred.secret = s3ext_secret;
    this->cred.keyid = s3ext_accessid;

    this->segid = s3ext_segid;
    this->segnum = s3ext_segnum;

    this->chunksize = s3ext_chunksize;
    this->concurrent_num = s3ext_threadnum;

    S3INFO("Created %d threads for downloading", s3ext_threadnum);
    S3INFO("File is splited to %d each", s3ext_chunksize);
}

S3ExtBase::~S3ExtBase() {}

S3Reader::~S3Reader() {}

S3Reader::S3Reader(const string &url) : S3ExtBase(url) {
    this->contentindex = -1;
    this->filedownloader = NULL;
    this->keylist = NULL;
}

// invoked by s3_import(), need to be exception safe
bool S3Reader::Init(int segid, int segnum, int chunksize) {
    try {
        // set segment id and num
        this->segid = s3ext_segid;
        this->segnum = s3ext_segnum;
        this->contentindex = this->segid;

        this->chunksize = chunksize;

        // Validate url first
        if (!this->ValidateURL()) {
            S3ERROR("The given URL(%s) is invalid", this->url.c_str());
            return false;
        }

        int initretry = 3;
        while (initretry--) {
            this->keylist = ListBucket(this->schema, this->region, this->bucket,
                                       this->prefix, this->cred);

            if (!this->keylist) {
                S3INFO("Can't get keylist from bucket %s",
                       this->bucket.c_str());
                if (initretry) {
                    S3INFO("Retrying");
                    continue;
                } else {
                    S3ERROR(
                        "Quit initialization because ListBucket keeps failing");
                    return false;
                }
            }

            if (this->keylist->contents.size() == 0) {
                S3INFO("Keylist of bucket is empty");
                if (initretry) {
                    S3INFO("Retry listing bucket");
                    delete this->keylist;
                    this->keylist = NULL;
                    continue;
                } else {
                    S3ERROR("Quit initialization because keylist is empty");
                    return false;
                }
            }
            break;
        }
        S3INFO("Got %d files to download", this->keylist->contents.size());
        if (!this->getNextDownloader()) {
            return false;
        }
    } catch (...) {
        S3ERROR("Caught an exception, aborting");
        return false;
    }

    // return this->filedownloader ? true : false;
    return true;
}

bool S3Reader::getNextDownloader() {
    if (this->filedownloader) {  // reset old downloader
        filedownloader->destroy();
        delete this->filedownloader;
        this->filedownloader = NULL;
    }

    if (this->contentindex >= this->keylist->contents.size()) {
        S3DEBUG("No more files to download");
        return true;
    }

    if (this->concurrent_num > 0) {
        this->filedownloader = new Downloader(this->concurrent_num);
    } else {
        S3ERROR("Failed to create filedownloader due to threadnum");
        return false;
    }

    if (!this->filedownloader) {
        S3ERROR("Failed to create filedownloader");
        return false;
    }
    BucketContent *c = this->keylist->contents[this->contentindex];
    string keyurl = this->getKeyURL(c->Key());
    S3DEBUG("key: %s, size: %llu", keyurl.c_str(), c->Size());

    if (!filedownloader->init(keyurl, this->region, c->Size(), this->chunksize,
                              &this->cred)) {
        delete this->filedownloader;
        this->filedownloader = NULL;
        return false;
    } else {  // move to next file
        // for now, every segment downloads its assigned files(mod)
        // better to build a workqueue in case not all segments are available
        this->contentindex += this->segnum;
    }

    return true;
}

string S3Reader::getKeyURL(const string &key) {
    stringstream sstr;
    sstr << this->schema << "://"
         << "s3-" << this->region << ".amazonaws.com/";
    sstr << this->bucket << "/" << key;
    return sstr.str();
}

// invoked by s3_import(), need to be exception safe
bool S3Reader::TransferData(char *data, uint64_t &len) {
    try {
        if (!this->filedownloader) {
            S3INFO("No files to download, exit");
            // not initialized?
            len = 0;
            return true;
        }
        uint64_t buflen;
    RETRY:
        buflen = len;
        // S3DEBUG("getlen is %d", len);
        bool result = filedownloader->get(data, buflen);
        if (!result) {  // read fail
            S3ERROR("Failed to get data from filedownloader");
            return false;
        }
        // S3DEBUG("getlen is %lld", buflen);
        if (buflen == 0) {
            // change to next downloader
            if (!this->getNextDownloader()) {
                return false;
            }

            if (this->filedownloader) {  // download next file
                S3INFO("Time to download new file");
                goto RETRY;
            }
        }
        len = buflen;
    } catch (...) {
        S3ERROR("Caught an exception, aborting");
        return false;
    }

    return true;
}

// invoked by s3_import(), need to be exception safe
bool S3Reader::Destroy() {
    try {
        if (this->filedownloader) {
            this->filedownloader->destroy();
            delete this->filedownloader;
            this->filedownloader = NULL;
        }

        if (this->keylist) {
            delete this->keylist;
            this->keylist = NULL;
        }
    } catch (...) {
        S3ERROR("Caught an exception, aborting");
        return false;
    }

    return true;
}

bool S3ExtBase::ValidateURL() {
    // http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region

    const char *awsdomain = ".amazonaws.com";
    size_t ibegin = 0;
    size_t iend = url.find("://");
    if (iend == string::npos) {  // Error
        return false;
    }

    this->schema = url.substr(ibegin, iend);
    if (this->schema == "s3") {
        if (s3ext_encryption)
            this->schema = "https";
        else
            this->schema = "http";
    }

    ibegin = url.find("://s3") + 4;  // "3"
    iend = url.find(awsdomain);

    if (iend == string::npos) {
        return false;
    } else if (ibegin + 1 == iend) {  // "s3.amazonaws.com"
        this->region = "external-1";
    } else {
        this->region = url.substr(ibegin + 2, iend - ibegin - 2);
    }

    if (this->region.compare("us-east-1") == 0) {
        this->region = "external-1";
    }

    ibegin = find_Nth(url, 3, "/");
    iend = find_Nth(url, 4, "/");
    if ((iend == string::npos) || (ibegin == string::npos)) {
        return false;
    }
    this->bucket = url.substr(ibegin + 1, iend - ibegin - 1);

    this->prefix = url.substr(iend + 1, url.length() - iend - 1);

    return true;
}

/*
bool S3Protocol_t::Write(char *data, size_t &len) {
    if (!this->fileuploader) {
        // not initialized?
        return false;
    }

    bool result = fileuploader->write(data, len);
    if (!result) {
        S3DEBUG("Failed to write data via fileuploader");
        return false;
    }

    return true;
}
*/

// invoked by s3_import(), need to be exception safe
S3Reader *reader_init(const char *url_with_options) {
    try {
        if (!url_with_options) {
            return NULL;
        }

        curl_global_init(CURL_GLOBAL_ALL);

        char *url = truncate_options(url_with_options);
        if (!url) {
            return NULL;
        }

        char *config_path = get_opt_s3(url_with_options, "config");
        if (!config_path) {
            // no config path in url, use default value
            // data_folder/gpseg0/s3/s3.conf
            config_path = strdup("s3/s3.conf");
        }

        bool result = InitConfig(config_path, "default");
        if (!result) {
            free(url);
            free(config_path);
            return NULL;
        } else {
            free(config_path);
        }

        InitRemoteLog();

        S3Reader *reader = (S3Reader *)CreateExtWrapper(url);

        free(url);

        if (!reader) {
            return NULL;
        }

        if (!reader->Init(s3ext_segid, s3ext_segnum, s3ext_chunksize)) {
            reader->Destroy();
            delete reader;
            return NULL;
        }

        return reader;
    } catch (...) {
        S3ERROR("Caught an exception, aborting");
        return NULL;
    }
}

// invoked by s3_import(), need to be exception safe
bool reader_transfer_data(S3Reader *reader, char *data_buf, int &data_len) {
    try {
        if (!reader || !data_buf || (data_len < 0)) {
            return false;
        }

        if (data_len == 0) {
            return true;
        }

        uint64_t read_len = data_len;
        if (!reader->TransferData(data_buf, read_len)) {
            return false;
        }

        // sure read_len <= data_len here, hence truncation will never happen
        data_len = (int)read_len;
    } catch (...) {
        S3ERROR("Caught an exception, aborting");
        return false;
    }

    return true;
}

// invoked by s3_import(), need to be exception safe
bool reader_cleanup(S3Reader **reader) {
    try {
        if (*reader) {
            if (!(*reader)->Destroy()) {
                delete *reader;
                *reader = NULL;
                return false;
            }

            delete *reader;
            *reader = NULL;
        } else {
            return false;
        }

        /*
         * Cleanup function for the XML library.
         */
        xmlCleanupParser();
    } catch (...) {
        S3ERROR("Caught an exception, aborting");
        return false;
    }

    return true;
}
