/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "cdb/cdbparquetfooterbuffer.h"

#define PARQUET_FOOTER_TEMPFILE_PREFIX  "footer_buffered_write"
#define PARQUET_FOOTER_TEMPFILE_SEQ     1

#define ASSERT_IS_A_READING_ONLY_BUFFER(buffer) \
        Assert( (buffer) != NULL && \
                (buffer)->Mode == PARQUET_FOOTER_BUFFERMODE_READ)

#define ASSERT_IS_A_WRITING_ONLY_BUFFER(buffer) \
        Assert( (buffer) != NULL && \
                (buffer)->Mode == PARQUET_FOOTER_BUFFERMODE_WRITE)

void fillBufferFromFile(File file, char *filename, uint8_t *buffer, int size);
void flushBufferToFile(File file, char *filename, uint8_t *buffer, int size);
int  fillBufferFromTempFile(File file, uint8_t *buffer, int size);

char ModeStringConstant[3][6] = {"ZERO","READ","WRITE"};


/*******************************************************************************
 * External function implementation.
 ******************************************************************************/

/*
 * Create one parquet footer buffer.
 *
 * @datafile        The footer file handler.
 * @filename        The filename of the footer file.
 * @footerlength    The total length of the footer data to read.
 * @capacity        The capacity of the buffer.
 * @mode            If the buffer is for reading or writing.
 *                  PARQUET_FOOTER_BUFFERMODE_READ  for reading,
 *                  PARQUET_FOOTER_BUFFERMODE_WRITE for write.
 *
 * Return created footer buffer instance.
 */
ParquetFooterBuffer *createParquetFooterBuffer( File  datafile, 
												char *filename,
                                                int   footerlength, 
                                                int   capacity,
                                                int   mode)
{
	/* Create new instance. memset to 0 is redundant, just for safety. */
	ParquetFooterBuffer *res = (ParquetFooterBuffer *)
							   palloc0(sizeof(ParquetFooterBuffer));

    /* Set the properties. */
    res->Mode            = mode;
    res->FileHandler     = datafile;
    res->ValidLen        = 0;
    res->Cursor          = -1;
    res->FooterProcessed = 0;
    res->FileName        = (char *)palloc(sizeof(char)*(strlen(filename)+1));
    strcpy(res->FileName, filename);
    
    if ( mode == PARQUET_FOOTER_BUFFERMODE_READ ) {
        res->FooterLength 	 = footerlength;
        res->TmpFile         = -1;
        /* Create buffer. If the footer is small, 
         * no need to acquire more space. */
        res->Capacity = capacity > footerlength ? footerlength : capacity;
        res->Head = (uint8_t *)palloc(sizeof(char)*res->Capacity);
        
    }
    else if ( mode == PARQUET_FOOTER_BUFFERMODE_WRITE ) {
        res->FooterLength 	 = -1;
        res->Capacity        = capacity;
        /* Create buffer. */
        res->Head = (uint8_t *)palloc((res->Capacity) * sizeof(char));
        res->TmpFile         = OpenTemporaryFile( PARQUET_FOOTER_TEMPFILE_PREFIX,
                                                  PARQUET_FOOTER_TEMPFILE_SEQ,
                                                  true,
                                                  true,
                                                  true,
                                                  true );
        
        /* NOTE: If the temporary file can not be created, 
         *       the error is raised by OpenTemporaryFile */
    }
    
    elog(DEBUG5, "Create parquet footer buffer. "
                 "INSTADDR=%p, "
                 "MODE=%s, "
                 "FILENAME=%s, "
                 "FOOTERLENGTH=%d, "
                 "CAPACITY=%d, "
                 "TMPFILEHANDLER=%d",
                 res,
                 ModeStringConstant[res->Mode],
                 res->FileName,
                 res->FooterLength,
                 res->Capacity,
                 res->TmpFile );
    
	return res;
}

/*
 * Free one parquet footer buffer.
 *
 * @buffer      The buffer to free. 
 */
void freeParquetFooterBuffer( ParquetFooterBuffer *buffer ) 
{
    elog(DEBUG5, "Free parquet footer buffer."
                 "INSTADDR=%p, "
                 "MODE=%s",
                 buffer,
                 ModeStringConstant[buffer->Mode] );

	/* Free allocated spaces */
	pfree(buffer->Head);
	pfree(buffer->FileName);
    
    /* Close temporary file which will automatically be deleted. */
    if ( buffer->TmpFile >= 0 ) {
        FileClose(buffer->TmpFile);
    }
    
    /* Free whole buffer instance. */
	pfree(buffer);
}

/*
 * Prepare to read specified amount of the bytes.
 *
 * After calling this function, caller can use (*ptr) to access a memory of size
 * length to read the data. Buffer handles forward only reading automatically.
 *
 * @buffer      The buffer to read the file content.
 * @length		The length of the data expected to read after calling.
 * @ptr        	The pointer to the valid memory for the prepared data.
 *
 * return		PARQUET_FOOTER_BUFFER_MOVE_OK if succeed. 
 *				Otherwise, PARQUET_FOOTER_BUFFER_MOVE_ERROR.
 */
int prepareFooterBufferForwardReading( ParquetFooterBuffer  *buffer,
                                       int                   length,
                                       uint8_t             **ptr )
{
	int loadsize = -1;

	Assert( buffer != NULL );
    ASSERT_IS_A_READING_ONLY_BUFFER(buffer);
	Assert( ptr != NULL );
	Assert( length > 0 );

	/* Check if too large data to prepare. This is not one valid request. */
	if ( length > buffer->Capacity ) {
		return PARQUET_FOOTER_BUFFER_MOVE_ERROR;
	}

	/* If all content is consumed already, should load data into buffer. */
	if ( buffer->Cursor == -1 ||
		 buffer->Cursor >= buffer->ValidLen ) {
		loadsize = buffer->FooterLength - buffer->FooterProcessed;
		loadsize = loadsize < buffer->Capacity ? loadsize : buffer->Capacity;

		fillBufferFromFile( buffer->FileHandler,
                            buffer->FileName,
                            buffer->Head,
                            loadsize );

		buffer->Cursor           = 0;
		buffer->ValidLen         = loadsize;
		buffer->FooterProcessed += loadsize;
        
        elog(DEBUG5, "Reset cursor back. "
                     "INSTADDR=%p, "
                     "ValidContentLength=%d, "
                     "ProcessedFooter=%d",
                     buffer,
                     buffer->ValidLen,
                     buffer->FooterProcessed );
        
	} 

	/* Check if should move memory content. */
	if ( buffer->Cursor >= 0 && buffer->Cursor < buffer->Capacity &&
		 buffer->ValidLen >= buffer->Cursor + length ) {
		/* Start from current buffer pointer , the caller can directly read 
		 * expected amount of bytes. */
		*ptr = buffer->Head + buffer->Cursor;
		buffer->Cursor += length;
        
        elog(DEBUG5, "No read action. "
                     "INSTADDR=%p, "
                     "ValidContentLength=%d, "
                     "ProcessedFooter=%d",
                     buffer,
                     buffer->ValidLen,
                     buffer->FooterProcessed );
        
		return PARQUET_FOOTER_BUFFER_MOVE_OK;
	}

	/* Move the data to the head, and make cursor start from the head. */
	memmove( buffer->Head, 
			 buffer->Head + buffer->Cursor, 
			 buffer->ValidLen - buffer->Cursor );

    elog(DEBUG5, "Shift %d bytes memory from offset %d to 0. "
                 "INSTADDR=%p.",
                 buffer->ValidLen - buffer->Cursor,
                 buffer->Cursor,
                 buffer);
    
	buffer->ValidLen = buffer->ValidLen - buffer->Cursor;
	buffer->Cursor   = 0;

	/* Keep loading data to fill the buffer. */
	loadsize = buffer->FooterLength - buffer->FooterProcessed;
	loadsize = loadsize < buffer->Capacity - buffer->ValidLen ?
			   loadsize :
			   buffer->Capacity - buffer->ValidLen;

	fillBufferFromFile( buffer->FileHandler,
                        buffer->FileName,
                        buffer->Head + buffer->ValidLen,
                        loadsize );

	buffer->ValidLen += loadsize;
	buffer->FooterProcessed += loadsize;

	/* If after loading data, there are still not enough data, return error. */
	if ( buffer->ValidLen < length )
		return PARQUET_FOOTER_BUFFER_MOVE_ERROR;
	
	*ptr = buffer->Head;
	buffer->Cursor += length;
	return PARQUET_FOOTER_BUFFER_MOVE_OK;
}

/*
 * Flush all the data from the memory buffer to temporary file.
 *
 * @buffer      The buffer to flush.
 */
void flushFooterBufferToTempFile( ParquetFooterBuffer *buffer )
{
    ASSERT_IS_A_WRITING_ONLY_BUFFER(buffer);
    Assert( buffer->TmpFile >= 0 );
    
    /* If no data saved in the buffer, no need to flush. */
    if ( buffer->Cursor == -1 )
        return;
    
    int writesize = FileWrite( buffer->TmpFile,
                              (char *)(buffer->Head),
                              buffer->Cursor+1 );
    if ( writesize != buffer->Cursor+1 ) {
        /*ereport error*/
        ereport(ERROR,
                (errcode(ERRCODE_IO_ERROR),
                 errmsg("Parquet Storage Write error on temporary file:"
                        "writing footer tempaorary file failure")));
    }
    
    buffer->FooterProcessed += buffer->Cursor+1;
    buffer->Cursor = -1;
    
    elog(DEBUG5, "Flush %d bytes from buffer to temporary file. "
                 "Total flushed %d bytes. "
                 "INSTADDR=%p. ",
                 writesize,
                 buffer->FooterProcessed,
                 buffer );
    
}

/*
 * Write data into the buffer which in fact is one temporary file.
 *
 * @buffer      The buffer to write data to
 * @content     The data to be copied into the buffer
 * @length      The length of the data to be copied.
 */
void writeToFooterBuffer( ParquetFooterBuffer *buffer,
                          uint8_t             *content,
                          int                  length)
{
    int totallen = length;
    
    ASSERT_IS_A_WRITING_ONLY_BUFFER(buffer);
    Assert( buffer->TmpFile >= 0 );
    
    
    while ( length > 0 ) {
    
        /* Decide how long data to write into this buffer. */
        int bufferwritelen = buffer->Capacity - buffer->Cursor -1;
        bufferwritelen = bufferwritelen > length ? length : bufferwritelen;
    
        /* Write to buffer */
        memcpy((void *)(buffer->Head + buffer->Cursor + 1),
               (void *)content,
               bufferwritelen );

        buffer->Cursor += bufferwritelen;
    
        /* Check if should flush the content to temporary file. */
        if ( buffer->Cursor >= buffer->Capacity - 1 ) {
             flushFooterBufferToTempFile(buffer);
        }
        
        /* reset content length and index for writting*/
        length -= bufferwritelen;
        content = content + bufferwritelen;
    }
    
    elog(DEBUG5, "Write %d bytes into buffer. "
                 "Total flushed %d bytes. "
                 "INSTADDR=%p. ",
                 totallen,
                 buffer->FooterProcessed,
                 buffer );
    
}

/*
 * Append temporary file content directly to the real target file.
 * 
 * After calling this function, the buffer can not be used to buffer data any
 * more, this function does return the buffer to a consistent status back. So
 * should call freeParquetFooterBuffer() to free the buffer instance.
 *
 * @file        The file handler to write
 */
int appendFooterBufferTempData (File targetFile, ParquetFooterBuffer *buffer )
{
    int appended  = 0;
    int readsize  = 0;
    
    ASSERT_IS_A_WRITING_ONLY_BUFFER(buffer);
    Assert( buffer->TmpFile >= 0 );

    /* Flush the buffer to ensure all are writen to the temporary file. */
    flushFooterBufferToTempFile(buffer);
    
    /* Seek to the beginning of the tempoarary file. */
    if(FileSeek( buffer->TmpFile, 0, SEEK_SET ) != 0){
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR), errmsg("Parquet Storage write "
						"error on segment file '%s': " "writing footer failure",
						buffer->FileName)));
    }
    
    while ( appended < buffer->FooterProcessed ) {
        /* Read data into the buffer */
        readsize = fillBufferFromTempFile(buffer->TmpFile,
                                          buffer->Head,
                                          buffer->Capacity);
        /* Write data into the target file */
        flushBufferToFile(targetFile,
                          buffer->FileName,
                          buffer->Head,
                          readsize);
        appended += readsize;
    }
    
    elog(DEBUG5, "Append temporary data to segment file '%s'."
                 "Total appended %d bytes."
                 "INSTADDR=%p. ",
                 buffer->FileName,
                 appended,
                 buffer );
    
    return buffer->FooterProcessed;
}

/*******************************************************************************
 * Internal function implementation.
 ******************************************************************************/
/*
 * Read data from file to the buffer.
 *
 * @file		The file handler to read
 * @filename	The file name string for generating error report
 * @buffer		The buffer to fill data into. Always sufficient to contain data.
 * @size		The data size to read. Always can read as many as size bytes.
 */
void fillBufferFromFile(File file, char *filename, uint8_t *buffer, int size)
{
	int actualreadsize  = 0;
	int readsize 		= 0;
	while( actualreadsize < size ) {
		readsize = FileRead( file,
                            (char *)(buffer) + actualreadsize,
                            size - actualreadsize);
		if ( readsize < 0 ) {
			/*ereport error*/
            ereport(ERROR,
                    (errcode(ERRCODE_IO_ERROR),
                     errmsg("Parquet Storage Read error on segment file '%s': "
                            "reading footer failure",
                            filename)));
		}
		actualreadsize += readsize;
	}
    
    elog(DEBUG5, "Read parquet footer data from segment file '%s'. "
                 "Loaded %d bytes into buffer.",
                 filename,
                 size);
    
	return;
}

/*
 * Write buffer data to target file.
 *
 * @file		The file handler to write
 * @filename	The file name string for generating error report
 * @buffer		The buffer as source data.
 * @size		The data size to write.
 */
void flushBufferToFile(File file, char *filename, uint8_t *buffer, int size)
{
    int writesize = FileWrite(file,
                              (char *)buffer,
                              size );
    if ( writesize != size ) {
        /*ereport error*/
        ereport(ERROR,
                (errcode(ERRCODE_IO_ERROR),
                 errmsg("Parquet Storage Write error on segment file '%s': "
                        "writing footer file failure",
                        filename)));
    }
}

/*
 * Read temporary file and fill the buffer as many as possible.  The content in
 * the buffer will be flushed again to the real target file.
 *
 * @file        The temporary file handler to read.
 * @buffer      The buffer to contain data.
 * @size        The size of the buffer.
 *
 * Return the actual size of the data read from the temporary file. If the value
 * returned is less than the buffer size and there is no error reported, the end
 * of the temporary file is reached.
 */
int fillBufferFromTempFile(File file, uint8_t *buffer, int size)
{
	int actualreadsize  = 0;
	int readsize 		= 0;
	while( actualreadsize < size ) {
		readsize = FileRead( file,
                            (char *)(buffer) + actualreadsize,
                            size - actualreadsize);
		if ( readsize < 0 ) {
			/*ereport error*/
            ereport(ERROR,
                    (errcode(ERRCODE_IO_ERROR),
                     errmsg("Parquet Storage Read error on temporary file:"
                            "reading footer temporary file failure")));
		}
        else if ( readsize == 0 )
            break;
        
		actualreadsize += readsize;
	}
	return actualreadsize;
}


