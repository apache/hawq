// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// http://code.google.com/p/protobuf/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package com.google.protobuf;

/**
 * Interface that generated extensions implement.
 *
 * @author liujisi@google.com (Jisi Liu)
 */
public abstract class Extension<ContainingType extends MessageLite, Type> {
  /** Returns the field number of the extension. */
  public abstract int getNumber();

  /** Returns the type of the field. */
  public abstract WireFormat.FieldType getLiteType();

  /** Returns whether it is a repeated field. */
  public abstract boolean isRepeated();

  /** Returns the descriptor of the extension. */
  public abstract Descriptors.FieldDescriptor getDescriptor();

  /** Returns the default value of the extension field. */
  public abstract Type getDefaultValue();

  /**
   * Returns the default instance of the extension field, if it's a message
   * extension.
   */
  public abstract MessageLite getMessageDefaultInstance();

  // All the methods below are extension implementation details.

  /**
   * The API type that the extension is used for.
   */
  protected enum ExtensionType {
    IMMUTABLE,
    MUTABLE,
    PROTO1,
  }

  protected ExtensionType getExtensionType() {
    // TODO(liujisi): make this abstract after we fix proto1.
    return ExtensionType.IMMUTABLE;
  }

  /**
   * Type of a message extension.
   */
  public enum MessageType {
    PROTO1,
    PROTO2,
  }
  
  /**
   * If the extension is a message extension (i.e., getLiteType() == MESSAGE),
   * returns the type of the message, otherwise undefined.
   */
  public MessageType getMessageType() {
    return MessageType.PROTO2;
  }

  protected abstract Object fromReflectionType(Object value);
  protected abstract Object singularFromReflectionType(Object value);
  protected abstract Object toReflectionType(Object value);
  protected abstract Object singularToReflectionType(Object value);
}
