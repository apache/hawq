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
/*
* $Id$
*
* Revision History
* ===================
* $Log: rng64.h,v $
* Revision 1.2  2007/04/07 08:10:40  cmcdevitt
* Fixes for dbgen with large scale factors
*
* Revision 1.2  2005/01/03 20:08:59  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:47  jms
* re-establish external server
*
* Revision 1.1.1.1  2003/08/08 21:57:34  jms
* recreation after CVS crash
*
* Revision 1.1  2003/08/08 21:57:34  jms
* first integration of rng64 for o_custkey and l_partkey
*
*
*/
DSS_HUGE AdvanceRand64( DSS_HUGE nSeed, DSS_HUGE nCount);
void dss_random64(DSS_HUGE *tgt, DSS_HUGE nLow, DSS_HUGE nHigh, long stream);
DSS_HUGE NextRand64(DSS_HUGE nSeed);
