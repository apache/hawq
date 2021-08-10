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
* $Log: shared.h,v $
* Revision 1.3  2007/04/07 08:10:40  cmcdevitt
* Fixes for dbgen with large scale factors
*
* Revision 1.2  2005/01/03 20:08:59  jms
* change line terminations
*
* Revision 1.1.1.1  2004/11/24 23:31:47  jms
* re-establish external server
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* recreation after CVS crash
*
* Revision 1.1.1.1  2003/04/03 18:54:21  jms
* initial checkin
*
*
*/
#define N_CMNT_LEN      72
#define N_CMNT_MAX      152
#define R_CMNT_LEN      72
#define R_CMNT_MAX      152
#define  MONEY_SCL     0.01
#define  V_STR_HGH    1.6
#define  P_NAME_LEN    55
#define  P_MFG_LEN     25
#define  P_BRND_LEN    10
#define  P_TYPE_LEN    25
#define  P_CNTR_LEN    10
#define  P_CMNT_LEN    14
#define  P_CMNT_MAX    23
#define  S_NAME_LEN    25
#define  S_ADDR_LEN    25
#define  S_ADDR_MAX    40
#define  S_CMNT_LEN    63
#define  S_CMNT_MAX   101
#define  PS_CMNT_LEN  124
#define  PS_CMNT_MAX  199
#define  C_NAME_LEN    18
#define  C_ADDR_LEN    25
#define  C_ADDR_MAX    40
#define  C_MSEG_LEN    10
#define  C_CMNT_LEN    73
#define  C_CMNT_MAX    117
#define  O_OPRIO_LEN   15
#define  O_CLRK_LEN    15
#define  O_CMNT_LEN    49
#define  O_CMNT_MAX    79
#define  L_CMNT_LEN    27
#define  L_CMNT_MAX    44
#define  L_INST_LEN    25
#define  L_SMODE_LEN   10
#define  T_ALPHA_LEN   10
#define  DATE_LEN      13  /* long enough to hold either date format */
#define  NATION_LEN    25
#define  REGION_LEN    25
#define  PHONE_LEN     15
#define  MAXAGG_LEN    20    /* max component length for a agg str */
#define  P_CMNT_SD      6
#define  PS_CMNT_SD     9
#define  O_CMNT_SD     12
#define  C_ADDR_SD     26
#define  C_CMNT_SD     31
#define  S_ADDR_SD     32
#define  S_CMNT_SD     36
#define  L_CMNT_SD     25

