/*

Note: about SrvLimitedResource

SrvLimitedResource is supposed to contain server-specific limited
resources. We tried to think of all the limited resources we could,
and didn't find any that belonged in this table. Things like memory,
disk space, etc. are already available through the default net-snmp
implementation, and it didn't seem like a good idea to re-implement
them here. XIDs are database-specific, so they're in the
DbLimitedResource table. So the SrvLimitedResource table was left
empty until we came up with useful information we could put in it.

*/
