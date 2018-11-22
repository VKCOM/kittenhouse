package persist

// Persistent logs have the following data flow:
// 1. insert() RPC call creates new log file if necessary and writes to file. No more actions are performed in this RPC call.
// 2. Single goroutine called (*sender).loop() periodically scans all files and sends to clickhouse all data that was not yet successfully delivered.
// 3. There is a limit on how much data we read from a single file at a time.
// 4. Limit exists in order not to eat up all memory and to give chance for all tables to be streamed more-or-less fairly if there is a bottleneck.

// File format (<tableName>/<date>_<hash>  -  <tableName> here does not contain fields and <hash> is md5(fields)):
// 1. Header that contains full table name with fields. Headers start with '#'
// 2. Single line looks like this: <line><CRC32>\n
//       - <line> is a single line that is inserted in VALUES, with '\n' and '\\' escaped using \
//       - <CRC32> is a checksum of <line> _with_escaping_ (in HEX format, taking 8 bytes)

// offsets.json contains offsets in files that have been acknowledged by clickhouse
