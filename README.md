List Store
==========

Introduction
------------
The List Store is a way to save list of items with the following characteristics:

* A row, or a list item, is a tuple of (ctime, seen-flag, dismissed-flag, content).
* A row is uniquely identified by ctime; no two rows shall have the same ctime.
* Successive insertion can only be tagged with younger ctime.
* The ctime and content of a row is immutable; only the seen-flag and dismissed-flag may be set.
* Each row belongs to a named list.
* Backed by AWS S3.
* Changes in the past 30 days are stored locally in Redis.

Usage
-----
A list where the content is immutable is useful in many real life situations. In the context of oDesk, we have notifications and contractor events.

Interface
---------

#### name
Name is a string that uniquely identifies a list. For our notification service, the name of a list will be ns-u1234 where “ns” denotes notification service, and u1234 specifies a user in the ODB Users table. For our event service, the name of a list will be es-u1234 or es-e6789 where “es” denotes the event service, while u1234 identifies a user in the ODB Users table, and e6789 identifies an employer in the ODB Companies table.

#### ctime
ctime is the creation denoted by a floating point number representing #seconds since epoch.

#### content
The content of a list item is not interpreted by the List Store. It is usually a compressed json object.

#### class liststore.ListStore(redis_host, redis_port, s3bucket, aws_access_key, aws_secret_key)
Instantiate a List Store object to work on an s3bucket, and utilizing REDIS service as specified.

#### ListStore.append(name, [(ctime, content), …])
Append a new row into the store in list :name.

If a record of ctime already exists, returns 403 Forbidden. 

If the ctime is older than the last record of the list :name,  return 403 Forbidden (i.e., you can only insert a row after the last row timewise by ctime). 

If the list :name does not exist, it will be created automatically. 

For batch insertion, specify an array of ctime/content pairs in the body of the call.

#### ListStore.delete(name, ctime)
Delete the row in list :name identified by :ctime.

#### ListStore.setSeen(name, ctime, prior=False)
Set the seen flags of a row in list :name. If the prior flag is true, then set the flags on all rows where ctime are less than or equal to :ctime.

#### ListStore.setDismissed(name, ctime, prior=False)
Set the dismissed flags of a row in list :name. If the prior flag is true, then set the flags on all rows where ctime are less than or equal to :ctime.

#### ListStore.retrieve(name, ctime)
Retrieve the row in list :name identified by :ctime.

#### ListStore.reverseScan(name, ctime, limit=100, offset=0, skipSeen=0, skipDismissed=1)
Retrieve up to :limit undismissed rows starting from :offset in the list :name where the ctime of the rows are less than or equal to :ctime. The rows are ordered in reverse chronological order based on ctime. 

Implementation
--------------
### Data Types

#### ctime
ctime is a timestamp in GMT. It is a floating point number representing #seconds since epoch.

#### seen flag / dismissed flag flag
Flags are boolean values denoted by 0 or 1.

### AWS
#### S3 Data Record
The S3 data records in the S3 bucket are keyed by a string of the form “/:name/:YYYYMM.gz”. 

Each S3 data record will store an array of tuples, grouped by :name, :YYYYMM in gzip format.

Each tuple consists of (ctime, content, seen flag, dismissed flag).

#### S3 Index Record
The S3 index records in the S3 bucket are keyed by a string of the form “/:name.gz”.

Each S3 index record will store in gzip format a dictionary { YYYYMM: (total#, dismissed#, seen#), YYYYMM:... }, identifying all S3 data records belonging to the list :name, and a count of tuples in the month YYYYMM, and of those, how many were dismissed or seen.

The index records are used internally to find items belonging to a particular :name, and provide capability to only retrieve S3 data records containing items that are neither seen nor dismissed.

REST Interface
--------------
We will not be doing this. But if we do, it will look like what follows.

### Append
`HTTP: POST /ls/v1/lists/:name

JSON Content: [{ ctime: X, content: Y }, … ]`

Append a new row into the store in list :name. 

If a record of ctime X already exists, returns 403 Forbidden. 

If the ctime X is older than the last record of the list :name,  return 403 Forbidden. I.e. you can only insert a row after the last row timewise by ctime. 

If the list :name does not exist, it will be created automatically. 

For batch insertion, specify an array of ctime/content pairs in the body of the call.

### Deletion
`HTTP: DELETE /ls/v1/lists/:name/ctime/:ctime

JSON Content: None`

Delete the row in list :name identified by :ctime.

### Retrieval
`HTTP: GET /ls/v1/lists/:name/ctime/:ctime

JSON Content: None`

Retrieve the row in list :name identified by :ctime.

### List
`HTTP: GET /ls/v1/lists/:name/ctime/:ctime?limit=:n&offset=:m

JSON Content: None`

Retrieve up to :n undismissed rows starting from offset :m in the list :name where the ctime of the rows are less than or equal to :ctime. The rows are ordered in reverse chronological order based on ctime.

### Set Flags
`HTTP: POST /ls/v1/lists/:name/ctime/:ctime

JSON Content: { seen: 1, dismissed: 1}


HTTP: POST /ls/v1/lists/:name/ctime/:ctime

JSON Content: { seen: 1, prior: 1 }`

Set the seen and/or dismissed flags of a row in list :name. If the prior flag is true, then set the flags on all rows where ctime are less than or equal to :ctime.

