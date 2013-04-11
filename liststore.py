import time, json, sys, os, calendar
import boto
import redis
import StringIO, gzip, bisect

### ------------------------------------------
class Error(Exception):
    '''Base class for exceptions in this module.'''
    pass

### ------------------------------------------
class DataError(Error):
    def __init__(self, msg):
        self.msg = msg

### ------------------------------------------
class NonFutureItemError(DataError):
    def __init__(self):
        super(NonFutureItemError, self).__init__('new ctime must be later than known ctime')


### ------------------------------------------
class ListStoreIndexPage:
    '''An Index Page contains these fields:
    magic: "ListStoreIndexPage"
    version: 1
    ymtab: htab of yyyymm ->  {yyyymm, total, seen, dismissed, ctime_max} records.
    '''
    
    def __init__(self, jsonString):
        if jsonString:
            s = json.loads(jsonString)
            if not isinstance(s, dict):
                raise DataError('bad index page')
            if not s.get('magic') == 'ListStoreIndexPage':
                raise DataError('bad index page')
            if not s.get('version') == 1:
                raise DataError('bad index page')
        else:
            s = {'magic': 'ListStoreIndexPage', 'version': 1, 'ymtab': {}}
        
        if not isinstance(s.get('ymtab'), dict):
            raise DataError('bad index page')

        self.s = s
        self.ymtab = s['ymtab']


    def toJson(self):
        return json.dumps(self.s)


### ------------------------------------------
class ListStoreDataPage:
    '''A Data Page contains these fields:
    magic: 'ListStoreDataPage'
    version: 1
    ctab: an array of {ctime, content, seen, dismissed} records.
    The array is ordered by ctime ascending.'''

    def __init__(self, jsonString):
        if jsonString:
            s = json.loads(jsonString)
            if not isinstance(s, dict):
                raise DataError('bad data page')
            if not s.get('magic') == 'ListStoreDataPage':
                raise DataError('bad data page')
            if not s.get('version') == 1:
                raise DataError('bad data page')
        else:
            s = {'magic': 'ListStoreDataPage', 'version': 1, 'ctab': []}

        if not isinstance(s.get('ctab'), list):
            raise DataError('bad data page')

        self.s = s
        self.ctab = s['ctab']


    def toJson(self):
        return json.dumps(self.s)

    def index(self, ctime):
        cta = [r['ctime'] for r in self.ctab]
        i = bisect.bisect_left(cta, ctime)
        found = (i < len(self.ctab) and self.ctab[i]['ctime'] == ctime)
        return (i, found)

    def find(self, ctime):
        (i, found) = self.index(ctime)
        if found:
            return self.ctab[i]
        return None

### ------------------------------------------
def compress(s):
    buf = StringIO.StringIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as f:
        f.write(s)
    return buf.getvalue()

### ------------------------------------------
def uncompress(z):
    buf = StringIO.StringIO(z)
    with gzip.GzipFile(fileobj=buf, mode='rb') as f:
        s = f.read()
    return s

### ------------------------------------------
def unixTimeToYYYYMM(t):
    t = time.gmtime(t)
    return '%04d%02d' % (t.tm_year, t.tm_mon)

### ------------------------------------------
class ListStore:

    ### ------------------------------------------
    def __init__(self, s3_bucket, aws_access_key, aws_secret_key, redis_host, redis_port):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.s3_bucket_name = s3_bucket
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.s3_bucket = None
        self.s3_conn = None
        self.rconn = None

    ### ------------------------------------------
    def __s3_bucket_handle(self):
        if not self.s3_conn:
            self.s3_conn = boto.connect_s3(self.aws_access_key, self.aws_secret_key)
        if not self.s3_bucket:
            self.s3_bucket = self.s3_conn.get_bucket(self.s3_bucket_name)
        return self.s3_bucket

    ### ------------------------------------------
    def __s3_key_handle(self, keystr):
        bkt = self.__s3_bucket_handle()
        kk = boto.s3.key.Key(bkt)
        kk.key = keystr
        return kk

    ### ------------------------------------------
    def __rconn(self):
        '''Get a Redis connection.'''
        if not self.rconn:
            self.rconn = redis.StrictRedis(self.redis_host, self.redis_port)
        return self.rconn
    
    ### ------------------------------------------
    def __rget(self, k):
        '''Read bytea in Redis named by key k'''
        return self.__rconn().get('liststore::%s::%s' % (self.s3_bucket_name, k))

    ### ------------------------------------------
    def __rset(self, k, s):
        '''Save key k -> bytea s in Redis. Expires in 30 days.'''
        return self.__rconn().setex('liststore::%s::%s' % (self.s3_bucket_name, k), 30 * 24 * 60 * 60, s)

    ### ------------------------------------------
    def __rdelete(self, k):
        '''Delete key k in Redis.'''
        return self.__rconn().delete('liststore::%s::%s' % (self.s3_bucket_name, k))


    ### ------------------------------------------
    def __write(self, k, s):
        '''Write key k -> compressed string s in S3 and Redis.'''
        z = compress(s)
        k = k + '.gz'
        kk = self.__s3_key_handle(k)
        try:
            kk.set_contents_from_string(z)
            # put (k, z) in redis
            self.__rset(k, z)
        finally:
            kk.close()

    ### ------------------------------------------
    def __read(self, k):
        '''Read compressed string for key k from Redis or S3.'''
        k = k + '.gz'
        z = self.__rget(k)
        if not z:
            # cache-miss. look in s3.
            kk = self.__s3_key_handle(k);
            try:
                z = kk.get_contents_as_string()
                self.__rset(k, z)
            except boto.exception.S3ResponseError as e:
                if e.status == 404: # not found error
                    self.__rdelete(k)
                    return None
                else:
                    raise e
            finally:
                kk.close()

        return uncompress(z)

    ### ------------------------------------------
    def __readIndexPage(self, name):
        return ListStoreIndexPage(self.__read(name))

    ### ------------------------------------------
    def __writeIndexPage(self, name, ip):
        return self.__write(name, ip.toJson())

    ### ------------------------------------------
    def __readDataPage(self, name, yyyymm):
        ip = self.__readIndexPage(name)
        r = ip.ymtab.get(yyyymm)
        if not r:
            return ListStoreDataPage('')
        dp = ListStoreDataPage(self.__read(name + '/' + yyyymm))
        # fix up dp to be consistent with r
        if len(dp.ctab) > r['total']:
            dp.s['ctab'] = dp.ctab[:r['total']]
            dp.ctab = dp.s['ctab'] 
        return dp

    ### ------------------------------------------
    def __writeDataPage(self, name, yyyymm, dp):
        ip = self.__readIndexPage(name)
        # compute total, seen, dismissed, ctime_max
        seen, dismissed = 0, 0
        total = len(dp.ctab)
        ctime_max = 0
        for i in dp.ctab:
            if i['seen']: seen = seen + 1
            if i['dismissed']: dismissed = dismissed + 1
            if ctime_max < i['ctime']: ctime_max = i['ctime']
        if ctime_max <= 0:
            ctime_max = calendar.timegm(time.strptime(yyyymm + '01', '%Y%m%d'))

        r = {'yyyymm': yyyymm, 'total': total, 
		'seen': seen, 'dismissed': dismissed, 
		'ctime_max': ctime_max}
        ip.ymtab[yyyymm] = r

        # write data page to s3
        self.__write(name + '/' + yyyymm, dp.toJson())

        # write index page to s3
        self.__writeIndexPage(name, ip)
        

    ### ------------------------------------------
    def __append(self, name, yyyymm, newrows):
        # sort by ctime
        newrows.sort(key = lambda x: x[0])

        ip = self.__readIndexPage(name)
        for _, r in ip.ymtab.items():
            if r['total'] > 0 and r['ctime_max'] >= newrows[0][0]:
                raise NonFutureItemError()

        # read the page, append, and write it
        dp = self.__readDataPage(name, yyyymm)
        if len(dp.ctab) and dp.ctab[-1]['ctime'] >= newrows[0][0]:
            raise NonFutureItemError()
        for (ctime, content) in newrows:
            dp.ctab += [ {'ctime':ctime, 'content':content, 
				'seen':0, 'dismissed':0} ]
        self.__writeDataPage(name, yyyymm, dp)

    ### ------------------------------------------
    def append(self, name, rows):
        '''Append a batch of new records into the store in list
        :name. If the list does not exist, it will be created
        automatically.  Each record is a (:ctime, :content) tuple.  If
        any of the new records have a ctime that is younger than last
        known ctime for this list, a NonFutureEventError will be
        raise.
        '''
        # group rows by month
        g = {}
        for (ctime, content) in rows:
            yyyymm = unixTimeToYYYYMM(ctime)
            g[yyyymm] = g.get(yyyymm, []) + [ (ctime, content) ]
        for yyyymm in sorted(g.keys()):
            self.__append(name, yyyymm, g[yyyymm])

    ### ------------------------------------------
    def delete(self, name, ctime):
        '''Delete the record in list :name identified by :ctime.'''
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        if ip.ymtab.get(yyyymm):
            dp = self.__readDataPage(name, yyyymm)
            (i, found) = dp.index(ctime)
            if found:
                del dp.ctab[i]
                self.__writeDataPage(name, yyyymm, dp)

    ### ------------------------------------------
    def __setFlag(self, name, flag, ctime, prior):
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        if not prior:
            if ip.ymtab.get(yyyymm):
                dp = self.__readDataPage(name, yyyymm)
                r = dp.find(ctime)
                if r and not r[flag]:
                    r[flag] = 1
                    self.__writeDataPage(name, yyyymm, dp)
            return

        # prior is True
        for i in ip.ymtab.keys():
            if i > yyyymm: continue
            r = ip.ymtab[i]
            if r['total'] == r[flag]: continue
            dp = self.__readDataPage(name, i)
            (j, found) = dp.index(ctime)
            if not found:
                j = j - 1
            dirty = 0
            for j in xrange(j, -1, -1):
                if not dp.ctab[j][flag]:
                    dp.ctab[j][flag] = 1
                    dirty = 1
            if dirty:
                self.__writeDataPage(name, i, dp)

    ### ------------------------------------------
    def setSeen(self, name, ctime, prior=False):
        '''Set the seen flag in the list :name for the record
        identified by :ctime. If the prior flag is set, go backwards
        chronologically and set the seen flag of all records
        younger than :ctime as well.'''
        self.__setFlag(name, 'seen', ctime, prior)

    ### ------------------------------------------
    def setDismissed(self, name, ctime, prior=False):
        '''Set the dismissed flag in the list :name for the record
        identified by :ctime. If the prior flag is set, go backwards
        chronologically and set the dismissed flag of all records
        younger than :ctime as well.'''
        self.__setFlag(name, 'dismissed', ctime, prior)

    ### ------------------------------------------
    def retrieve(self, name, ctime):
        '''Retrieve a record identified by :ctime in the list
        :name. If it does not exist, return None.'''
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        r = ip.ymtab.get(yyyymm)
        if r and r['total'] > r['dismissed']:
            dp = self.__readDataPage(name, yyyymm)
            r = dp.find(ctime)
            if r and not r['dismissed']:
                return r
        return None

    ### ------------------------------------------
    def reverseScan(self, name, ctime, limit=100, offset=0, skipSeen=0, skipDismissed=1):
        '''Scan the list :name backwards chronologically starting from
        :ctime. Read at most :limit records starting at :offset. If
        skipSeen is set, include only not-seen entries. If
        skipDismissed is set, include only non-dismissed entries. An
        array of qualified records will be returned in descending
        order by :ctime of each record.'''
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        out = []
        for i in sorted(ip.ymtab.keys(), reverse=True):
            if i > yyyymm: continue
            if limit <= 0: break
            ir = ip.ymtab[i]
            if skipDismissed and ir['total'] == ir['dismissed']:
                continue
            if skipSeen and ir['total'] == ir['seen']:
                continue
            dp = self.__readDataPage(name, i)
            (j, found) = dp.index(ctime)
            if not found:
                j = j - 1
            for j in xrange(j, -1, -1):
                if limit <= 0: break
                jr = dp.ctab[j]
                if skipDismissed and jr['dismissed']:
                    continue
                if skipSeen and jr['seen']:
                    continue
                if offset > 0:
                    offset = offset - 1
                    continue
                limit = limit - 1
                out += [jr]

        return out


    ### ------------------------------------------
    def count(self, name):
        ip = self.__readIndexPage(name)
        total, dismissed, seen = 0, 0, 0
        for _, r in ip.ymtab.items():
            total += r['total']
            dismissed += r['dismissed']
            seen += r['seen']

        return {'total':total, 'dismissed':dismissed, 'seen':seen}


    ### ------------------------------------------
    def deleteName(self, name):
        '''Delete the list :name. All known records of the list will
        be deleted.'''
        bkt = self.__s3_bucket_handle()
        rs = bkt.list(name)
        for key in rs:
            bkt.delete_key(key)
            self.__rconn().delete(key.name)
        self.clearCache(name)


    ### ------------------------------------------
    def clearCache(self, name):
        '''Drop all Redis cache of the records belonging to the :name list.'''
        k = name + '.gz'
        self.__rconn().delete('liststore::%s::%s' % (self.s3_bucket_name, k))
        k = name + '/*.gz'
        keys = self.__rconn().keys('liststore::%s::%s' % (self.s3_bucket_name, k))
        for k in keys:
            self.__rconn().delete(k)

