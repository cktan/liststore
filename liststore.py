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
class ListStoreIndexPage:
    def __init__(self, jsonString):
        self.ymtab = jsonString and json.loads(jsonString) or []
        if not isinstance(self.ymtab, list):
            raise DataError('bad index page')

    def index(self, yyyymm):
        a = [r['yyyymm'] for r in self.ymtab]
        i = bisect.bisect_left(a, yyyymm)
        found = (i < len(self.ymtab) and self.ymtab[i]['yyyymm'] == yyyymm)
        return (i, found)

    def find(self, yyyymm):
        (i, found) = self.index(yyyymm)
        if found:
            return self.ymtab[i]
        return None

    def toJson(self):
        return json.dumps(self.ymtab)


### ------------------------------------------
class ListStoreDataPage:
    def __init__(self, jsonString):
        self.rows = jsonString and json.loads(jsonString) or []
        if not isinstance(self.rows, list):
            raise DataError('bad data page')

    def toJson(self):
        return json.dumps(self.rows)

    def index(self, ctime):
        cta = [r['ctime'] for r in self.rows]
        i = bisect.bisect_left(cta, ctime)
        found = (i < len(self.rows) and self.rows[i]['ctime'] == ctime)
        return (i, found)

    def find(self, ctime):
        (i, found) = self.index(ctime)
        if found:
            return self.rows[i]
        return None

### ------------------------------------------
def compress(s):
    buf = StringIO.StringIO()
    f = gzip.GzipFile(fileobj=buf, mode='wb')
    try:
        f.write(s)
    finally:
        f.close()
    return buf.getvalue()

### ------------------------------------------
def uncompress(z):
    buf = StringIO.StringIO(z)
    f = gzip.GzipFile(fileobj=buf, mode='rb')
    try:
        s = f.read()
    finally:
        f.close()
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
        self._s3_bucket = None
        self._s3_conn = None
        self._rconn = None

    ### ------------------------------------------
    def s3_bucket(self):
        if not self._s3_conn:
            self._s3_conn = boto.connect_s3(self.aws_access_key, self.aws_secret_key)
        if not self._s3_bucket:
            self._s3_bucket = self._s3_conn.get_bucket(self.s3_bucket_name)
        return self._s3_bucket

    ### ------------------------------------------
    def s3_key(self, keystr):
        bkt = self.s3_bucket()
        kk = boto.s3.key.Key(bkt)
        kk.key = keystr
        return kk

    ### ------------------------------------------
    def rconn(self):
        if not self._rconn:
            self._rconn = redis.StrictRedis(self.redis_host, self.redis_port)
        return self._rconn
    
    ### ------------------------------------------
    def rget(self, k):
        return self.rconn().get('liststore::' + k)

    ### ------------------------------------------
    def rset(self, k, z):
        return self.rconn().set('liststore::' + k, z)

    ### ------------------------------------------
    def rdelete(self, k):
        return self.rconn().delete('liststore::' + k)

    ### ------------------------------------------
    def _write(self, k, s):
        k = k + '.gz'
        kk = self.s3_key(k)
        z = compress(s)
        kk.set_contents_from_string(z)
        
        # put (k, z) in redis
        self.rset(k, z)

    ### ------------------------------------------
    def _read(self, k):
        k = k + '.gz'
        z = self.rget(k)
        if not z:
            kk = self.s3_key(k)
            try:
                z = kk.get_contents_as_string()
            except boto.exception.S3ResponseError as e:
                if e.status == 404:
                    z = ''
                else:
                    raise e
        return z and uncompress(z) or ''

    ### ------------------------------------------
    def _readIndexPage(self, name):
        return ListStoreIndexPage(self._read(name))

    ### ------------------------------------------
    def _writeIndexPage(self, name, ip):
        return self._write(name, ip.toJson())

    ### ------------------------------------------
    def _readDataPage(self, name, yyyymm):
        ip = self._readIndexPage(name)
        if not ip.find(yyyymm):
            return ListStoreDataPage('')
        return ListStoreDataPage(self._read(name + '/' + yyyymm))
        

    ### ------------------------------------------
    def _writeDataPage(self, name, yyyymm, dp):
        ip = self._readIndexPage(name)
        # compute total, seen, dismissed, ctime_max
        seen, dismissed = 0, 0
        total = len(dp.rows)
        ctime_max = 0
        for i in dp.rows:
            if i['seen']: seen = seen + 1
            if i['dismissed']: dismissed = dismissed + 1
            if ctime_max < i['ctime']: ctimeMax = i['ctime']
        if ctime_max <= 0:
            ctime_max = calendar.timegm(time.strptime(yyyymm + '01', '%Y%m%d'))

        r = {'yyyymm': yyyymm, 'total': total, 'seen': seen, 'dismissed': dismissed, 'ctime_max': ctime_max}
        (i, found) = ip.index(yyyymm)
        if found:
            ip.ymtab[i] = r
        else:
            ip.ymtab.insert(i, r)

        # write data page to s3
        self._write(name + '/' + yyyymm, dp.toJson())

        # write index page to s3
        self._writeIndexPage(name, ip)
        

    ### ------------------------------------------
    def _append(self, name, yyyymm, rows):
        # sort by ctime
        rows.sort(key = lambda x: x[0])

        ip = self._readIndexPage(name)
        if ip.ymtab:
            last = ip.ymtab[-1]
            if last['ctime_max'] >= rows[0][0]:
                raise DataError('ctime ' + time.asctime(time.gmtime(rows[0][0])) + ' is younger than current last record')

        # read the page, append, and write it
        dp = self._readDataPage(name, yyyymm)
        for (ctime, content) in rows:
            dp.rows += [ {'ctime':ctime, 'content':content, 'seen':0, 'dismissed':0} ]
        self._writeDataPage(name, yyyymm, dp)

    ### ------------------------------------------
    def append(self, name, rows):
        # group rows by month
        g = {}
        for i in rows:
            ctime, content = i
            yyyymm = unixTimeToYYYYMM(ctime)
            a = g.get(yyyymm, False)
            if a:
                a.append(i)
            else:
                g[yyyymm] = [i]
        for yyyymm in sorted(g.keys()):
            self._append(name, yyyymm, g[yyyymm])

    ### ------------------------------------------
    def delete(self, name, ctime):
        ip = self._readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        r = ip.find(yyyymm)
        if r:
            dp = self._readDataPage(name, yyyymm)
            (i, found) = dp.index(ctime)
            if found:
                print 'deleting', dp.rows[i]
                del dp.rows[i]
                self._writeDataPage(name, yyyymm, dp)

    ### ------------------------------------------
    def setSeen(self, name, ctime, prior=False):
        pass

    ### ------------------------------------------
    def setDismissed(self, name, ctime, prior=False):
        pass

    ### ------------------------------------------
    def retrieve(self, name, ctime):
        ip = self._readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        r = ip.find(yyyymm)
        if r and r['total'] > r['dismissed']:
            dp = self._readDataPage(name, yyyymm)
            r = dp.find(ctime)
            if r and not r['dismissed']:
                return r
        return None

    ### ------------------------------------------
    def reverseScan(self, name, ctime, limit=100, offset=0, skipSeen=0, skipDismissed=1):
        ip = self._readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        i = ip.index(yyyymm)
        if (i < len(ip.ymtab) and ip.ymtab[i]['yyyymm'] > yyyymm):
            i = i - 1
        if (i >= len(ip.ymtab)):
            return []
        out = []
        for i in xrange(i, -1, -1):
            if len(out) >= limit:
                break
            if skipDismissed and ip.ymtab[i]['total'] > ip.ymtab[i]['dismissed']:
                continue
            if skipSeen and ip.ymtab[i]['total'] > ip.ymtab[i]['seen']:
                continue
            dp = self._readDataPage(name, ip.ymtab[i]['yyyymm'])
            for j in xrange(len(dp.rows) - 1, -1, -1):
                if len(out) >= limit:
                    break
                if skipDismissed and dp.rows[j]['dismissed']:
                    continue
                if skipSeen and dp.rows[j]['seen']:
                    continue
                out += [r]

        return out

    ### ------------------------------------------
    def reset(self, name):
        bkt = self.s3_bucket()
        rs = bkt.list(name)
        for key in rs:
            bkt.delete_key(key)
            self.rdelete(key.name)
            

if __name__ == '__main__':
    if not os.environ.get('AWS_ACCESS_KEY'):
        sys.exit('AWS_ACCESS_KEY not set')
    if not os.environ.get('AWS_SECRET_KEY'):
        sys.exit('AWS_SECRET_KEY not set')
    if not (2 <= len(sys.argv) and len(sys.argv) <= 4):
        sys.exit('Usage: %s bucket_name [redis_host [redis_port]]' % sys.argv[0])

    name = 'cktan'
    ls = ListStore(sys.argv[1], os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'],
                   len(sys.argv) >= 3 and sys.argv[2] or 'localhost',
                   len(sys.argv) >= 4 and sys.argv[3] or 6379)
    # ls.reset(name)

    # insert one item per day for the whole year in batches of 1, 2, 4, 8, 16, 64
    start = calendar.timegm(time.strptime('20130101', '%Y%m%d'))
    for i in xrange(365):
        ls.delete(start + i * (24 * 60 * 60)
    i = 0
    while i < 365:
        out = []
        for j in xrange(1 << (i % 7)):
            if i >= 365: break
            t = start + i * (24 * 60 * 60)
            out += [(t, 'hello ' + time.asctime(time.gmtime(t)))]
            i = i + 1
        if out:
            ls.append(name, out)
            for x in out:
                print ls.retrieve(name, x[0])

    
