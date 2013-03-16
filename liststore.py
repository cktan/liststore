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
        if not self.rconn:
            self.rconn = redis.StrictRedis(self.redis_host, self.redis_port)
        return self.rconn
    
    ### ------------------------------------------
    def __rget(self, k):
        return self.__rconn().get('liststore::' + k)

    ### ------------------------------------------
    def __rset(self, k, z):
        return self.__rconn().setex('liststore::' + k, 30 * 24 * 60 * 60, z)

    ### ------------------------------------------
    def __rdelete(self, k):
        return self.__rconn().delete('liststore::' + k)


    ### ------------------------------------------
    def __write(self, k, s):
        k = k + '.gz'
        kk = self.__s3_key_handle(k)
        z = compress(s)
        kk.set_contents_from_string(z)
        
        # put (k, z) in redis
        self.__rset(k, z)

    ### ------------------------------------------
    def __read(self, k):
        k = k + '.gz'
        z = self.__rget(k)
        if not z:
            kk = self.__s3_key_handle(k)
            try:
                z = kk.get_contents_as_string()
                self.__rset(k, z)
            except boto.exception.S3ResponseError as e:
                if e.status == 404:
                    z = ''
                else:
                    raise e
        return z and uncompress(z) or ''

    ### ------------------------------------------
    def __readIndexPage(self, name):
        return ListStoreIndexPage(self.__read(name))

    ### ------------------------------------------
    def __writeIndexPage(self, name, ip):
        return self.__write(name, ip.toJson())

    ### ------------------------------------------
    def __readDataPage(self, name, yyyymm):
        ip = self.__readIndexPage(name)
        r = ip.find(yyyymm)
        if not r:
            return ListStoreDataPage('')
        dp = ListStoreDataPage(self.__read(name + '/' + yyyymm))
        # fix up dp to be consistent with r
        if len(dp.rows) > r['total']:
            dp.rows = dp.rows[:r['total']]
        return dp

    ### ------------------------------------------
    def __writeDataPage(self, name, yyyymm, dp):
        ip = self.__readIndexPage(name)
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
        self.__write(name + '/' + yyyymm, dp.toJson())

        # write index page to s3
        self.__writeIndexPage(name, ip)
        

    ### ------------------------------------------
    def __append(self, name, yyyymm, rows):
        # sort by ctime
        rows.sort(key = lambda x: x[0])

        ip = self.__readIndexPage(name)
        for i in xrange(len(ip.ymtab)-1, -1, -1):
            last = ip.ymtab[i]
            if last['total'] > 0 and last['ctime_max'] >= rows[0][0]:
                raise DataError('ctime ' + time.asctime(time.gmtime(rows[0][0])) + ' is younger than current last record')

        # read the page, append, and write it
        dp = self.__readDataPage(name, yyyymm)
        for (ctime, content) in rows:
            dp.rows += [ {'ctime':ctime, 'content':content, 'seen':0, 'dismissed':0} ]
        self.__writeDataPage(name, yyyymm, dp)

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
            self.__append(name, yyyymm, g[yyyymm])

    ### ------------------------------------------
    def delete(self, name, ctime):
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        r = ip.find(yyyymm)
        if r:
            dp = self.__readDataPage(name, yyyymm)
            (i, found) = dp.index(ctime)
            if found:
                del dp.rows[i]
                self.__writeDataPage(name, yyyymm, dp)

    ### ------------------------------------------
    def __setFlag(self, name, flag, ctime, prior):
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        if not prior:
            r = ip.find(yyyymm)
            if r: 
                dp = self.__readDataPage(name, yyyymm)
                r = dp.find(ctime)
                if r and not r[flag]:
                    r[flag] = 1
                    self.__writeDataPage(name, yyyymm, dp)
            return

        # prior is True
        (i, found) = ip.index(yyyymm)
        if not found:
            i = i - 1
        for i in xrange(i, -1, -1):
            yyyymm = ip.ymtab[i]['yyyymm']
            dp = self.__readDataPage(name, yyyymm)
            (j, found) = dp.index(ctime)
            if not found:
                j = j - 1
            dirty = 0
            for j in xrange(j, -1, -1):
                if not dp.rows[j][flag]:
                    dp.rows[j][flag] = 1
                    dirty = 1
            if dirty:
                self.__writeDataPage(name, yyyymm, dp)

    ### ------------------------------------------
    def setSeen(self, name, ctime, prior=False):
        self.__setFlag(name, 'seen', ctime, prior)

    ### ------------------------------------------
    def setDismissed(self, name, ctime, prior=False):
        self.__setFlag(name, 'dismissed', ctime, prior)

    ### ------------------------------------------
    def retrieve(self, name, ctime):
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        r = ip.find(yyyymm)
        if r and r['total'] > r['dismissed']:
            dp = self.__readDataPage(name, yyyymm)
            r = dp.find(ctime)
            if r and not r['dismissed']:
                return r
        return None

    ### ------------------------------------------
    def reverseScan(self, name, ctime, limit=100, offset=0, skipSeen=0, skipDismissed=1):
        ip = self.__readIndexPage(name)
        yyyymm = unixTimeToYYYYMM(ctime)
        (i, found) = ip.index(yyyymm)
        if not found:
            i = i - 1
        out = []
        for i in xrange(i, -1, -1):
            if len(out) >= limit:
                break
            if skipDismissed and ip.ymtab[i]['total'] > ip.ymtab[i]['dismissed']:
                continue
            if skipSeen and ip.ymtab[i]['total'] > ip.ymtab[i]['seen']:
                continue
            dp = self.__readDataPage(name, ip.ymtab[i]['yyyymm'])
            (j, found) = dp.index(ctime)
            if not found:
                j = j - 1
            for j in xrange(j, -1, -1):
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
        bkt = self.__s3_bucket_handle()
        rs = bkt.list(name)
        for key in rs:
            # print 'deleting', key
            bkt.delete_key(key)
            self.__rdelete(key.name)
        self.uncache(name)

    ### ------------------------------------------
    def uncache(self, name):
        self.__rdelete('liststore::' + name + '.gz')
        keys = self.__rconn().keys('liststore::' + name + '/*.gz')
        # print keys
        for k in keys:
            self.__rdelete(k)

if __name__ == '__main__':
    if not os.environ.get('AWS_ACCESS_KEY'):
        sys.exit('AWS_ACCESS_KEY not set')
    if not os.environ.get('AWS_SECRET_KEY'):
        sys.exit('AWS_SECRET_KEY not set')
    if not (2 <= len(sys.argv) and len(sys.argv) <= 4):
        sys.exit('Usage: %s bucket_name [redis_host [redis_port]]' % sys.argv[0])


    bucketname = sys.argv[1] 
    redis_host = len(sys.argv) >= 3 and sys.argv[2] or 'localhost'
    redis_port = len(sys.argv) >= 4 and sys.argv[3] or 6379

    ls = ListStore(bucketname,
                   os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'],
                   redis_host, redis_port)
    
    name = 'cktan'
    start = calendar.timegm(time.strptime('20130101', '%Y%m%d'))
    
    # fresh start for test
    ls.uncache(name)
    ls.reset(name)

    # insert one item per day for the whole year in batches of 1, 2, 4, 8, 16, 64
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

    testDelete = False
    if testDelete: 
        # delete all
        for i in xrange(365):
            ls.delete(name, start + i * (24 * 60 * 60))

        # again:
        # insert one item per day for the whole year in batches of 1, 2, 4, 8, 16, 64
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

    # dismiss March 31
    mar31 = calendar.timegm(time.strptime('20130331', '%Y%m%d'))
    ls.setDismissed(name, mar31, prior=False)
    r = ls.retrieve(name, mar31)
    assert r == None, 'Dismissed record is not dismissed'
    
    # dismiss everything on and before Feb 14
    feb14 = calendar.timegm(time.strptime('20130214', '%Y%m%d'))
    ls.setDismissed(name, feb14, prior=True)
    r = ls.retrieve(name, feb14)
    assert r == None, 'Dismissed record is not dismissed'
    jan10 = calendar.timegm(time.strptime('20130110', '%Y%m%d'))
    r = ls.retrieve(name, jan10)
    assert r == None, 'Dismissed record is not dismissed'
    
    # seen on June 1
    jun1 = calendar.timegm(time.strptime('20130601', '%Y%m%d'))
    ls.setSeen(name, jun1, prior=False)
    r = ls.retrieve(name, jun1)
    assert r and r['seen'], 'Seen record is not seen'
    
    # set everything seen on and before March 14
    mar14 = calendar.timegm(time.strptime('20130314', '%Y%m%d'))
    ls.setSeen(name, mar14, prior=True)

    # verify
    for i in xrange(365):
        t = start + i * 24 * 60 * 60
        r = ls.retrieve(name, t)

        if t <= feb14 or t == mar31:
            assert r == None, 'Dismissed record is not dismissed'
            continue

        assert r != None, 'Non-dismissed record is not found'

        if t <= mar14 or t == jun1:
            assert r['seen'], 'Seen record is not seen'
            continue

        assert not r['seen'], 'Not-seen record is seen'
