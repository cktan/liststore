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
class DocStore:

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
        return self.__rconn().get('docstore::' + k)

    ### ------------------------------------------
    def __rset(self, k, z):
        return self.__rconn().setex('docstore::' + k, 30 * 24 * 60 * 60, z)

    ### ------------------------------------------
    def __rdelete(self, k):
        return self.__rconn().delete('docstore::' + k)

    ### ------------------------------------------
    def __delete(self, k, s):
        k = k + '.gz'
        bkt = self.__s3_bucket_handle()
        bkt.delete_key(k)
        self.__rdelete(k)

    ### ------------------------------------------
    def __write(self, k, s):
        z = compress(s)
        k = k + '.gz'
        kk = self.__s3_key_handle(k);
        try:
            kk.set_contents_from_string(z)
            # put (k, z) in redis
            self.__rset(k, z)
        finally:
            kk.close()

    ### ------------------------------------------
    def __read(self, k):
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
                    return ''
                else:
                    raise e
            finally:
                kk.close()

        return uncompress(z)

    ### ------------------------------------------
    def put(self, path, id, s):
        k = path + '/' + id
        self.__write(k, s)

    ### ------------------------------------------
    def get(self, path, id):
        k = path + '/' + id
        return self.__read(k)

    ### ------------------------------------------
    def delete(self, path, id):
        k = path + '/' + id
        self.__delete(k)

    ### ------------------------------------------
    def list(self, path, limit):
        bkt = self.__s3_bucket_handle()
        rs = bkt.list(path)
        out = []
        for key in rs:
            if limit == 0: break
            out += [key]
            limit = limit - 1
        return out


if __name__ == '__main__':
    pass
